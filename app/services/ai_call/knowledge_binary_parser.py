from __future__ import annotations

import argparse
import array
import fcntl
import json
import os
import signal
import socket
import stat
import struct
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, BinaryIO

from lingchen_knowledge.content import build_content
from lingchen_knowledge.documents import PdfNeedsOcr, material_document_policy, parse_document_bytes
from lingchen_knowledge.formats import classify
from lingchen_knowledge.models import Block, Content, KnowledgeError, Parsed

_MAX_REQUEST_BYTES = 4096
_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
_DEFAULT_PARSE_TIMEOUT_SECONDS = 25.0
_LENGTH = struct.Struct("!I")
_FORBIDDEN_ENVIRONMENT_PREFIXES = (
    "AI_CALL_KNOWLEDGE_COS_",
    "DATABASE_",
    "DASHSCOPE_",
    "REDIS_",
)
_FORBIDDEN_ENVIRONMENT_NAMES = {
    "SECRET_KEY",
    "JWT_SECRET",
    "TOKEN",
    "AI_CALL_KNOWLEDGE_OCR_API_KEY",
    "ASR_API_KEY",
}


class KnowledgeBinaryParseError(KnowledgeError):
    def __init__(self, message: str):
        super().__init__("DOCUMENT_WORKER_PROTOCOL", message, retryable=True)


class KnowledgeBinaryParserClient:
    def __init__(self, socket_path: str | Path, *, timeout_seconds: float = 30.0) -> None:
        self.socket_path = Path(socket_path)
        self.timeout_seconds = timeout_seconds

    def parse(self, source_path: str | Path, *, extension: str) -> Content:
        request = json.dumps(
            {"extension": extension},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        packet = _LENGTH.pack(len(request)) + request
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.connect(str(self.socket_path))
            with Path(source_path).open("rb") as source:
                rights = array.array("i", [source.fileno()])
                sent = connection.sendmsg(
                    [packet],
                    [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)],
                )
                if sent < len(packet):
                    connection.sendall(packet[sent:])
            response_size = _LENGTH.unpack(_recv_exact(connection, _LENGTH.size))[0]
            if response_size > _MAX_RESPONSE_BYTES:
                raise KnowledgeBinaryParseError("解析器响应超过限制")
            try:
                response = json.loads(_recv_exact(connection, response_size))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise KnowledgeBinaryParseError("解析器响应不合法") from exc
        if not isinstance(response, dict):
            raise KnowledgeBinaryParseError("解析器响应不合法")
        if response.get("ok") is not True:
            if response.get("code") == "PDF_NEEDS_OCR":
                pages, count = response.get("pages"), response.get("page_count")
                if (
                    not isinstance(pages, list)
                    or not pages
                    or type(count) is not int
                    or not all(type(page) is int and 1 <= page <= count for page in pages)
                ):
                    raise KnowledgeBinaryParseError("扫描页信息不合法")
                raise PdfNeedsOcr(tuple(pages), count)
            raise KnowledgeError(
                str(response.get("code") or "DOCUMENT_WORKER_PROTOCOL")[:64],
                str(response.get("message") or "文档解析失败")[:1000],
                retryable=response.get("retryable") is True,
            )
        try:
            version = response["parser_version"]
            if not isinstance(version, str) or not 0 < len(version) <= 64:
                raise KnowledgeBinaryParseError("解析器版本不合法")
            blocks = tuple(
                Block(text=block["text"], page=block.get("page"), paragraph=block.get("paragraph"))
                for block in response["blocks"]
            )
            for block in blocks:
                if not isinstance(block.text, str) or any(
                    pos is not None and (type(pos) is not int or not 0 < pos <= 2_147_483_647)
                    for pos in (block.page, block.paragraph)
                ):
                    raise KnowledgeBinaryParseError("文档片段内容或来源位置不合法")
            return build_content(Parsed(version, blocks))
        except (KeyError, TypeError) as exc:
            raise KnowledgeBinaryParseError("解析器响应不合法") from exc


def serve(
    *,
    socket_path: str | Path,
    max_requests: int | None = None,
    parse_timeout_seconds: float = _DEFAULT_PARSE_TIMEOUT_SECONDS,
) -> None:
    if parse_timeout_seconds <= 0:
        raise ValueError("解析超时必须大于 0 秒")
    path = Path(socket_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not stat.S_ISSOCK(path.lstat().st_mode):
            raise RuntimeError("解析器 Socket 路径已被普通文件占用")
        path.unlink()
    handled = 0
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(str(path))
        path.chmod(0o660)
        server.listen(4)
        while max_requests is None or handled < max_requests:
            connection, _ = server.accept()
            with connection:
                connection.settimeout(parse_timeout_seconds)
                _handle_connection(
                    connection,
                    parse_timeout_seconds=parse_timeout_seconds,
                )
            handled += 1
    finally:
        server.close()
        if path.exists() and stat.S_ISSOCK(path.lstat().st_mode):
            path.unlink()


def _handle_connection(
    connection: socket.socket,
    *,
    parse_timeout_seconds: float,
) -> None:
    file_descriptor: int | None = None
    try:
        request, file_descriptor = _receive_request(connection)
        with os.fdopen(file_descriptor, "rb", closefd=True) as source:
            file_descriptor = None
            result = _parse_with_timeout(
                source,
                extension=str(request.get("extension") or ""),
                timeout_seconds=parse_timeout_seconds,
            )
        response = {
            "ok": True,
            "parser_version": result.parser_version,
            "blocks": [asdict(block) for block in result.blocks],
        }
    except PdfNeedsOcr as exc:
        response = {"ok": False, "code": exc.code, "pages": exc.pages, "page_count": exc.page_count}
    except KnowledgeError as exc:
        response = {"ok": False, "code": exc.code, "message": str(exc), "retryable": exc.retryable}
    except Exception:
        response = {"ok": False, "code": "INVALID_DOCUMENT", "message": "文档无法解析"}
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
    payload = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode()
    if len(payload) > _MAX_RESPONSE_BYTES:
        payload = b'{"ok":false,"message":"parser response exceeds limit"}'
    try:
        connection.sendall(_LENGTH.pack(len(payload)) + payload)
    except (BrokenPipeError, ConnectionResetError, TimeoutError):
        # 调用方可能已取消任务，独立进程继续接收后续请求。
        return


def _parse_with_timeout(source: BinaryIO, *, extension: str, timeout_seconds: float) -> Parsed:
    def raise_timeout(_signum: int, _frame: Any) -> None:
        raise KnowledgeError("PARSE_TIMEOUT", "文档解析超时", retryable=True)

    previous_handler = signal.signal(signal.SIGALRM, raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return parse_document(source, extension=extension)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _receive_request(connection: socket.socket) -> tuple[dict[str, Any], int]:
    data, ancillary, _, _ = connection.recvmsg(
        _MAX_REQUEST_BYTES + _LENGTH.size,
        socket.CMSG_SPACE(array.array("i").itemsize),
    )
    file_descriptors = array.array("i")
    for level, message_type, message in ancillary:
        if level == socket.SOL_SOCKET and message_type == socket.SCM_RIGHTS:
            usable = len(message) - (len(message) % file_descriptors.itemsize)
            file_descriptors.frombytes(message[:usable])
    if not file_descriptors:
        raise KnowledgeBinaryParseError("解析请求缺少只读文件描述符")
    file_descriptor = file_descriptors[0]
    for extra in file_descriptors[1:]:
        os.close(extra)
    try:
        if fcntl.fcntl(file_descriptor, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY:
            raise KnowledgeBinaryParseError("解析请求必须提供只读文件描述符")
        if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
            raise KnowledgeBinaryParseError("解析请求必须提供普通文件")
        while len(data) < _LENGTH.size:
            chunk = connection.recv(_LENGTH.size - len(data))
            if not chunk:
                raise KnowledgeBinaryParseError("解析请求提前关闭")
            data += chunk
        size = _LENGTH.unpack(data[: _LENGTH.size])[0]
        if size > _MAX_REQUEST_BYTES:
            raise KnowledgeBinaryParseError("解析请求超过限制")
        body = data[_LENGTH.size :]
        while len(body) < size:
            chunk = connection.recv(size - len(body))
            if not chunk:
                raise KnowledgeBinaryParseError("解析请求提前关闭")
            body += chunk
        request = json.loads(body[:size])
        if not isinstance(request, dict):
            raise KnowledgeBinaryParseError("解析请求不合法")
        return request, file_descriptor
    except Exception:
        os.close(file_descriptor)
        raise


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = connection.recv(size - len(result))
        if not chunk:
            raise KnowledgeBinaryParseError("解析器连接提前关闭")
        result.extend(chunk)
    return bytes(result)


def parse_document(source: BinaryIO, *, extension: str) -> Parsed:
    filename = f"source.{extension.lstrip('.').lower()}"
    fmt = classify(filename)
    if fmt.media_type != "document":
        raise KnowledgeError("UNSUPPORTED_FORMAT", "不支持此文档格式")
    raw = source.read(fmt.max_bytes + 1)
    if len(raw) > fmt.max_bytes:
        raise KnowledgeError("FILE_TOO_LARGE", "文档超过大小限制")
    return parse_document_bytes(filename, raw, policy=material_document_policy())


def verify_isolated_runtime() -> None:
    if os.getuid() == 0:
        raise RuntimeError("解析器禁止以 root 运行")
    leaked = sorted(
        name
        for name in os.environ
        if name in _FORBIDDEN_ENVIRONMENT_NAMES or name.startswith(_FORBIDDEN_ENVIRONMENT_PREFIXES)
    )
    if leaked:
        raise RuntimeError("解析器环境包含业务凭证变量")
    if sys.platform.startswith("linux"):
        ipv4_routes = Path("/proc/net/route").read_text().splitlines()[1:]
        ipv6_routes = Path("/proc/net/ipv6_route").read_text().splitlines()
        if any(route.strip() for route in ipv4_routes) or any(
            route.split()[-1] != "lo" for route in ipv6_routes if route.split()
        ):
            raise RuntimeError("解析器必须在无网络命名空间运行")


def main() -> None:
    parser = argparse.ArgumentParser(description="AI Call isolated knowledge parser")
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--require-isolation", action="store_true")
    parser.add_argument(
        "--parse-timeout-seconds",
        type=float,
        default=_DEFAULT_PARSE_TIMEOUT_SECONDS,
    )
    arguments = parser.parse_args()
    if arguments.require_isolation:
        verify_isolated_runtime()
    serve(
        socket_path=arguments.socket,
        parse_timeout_seconds=arguments.parse_timeout_seconds,
    )


if __name__ == "__main__":
    main()
