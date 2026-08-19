from __future__ import annotations

import argparse
import array
import fcntl
import hashlib
import json
import os
import re
import signal
import socket
import stat
import struct
import sys
import unicodedata
import zipfile
from pathlib import Path
from typing import Any, BinaryIO
from xml.etree import ElementTree

PPTX_PARSER_VERSION = "pptx-ooxml-stdlib-v1"
PPTX_CHUNK_STRATEGY_VERSION = "pptx-slide-semantic-900-1200-v1"

_TEXT_TAG = "{http://schemas.openxmlformats.org/drawingml/2006/main}t"
_PARAGRAPH_TAG = "{http://schemas.openxmlformats.org/drawingml/2006/main}p"
_SLIDE_RE = re.compile(r"ppt/slides/slide(\d+)\.xml$")
_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[。！？!?；;])|\n+")
_TARGET_CHARS = 900
_MAX_CHARS = 1200
_UNSAFE_PARTS = ("ppt/vbaproject.bin", "ppt/activex/")
_MAX_ARCHIVE_ENTRIES = 10_000
_MAX_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
_MAX_COMPRESSION_RATIO = 200
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
_FORBIDDEN_ENVIRONMENT_NAMES = {"SECRET_KEY", "JWT_SECRET", "TOKEN"}


class KnowledgeBinaryParseError(ValueError):
    pass


class KnowledgeBinaryParserClient:
    def __init__(self, socket_path: str | Path, *, timeout_seconds: float = 30.0) -> None:
        self.socket_path = Path(socket_path)
        self.timeout_seconds = timeout_seconds

    def parse(self, source_path: str | Path, *, extension: str) -> dict[str, Any]:
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
            response = json.loads(_recv_exact(connection, response_size))
        if not isinstance(response, dict) or response.get("ok") is not True:
            message = response.get("message") if isinstance(response, dict) else None
            raise KnowledgeBinaryParseError(str(message or "二进制文件解析失败"))
        result = response.get("result")
        if not isinstance(result, dict):
            raise KnowledgeBinaryParseError("解析器响应不合法")
        return result


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
        response = {"ok": True, "result": result}
    except (KnowledgeBinaryParseError, zipfile.BadZipFile, ElementTree.ParseError) as exc:
        response = {"ok": False, "message": str(exc) or "二进制文件解析失败"}
    except Exception:
        response = {"ok": False, "message": "二进制文件解析失败"}
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
    payload = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode()
    if len(payload) > _MAX_RESPONSE_BYTES:
        payload = b'{"ok":false,"message":"parser response exceeds limit"}'
    connection.sendall(_LENGTH.pack(len(payload)) + payload)


def _parse_with_timeout(
    source: BinaryIO,
    *,
    extension: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    def raise_timeout(_signum: int, _frame: Any) -> None:
        raise KnowledgeBinaryParseError("二进制文件解析超时")

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


def parse_document(source: BinaryIO, *, extension: str) -> dict[str, Any]:
    normalized_extension = extension.strip().lower().lstrip(".")
    if normalized_extension != "pptx":
        raise KnowledgeBinaryParseError("当前二进制解析器只支持 PPTX")

    chunks: list[dict[str, Any]] = []
    with zipfile.ZipFile(source) as archive:
        entries = archive.infolist()
        if (
            len(entries) > _MAX_ARCHIVE_ENTRIES
            or sum(entry.file_size for entry in entries) > _MAX_UNCOMPRESSED_BYTES
            or any(
                entry.file_size > 1024 * 1024
                and entry.file_size / max(entry.compress_size, 1) > _MAX_COMPRESSION_RATIO
                for entry in entries
            )
        ):
            raise KnowledgeBinaryParseError("PPTX 超过安全解压限制")
        names = archive.namelist()
        if "[Content_Types].xml" not in names:
            raise KnowledgeBinaryParseError("PPTX 缺少 OOXML 清单")
        if any(entry.flag_bits & 0x1 for entry in entries):
            raise KnowledgeBinaryParseError("PPTX 不支持加密成员")
        if any(
            normalized == _UNSAFE_PARTS[0]
            or normalized.startswith(_UNSAFE_PARTS[1:])
            for normalized in map(str.lower, names)
        ):
            raise KnowledgeBinaryParseError("PPTX 包含活动内容")
        slide_names = sorted(
            (name for name in names if _SLIDE_RE.fullmatch(name)),
            key=lambda name: int(_SLIDE_RE.fullmatch(name).group(1)),  # type: ignore[union-attr]
        )
        for name in slide_names:
            page_no = int(_SLIDE_RE.fullmatch(name).group(1))  # type: ignore[union-attr]
            root = ElementTree.fromstring(archive.read(name))
            paragraphs = []
            for paragraph in root.iter(_PARAGRAPH_TAG):
                text = _normalized_text(
                    "".join(node.text or "" for node in paragraph.iter(_TEXT_TAG))
                )
                if text:
                    paragraphs.append(text)
            for content in _split_long_text("\n".join(paragraphs)):
                chunks.append(_chunk(len(chunks), content, page_no))
    if not chunks:
        raise KnowledgeBinaryParseError("PPTX 没有可用正文")

    chunk_set_sha256 = hashlib.sha256(
        "".join(
            f"{chunk['chunkIndex']}:{chunk['contentChecksum']}\n" for chunk in chunks
        ).encode()
    ).hexdigest()
    return {
        "parserName": "pptx",
        "parserVersion": PPTX_PARSER_VERSION,
        "chunkStrategyVersion": PPTX_CHUNK_STRATEGY_VERSION,
        "chunkSetSha256": chunk_set_sha256,
        "chunks": chunks,
    }


def _normalized_text(value: str) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value)).strip()


def _split_long_text(value: str) -> list[str]:
    if len(value) <= _MAX_CHARS:
        return [value] if value else []
    parts = [part.strip() for part in _SENTENCE_BOUNDARY_RE.split(value) if part.strip()]
    chunks: list[str] = []
    current = ""
    for part in parts:
        while len(part) > _MAX_CHARS:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(part[:_MAX_CHARS])
            part = part[_MAX_CHARS:]
        candidate = f"{current}\n{part}".strip() if current else part
        if current and len(candidate) > _MAX_CHARS:
            chunks.append(current)
            current = part
        else:
            current = candidate
        if len(current) >= _TARGET_CHARS:
            chunks.append(current)
            current = ""
    if current:
        chunks.append(current)
    return chunks


def _chunk(index: int, content: str, page_no: int) -> dict[str, Any]:
    return {
        "chunkIndex": index,
        "content": content,
        "contentChecksum": hashlib.sha256(content.encode()).hexdigest(),
        "contentType": "TEXT",
        "sourceType": "PPTX",
        "pageNo": page_no,
        "sectionPath": None,
        "sourcePath": f"slides/{page_no}",
        "startMs": None,
        "endMs": None,
    }


def verify_isolated_runtime() -> None:
    if os.getuid() == 0:
        raise RuntimeError("解析器禁止以 root 运行")
    leaked = sorted(
        name
        for name in os.environ
        if name in _FORBIDDEN_ENVIRONMENT_NAMES
        or name.startswith(_FORBIDDEN_ENVIRONMENT_PREFIXES)
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
