from __future__ import annotations

from io import BytesIO
from multiprocessing import get_context
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from app.services.ai_call import knowledge_binary_parser
from app.services.ai_call.knowledge_binary_parser import (
    KnowledgeBinaryParseError,
    KnowledgeBinaryParserClient,
    parse_document,
    serve,
)


def _pptx(*slides: str, extra_files: dict[str, bytes] | None = None) -> BytesIO:
    payload = BytesIO()
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        )
        for index, text in enumerate(slides, start=1):
            archive.writestr(
                f"ppt/slides/slide{index}.xml",
                (
                    '<p:sld xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" '
                    'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">'
                    f"<p:cSld><a:p><a:r><a:t>{text}</a:t></a:r></a:p></p:cSld></p:sld>"
                ),
            )
        for name, content in (extra_files or {}).items():
            archive.writestr(name, content)
    payload.seek(0)
    return payload


def _docx(*paragraphs: str, extra_files: dict[str, bytes] | None = None) -> BytesIO:
    payload = BytesIO()
    body = "".join(f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>" for text in paragraphs)
    with ZipFile(payload, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"/>',
        )
        archive.writestr(
            "word/document.xml",
            (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/'
                f'wordprocessingml/2006/main"><w:body>{body}</w:body></w:document>'
            ),
        )
        for name, content in (extra_files or {}).items():
            archive.writestr(name, content)
    payload.seek(0)
    return payload


def _pdf(*pages: str) -> BytesIO:
    page_ids = [4 + index * 2 for index in range(len(pages))]
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        f"<< /Type /Pages /Kids [{' '.join(f'{page_id} 0 R' for page_id in page_ids)}] /Count {len(pages)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    for page_id, text in zip(page_ids, pages, strict=True):
        content_id = page_id + 1
        escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        stream = f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode()
        objects.extend([
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
            ).encode(),
            f"<< /Length {len(stream)} >>\nstream\n".encode()
            + stream
            + b"\nendstream",
        ])

    payload = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for number, body in enumerate(objects, start=1):
        offsets.append(len(payload))
        payload.extend(f"{number} 0 obj\n".encode() + body + b"\nendobj\n")
    xref_offset = len(payload)
    payload.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    payload.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        payload.extend(f"{offset:010d} 00000 n \n".encode())
    payload.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n".encode()
    )
    return BytesIO(payload)


def test_pptx_parser_returns_deterministic_page_citations() -> None:
    first = parse_document(_pptx("退款将在审核通过后原路退回", "交付周期为两周"), extension="pptx")
    second = parse_document(_pptx("退款将在审核通过后原路退回", "交付周期为两周"), extension=".PPTX")

    assert first == second
    assert first["parserName"] == "pptx"
    assert first["parserVersion"] == "pptx-ooxml-stdlib-v1"
    assert first["chunkStrategyVersion"] == "pptx-slide-semantic-900-1200-v1"
    assert [chunk["pageNo"] for chunk in first["chunks"]] == [1, 2]
    assert [chunk["sourcePath"] for chunk in first["chunks"]] == ["slides/1", "slides/2"]
    assert [chunk["content"] for chunk in first["chunks"]] == [
        "退款将在审核通过后原路退回",
        "交付周期为两周",
    ]


def test_pptx_parser_keeps_sentence_boundaries_when_splitting_long_slides() -> None:
    result = parse_document(_pptx("abcdefghij。" * 150), extension="pptx")

    assert len(result["chunks"]) == 2
    assert all(len(chunk["content"]) <= 1200 for chunk in result["chunks"])
    assert result["chunks"][0]["content"].endswith("。")


@pytest.mark.parametrize(
    "unsafe_name",
    [
        "ppt/vbaProject.bin",
        "ppt/activeX/activeX1.bin",
    ],
)
def test_pptx_parser_rejects_active_or_embedded_content(unsafe_name: str) -> None:
    source = _pptx("正常正文", extra_files={unsafe_name: b"unsafe"})

    with pytest.raises(KnowledgeBinaryParseError, match="活动内容"):
        parse_document(source, extension="pptx")


def test_pptx_parser_ignores_embedded_objects_without_opening_them() -> None:
    source = _pptx(
        "只提取幻灯片正文",
        extra_files={"ppt/embeddings/oleObject1.bin": b"not a real document"},
    )

    result = parse_document(source, extension="pptx")

    assert result["chunks"][0]["content"] == "只提取幻灯片正文"


def test_docx_parser_returns_paragraph_citations() -> None:
    result = parse_document(
        _docx("Refunds return to the original account.", "Delivery takes two weeks."),
        extension="docx",
    )

    assert result["parserName"] == "docx"
    assert result["parserVersion"] == "docx-ooxml-stdlib-v1"
    assert result["chunkStrategyVersion"] == "docx-paragraph-900-1200-v1"
    assert result["chunks"][0]["pageNo"] is None
    assert result["chunks"][0]["sourcePath"] == "word/document.xml#paragraphs/1-2"
    assert "Delivery takes two weeks." in result["chunks"][0]["content"]


def test_docx_parser_rejects_active_content() -> None:
    source = _docx("Safe text", extra_files={"word/vbaProject.bin": b"unsafe"})

    with pytest.raises(KnowledgeBinaryParseError, match="活动内容"):
        parse_document(source, extension="docx")


def test_pdf_parser_returns_page_citations() -> None:
    result = parse_document(_pdf("Refund policy", "Delivery timeline"), extension="pdf")

    assert result["parserName"] == "pdf"
    assert result["parserVersion"] == "pdf-pypdf-6.16.1-v1"
    assert result["chunkStrategyVersion"] == "pdf-page-semantic-900-1200-v1"
    assert [chunk["pageNo"] for chunk in result["chunks"]] == [1, 2]
    assert [chunk["sourcePath"] for chunk in result["chunks"]] == ["pages/1", "pages/2"]


def test_pdf_parser_rejects_document_without_text() -> None:
    with pytest.raises(KnowledgeBinaryParseError, match="没有可用正文"):
        parse_document(_pdf(""), extension="pdf")


def test_pptx_parser_rejects_extreme_zip_expansion() -> None:
    source = _pptx("A" * (2 * 1024 * 1024))

    with pytest.raises(KnowledgeBinaryParseError, match="解压限制"):
        parse_document(source, extension="pptx")


def test_pptx_parser_rejects_zip_without_ooxml_manifest() -> None:
    source = BytesIO()
    with ZipFile(source, "w", ZIP_DEFLATED) as archive:
        archive.writestr("ppt/slides/slide1.xml", "<slide />")
    source.seek(0)

    with pytest.raises(KnowledgeBinaryParseError, match="OOXML"):
        parse_document(source, extension="pptx")


def test_parser_enforces_processing_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def hang(_source: BytesIO, *, extension: str) -> dict[str, object]:
        sleep(1)
        return {"extension": extension}

    monkeypatch.setattr(knowledge_binary_parser, "parse_document", hang)
    started_at = monotonic()

    with pytest.raises(KnowledgeBinaryParseError, match="解析超时"):
        knowledge_binary_parser._parse_with_timeout(
            BytesIO(),
            extension="pptx",
            timeout_seconds=0.02,
        )

    assert monotonic() - started_at < 0.5


def test_unix_socket_parser_receives_read_only_file_descriptor() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        socket_path = root / "parser.sock"
        source_path = root / "source.pptx"
        source_path.write_bytes(_pptx("来自独立解析服务").getvalue())
        server = get_context("spawn").Process(
            target=serve,
            kwargs={"socket_path": socket_path, "max_requests": 1},
        )
        server.start()
        deadline = monotonic() + 2
        while not socket_path.exists() and monotonic() < deadline:
            sleep(0.01)

        result = KnowledgeBinaryParserClient(socket_path, timeout_seconds=2).parse(
            source_path,
            extension="pptx",
        )
        server.join(timeout=2)

    assert not server.is_alive()
    assert result["chunks"][0]["content"] == "来自独立解析服务"
