from __future__ import annotations

from io import BytesIO
from multiprocessing import get_context
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from lingchen_knowledge.documents import PdfNeedsOcr
from lingchen_knowledge.models import KnowledgeError

from app.services.ai_call import knowledge_binary_parser
from app.services.ai_call.knowledge_binary_parser import (
    KnowledgeBinaryParserClient,
    parse_document,
    serve,
)


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
            f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream",
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


def test_sidecar_rejects_pptx() -> None:
    with pytest.raises(KnowledgeError) as error:
        parse_document(BytesIO(b"unsupported"), extension="pptx")
    assert error.value.code == "UNSUPPORTED_FORMAT"


def test_docx_preserves_paragraphs_and_rejects_unsafe_parts() -> None:
    parsed = parse_document(_docx("产品能力", "交付周期"), extension="docx")
    assert parsed.parser_version == "docx-xml-v2"
    assert [(b.paragraph, b.text) for b in parsed.blocks] == [(1, "产品能力"), (2, "交付周期")]
    with pytest.raises(KnowledgeError, match="不安全"):
        parse_document(
            _docx("产品能力", extra_files={"word/vbaProject.bin": b"macro"}), extension="docx"
        )


def test_pdf_preserves_pages_and_signals_scans() -> None:
    parsed = parse_document(
        _pdf("Product capability includes delivery.", "The delivery time is five business days."),
        extension="pdf",
    )
    assert parsed.parser_version == "pypdf-v1"
    assert [block.page for block in parsed.blocks] == [1, 2]
    with pytest.raises(PdfNeedsOcr) as error:
        parse_document(_pdf(""), extension="pdf")
    assert error.value.pages == (1,)


def test_parser_enforces_processing_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def hang(_source: BytesIO, *, extension: str) -> dict[str, object]:
        sleep(1)
        return {"extension": extension}

    monkeypatch.setattr(knowledge_binary_parser, "parse_document", hang)
    started_at = monotonic()

    with pytest.raises(KnowledgeError, match="解析超时"):
        knowledge_binary_parser._parse_with_timeout(
            BytesIO(),
            extension="docx",
            timeout_seconds=0.02,
        )

    assert monotonic() - started_at < 0.5


def test_unix_socket_parser_receives_read_only_file_descriptor() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        socket_path = root / "parser.sock"
        source_path = root / "source.docx"
        source_path.write_bytes(_docx("来自独立解析服务").getvalue())
        server = get_context("spawn").Process(
            target=serve,
            kwargs={"socket_path": socket_path, "max_requests": 1},
        )
        server.daemon = True
        server.start()
        deadline = monotonic() + 2
        while not socket_path.exists() and monotonic() < deadline:
            sleep(0.01)

        result = KnowledgeBinaryParserClient(socket_path, timeout_seconds=2).parse(
            source_path,
            extension="docx",
        )
        server.join(timeout=2)

    assert not server.is_alive()
    assert result.chunks[0].text == "来自独立解析服务"
    assert result.chunks[0].locator["paragraph"] == 1
