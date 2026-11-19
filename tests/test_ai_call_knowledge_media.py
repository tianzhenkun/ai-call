import json
import wave
from datetime import UTC, datetime
from io import BytesIO
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from lingchen_knowledge import links
from lingchen_knowledge.models import Claim, Item, KnowledgeError, Original
from PIL import Image

from app.config.setting import Settings
from app.services.ai_call.knowledge import PlatformOssKnowledgeStore
from app.services.ai_call.knowledge_media import (
    build_knowledge_media_providers,
    discard_knowledge_audio,
)
from app.utils.minio_util import MinioUtil


def _claim(media_type, filename):
    now = datetime.now(UTC)
    original = Original("private/original", filename, "", 0, "")
    item = Item(
        id="42",
        tenant_id="7",
        scope="user",
        owner_user_id="1",
        workspace_id=None,
        name=filename,
        media_type=media_type,
        category="OTHER",
        note=None,
        original=original,
        status="processing",
        created_at=now,
        updated_at=now,
    )
    return Claim(item=item, run_no=2, token="lease", attempt=1)


def _store(*, https=True):
    return PlatformOssKnowledgeStore({
        "endpoint": "oss.example.com",
        "bucket_name": "private",
        "access_key": "test",
        "secret_key": "test-secret",
        "is_https": "Y" if https else "N",
    })


@pytest.mark.anyio
async def test_audio_sends_only_the_signed_transcoded_track_then_cleans_up(monkeypatch):
    uploaded = AsyncMock()
    removed = AsyncMock()
    monkeypatch.setattr(MinioUtil, "put_object", uploaded)
    monkeypatch.setattr(MinioUtil, "delete_object", removed)
    source = BytesIO()
    with wave.open(source, "wb") as wav:
        wav.setparams((1, 2, 16000, 0, "NONE", ""))
        wav.writeframes(bytes(32000))
    claim = _claim("audio", "recording.wav")
    store = _store()
    submitted_urls = []

    def respond(request):
        if request.method == "POST":
            submitted_urls.extend(json.loads(request.content)["input"]["file_urls"])
            return httpx.Response(200, json={"output": {"task_id": "task1"}})
        if request.url.host == "dashscope.aliyuncs.com":
            return httpx.Response(
                200,
                json={
                    "output": {
                        "task_status": "SUCCEEDED",
                        "results": [
                            {
                                "subtask_status": "SUCCEEDED",
                                "transcription_url": "https://dashscope-result-bj.oss-cn-beijing.aliyuncs.com/a.json",
                            }
                        ],
                    }
                },
            )
        assert "Authorization" not in request.headers
        return httpx.Response(
            200,
            json={
                "transcripts": [
                    {"sentences": [{"text": "样例", "begin_time": 25, "end_time": 720}]}
                ]
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        providers = build_knowledge_media_providers(
            Settings(ASR_API_KEY="test"), store, client=client
        )
        assert providers["video"] is providers["audio"]
        asr = providers["audio"]
        url = await asr.prepare(claim, source.getvalue(), "42-2")
        job = await asr.submit(url, "42-2")
        parsed = await asr.poll(job)

    object_key = "reach/knowledge-asr/7/42-2.mp3"
    assert uploaded.await_count == 1
    _, saved_key, mp3, mime = uploaded.await_args.args
    assert (saved_key, mime) == (object_key, "audio/mpeg")
    assert mp3.startswith(b"ID3") and mp3 != source.getvalue()
    assert submitted_urls == [url]
    signed = urlsplit(url)
    assert signed.path == f"/private/{object_key}" and signed.scheme == "https"
    query = parse_qs(signed.query)
    assert query["X-Amz-Expires"] == ["3600"] and query["X-Amz-Signature"]
    assert parsed and [(b.text, b.start_ms, b.end_ms) for b in parsed.blocks] == [("样例", 25, 720)]
    removed.assert_not_awaited()
    await discard_knowledge_audio(store, claim)
    removed.assert_awaited_once_with(store.config, object_key)


@pytest.mark.anyio
@pytest.mark.parametrize("bucket", [2, 27])
async def test_ocr_handles_images_and_scan_pages_with_shared_adapter(bucket, monkeypatch):
    image = BytesIO()
    Image.new("RGB", (12, 12)).save(image, format="JPEG")
    pdf = BytesIO()
    Image.new("RGB", (12, 12)).save(pdf, format="PDF")
    submitted = []

    def respond(request):
        if request.method == "POST":
            submitted.append(request.content)
            return httpx.Response(200, json={"data": {"jobId": "ocr1"}})
        if request.url.host == "paddleocr.aistudio-app.com":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "state": "done",
                        "resultUrl": {
                            "jsonUrl": f"https://paddleocr-store-{bucket}.bj.bcebos.com/result.jsonl"
                        },
                    }
                },
            )
        pytest.fail("任务客户端只应调用百度 OCR 接口")

    async def result(request):
        assert urlsplit(request.url).hostname == f"paddleocr-store-{bucket}.bj.bcebos.com"
        assert request.address == "8.8.8.8"
        body = {"result": {"layoutParsingResults": [
            {"markdown": {"text": "第一页"}}, {"markdown": {"text": "第二页"}},
        ]}}
        return links.LinkResponse(200, {}, json.dumps(body).encode())

    monkeypatch.setattr(links, "_default_dns", AsyncMock(return_value=[("8.8.8.8", 4)]))
    monkeypatch.setattr(links, "_default_transport", result)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        settings = Settings(
            AI_CALL_KNOWLEDGE_OCR_API_KEY="test", ASR_API_KEY="", DASHSCOPE_API_KEY=""
        )
        providers = build_knowledge_media_providers(settings, _store(), client=client)
        assert set(providers) == {"document", "image"}
        assert providers["document"] is providers["image"]
        for media, filename, content in [
            ("image", "image.jpg", image.getvalue()),
            ("document", "scan.pdf", pdf.getvalue()),
        ]:
            provider = providers[media]
            prepared = await provider.prepare(_claim(media, filename), content, "42-2")
            job = await provider.submit(prepared, "42-2")
            parsed = await provider.poll(job)
            assert parsed and [(b.text, b.page) for b in parsed.blocks] == [
                ("第一页", 1),
                ("第二页", 2),
            ]
    assert b"image/png" in submitted[0]
    assert b"application/pdf" in submitted[1] and pdf.getvalue() in submitted[1]


@pytest.mark.anyio
async def test_unconfigured_or_insecure_media_storage(monkeypatch):
    uploaded = AsyncMock()
    monkeypatch.setattr(MinioUtil, "put_object", uploaded)
    async with httpx.AsyncClient() as client:
        settings = Settings(AI_CALL_KNOWLEDGE_OCR_API_KEY="", ASR_API_KEY="", DASHSCOPE_API_KEY="")
        assert not build_knowledge_media_providers(settings, _store(), client=client)
        settings.DASHSCOPE_API_KEY = "test"
        providers = build_knowledge_media_providers(settings, _store(https=False), client=client)
        asr = providers["audio"]
        with pytest.raises(KnowledgeError, match="HTTPS"):
            await asr.stage_audio(_claim("audio", "sample.mp3"), "42-2", b"sample")
    uploaded.assert_not_awaited()
