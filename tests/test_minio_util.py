import httpx
import pytest

from app.api.v1.system.oss.service import OssService
from app.core.exceptions import CustomException
from app.utils.minio_util import MinioUtil


def test_build_url_keeps_configured_https_domain_and_adds_bucket():
    config = {
        "is_https": "N",
        "domain": "https://oss.lingchen-ai.com",
        "endpoint": "81.68.166.109:9000",
        "bucket_name": "recov",
    }

    url = MinioUtil._build_url(config, "2026/06/01/demo.png")

    assert url == "https://oss.lingchen-ai.com/recov/2026/06/01/demo.png"


def test_build_url_uses_https_flag_when_domain_has_no_scheme():
    config = {
        "is_https": "Y",
        "domain": "oss.lingchen-ai.com",
        "endpoint": "81.68.166.109:9000",
        "bucket_name": "recov",
    }

    url = MinioUtil._build_url(config, "2026/06/01/demo.pdf")

    assert url == "https://oss.lingchen-ai.com/recov/2026/06/01/demo.pdf"


def test_build_url_does_not_duplicate_bucket_when_domain_already_contains_bucket():
    config = {
        "is_https": "Y",
        "domain": "https://oss.lingchen-ai.com/recov",
        "endpoint": "81.68.166.109:9000",
        "bucket_name": "recov",
    }

    url = MinioUtil._build_url(config, "2026/06/01/demo.pdf")

    assert url == "https://oss.lingchen-ai.com/recov/2026/06/01/demo.pdf"


def test_oss_service_build_object_url_matches_upload_url_rule():
    config = {
        "is_https": "N",
        "domain": "https://oss.lingchen-ai.com",
        "endpoint": "81.68.166.109:9000",
        "bucket_name": "recov",
    }

    url = OssService.build_object_url(
        config,
        "ai-call/recordings/call_325209354604376064.mp4",
    )

    assert url == "https://oss.lingchen-ai.com/recov/ai-call/recordings/call_325209354604376064.mp4"


def _private_object_config() -> dict:
    return {
        "is_https": "Y",
        "endpoint": "minio.example.com",
        "bucket_name": "recov",
        "access_key": "public-key",
        "secret_key": "private-secret",
        "region": "cn-north-1",
    }


def _exception_chain_text(error: BaseException) -> str:
    pending = [error]
    seen: set[int] = set()
    messages: list[str] = []
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        messages.append(str(current))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return "\n".join(messages)


@pytest.mark.anyio
async def test_get_and_delete_object_use_signed_private_requests() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, content=b"sample")
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)
    data = await MinioUtil.get_object(
        _private_object_config(),
        "ai-call/voice-samples/中文 sample.wav",
        transport=transport,
    )
    await MinioUtil.delete_object(
        _private_object_config(),
        "ai-call/voice-samples/中文 sample.wav",
        transport=transport,
    )

    assert data == b"sample"
    assert [request.method for request in requests] == ["GET", "DELETE"]
    for request in requests:
        assert request.url.path == "/recov/ai-call/voice-samples/中文 sample.wav"
        assert request.headers["x-amz-content-sha256"] == (
            "e3b0c44298fc1c149afbf4c8996fb924"
            "27ae41e4649b934ca495991b7852b855"
        )
        assert request.headers["x-amz-date"]
        assert request.headers["authorization"].startswith(
            "AWS4-HMAC-SHA256 Credential=public-key/"
        )
        assert "private-secret" not in str(request.headers)


@pytest.mark.anyio
@pytest.mark.parametrize("status_code", [200, 204])
async def test_delete_object_accepts_success_statuses(status_code: int) -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(status_code, content=b"ignored")
    )

    await MinioUtil.delete_object(
        _private_object_config(),
        "ai-call/voice-samples/sample.wav",
        transport=transport,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method_name", "failure_message"),
    [("get_object", "MinIO读取对象失败"), ("delete_object", "MinIO删除对象失败")],
)
async def test_private_object_http_error_does_not_expose_secret(
    method_name: str,
    failure_message: str,
) -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(
            403,
            text="provider echoed private-secret",
        )
    )

    with pytest.raises(CustomException, match=failure_message) as error:
        await getattr(MinioUtil, method_name)(
            _private_object_config(),
            "ai-call/voice-samples/sample.wav",
            transport=transport,
        )

    assert "private-secret" not in str(error.value)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method_name", "failure_message"),
    [("get_object", "MinIO读取对象失败"), ("delete_object", "MinIO删除对象失败")],
)
async def test_private_object_network_error_clears_sensitive_exception_chain(
    method_name: str,
    failure_message: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection used private-secret", request=request)

    with pytest.raises(CustomException, match=failure_message) as error:
        await getattr(MinioUtil, method_name)(
            _private_object_config(),
            "ai-call/voice-samples/sample.wav",
            transport=httpx.MockTransport(handler),
        )

    assert "private-secret" not in _exception_chain_text(error.value)
