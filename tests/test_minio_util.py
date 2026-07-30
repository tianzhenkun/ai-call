import datetime as dt
import hashlib
import hmac
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

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


def test_presigned_get_url_allows_private_object_download_without_exposing_secret():
    config = {
        "is_https": "Y",
        "endpoint": "minio.example.com",
        "bucket_name": "recov",
        "access_key": "public-key",
        "secret_key": "private-secret",
        "region": "us-east-1",
    }

    url = MinioUtil.presigned_get_url(
        config,
        "ai-call/recordings/call-1.mp3",
        expires_seconds=900,
        now=datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc),
    )

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert parsed.netloc == "minio.example.com"
    assert parsed.path == "/recov/ai-call/recordings/call-1.mp3"
    assert query["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert query["X-Amz-Expires"] == ["900"]
    assert query["X-Amz-SignedHeaders"] == ["host"]
    assert len(query["X-Amz-Signature"][0]) == 64
    assert "private-secret" not in url


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


def _test_hmac_sha256(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def _expected_authorization(request: httpx.Request, *, secret_key: str) -> str:
    amz_date = request.headers["x-amz-date"]
    date_stamp = amz_date[:8]
    region = "cn-north-1"
    payload_hash = hashlib.sha256(b"").hexdigest()
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_headers = (
        f"host:{request.headers['host']}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    canonical_request = "\n".join(
        [
            request.method,
            request.url.raw_path.decode("ascii"),
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signing_key = _test_hmac_sha256(f"AWS4{secret_key}".encode(), date_stamp)
    signing_key = _test_hmac_sha256(signing_key, region)
    signing_key = _test_hmac_sha256(signing_key, "s3")
    signing_key = _test_hmac_sha256(signing_key, "aws4_request")
    signature = hmac.new(
        signing_key,
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    return (
        f"AWS4-HMAC-SHA256 Credential=public-key/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )


def _expected_put_authorization(request: httpx.Request, *, secret_key: str) -> str:
    amz_date = request.headers["x-amz-date"]
    date_stamp = amz_date[:8]
    region = "cn-north-1"
    payload_hash = hashlib.sha256(request.content).hexdigest()
    signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
    canonical_headers = (
        f"content-type:{request.headers['content-type']}\n"
        f"host:{request.headers['host']}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    canonical_request = "\n".join(
        [
            request.method,
            request.url.raw_path.decode("ascii"),
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signing_key = _test_hmac_sha256(f"AWS4{secret_key}".encode(), date_stamp)
    signing_key = _test_hmac_sha256(signing_key, region)
    signing_key = _test_hmac_sha256(signing_key, "s3")
    signing_key = _test_hmac_sha256(signing_key, "aws4_request")
    signature = hmac.new(
        signing_key,
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    return (
        f"AWS4-HMAC-SHA256 Credential=public-key/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )


@pytest.mark.anyio
async def test_put_object_uses_exact_key_body_content_type_and_sigv4() -> None:
    requests: list[httpx.Request] = []
    object_key = "ai-call/voice-samples/tenant-digest/123.wav"
    signed_at = dt.datetime(2026, 7, 30, 1, 2, 3, tzinfo=dt.timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    await MinioUtil.put_object(
        _private_object_config(),
        object_key,
        b"sample-body",
        "audio/wav",
        transport=httpx.MockTransport(handler),
        now=signed_at,
    )

    request = requests[0]
    assert request.method == "PUT"
    assert request.url.path == f"/recov/{object_key}"
    assert request.content == b"sample-body"
    assert request.headers["content-type"] == "audio/wav"
    assert request.headers["x-amz-content-sha256"] == hashlib.sha256(
        b"sample-body"
    ).hexdigest()
    assert request.headers["authorization"] == _expected_put_authorization(
        request,
        secret_key="private-secret",
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method_name", "http_method"),
    [("get_object", "GET"), ("delete_object", "DELETE")],
)
async def test_private_object_signature_uses_normalized_http_host_and_raw_path(
    method_name: str,
    http_method: str,
) -> None:
    requests: list[httpx.Request] = []
    config = {
        **_private_object_config(),
        "endpoint": "MINIO.EXAMPLE.COM:443",
    }
    signed_at = dt.datetime(2026, 7, 30, 1, 2, 3, tzinfo=dt.timezone.utc)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"sample")

    await getattr(MinioUtil, method_name)(
        config,
        "ai-call/voice-samples/中文 sample.wav",
        transport=httpx.MockTransport(handler),
        now=signed_at,
    )

    request = requests[0]
    assert request.method == http_method
    assert request.headers["host"] == "minio.example.com"
    assert request.headers["x-amz-date"] == "20260730T010203Z"
    assert request.headers["authorization"] == _expected_authorization(
        request,
        secret_key=config["secret_key"],
    )


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
