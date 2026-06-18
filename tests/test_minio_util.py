from app.api.v1.system.oss.service import OssService
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
