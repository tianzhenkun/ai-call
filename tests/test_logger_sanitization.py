from app.core.logger import sanitize_log_message


def test_sanitize_log_message_redacts_signed_url_query() -> None:
    message = (
        "HTTP Request: GET https://example.test/result.json?"
        "Expires=123&OSSAccessKeyId=public-id&Signature=temporary-signature"
    )

    sanitized = sanitize_log_message(message)

    assert sanitized == (
        "HTTP Request: GET https://example.test/result.json?<redacted>"
    )
    assert "public-id" not in sanitized
    assert "temporary-signature" not in sanitized


def test_sanitize_log_message_preserves_ordinary_url_query() -> None:
    message = "HTTP Request: GET https://example.test/search?page=2"

    assert sanitize_log_message(message) == message
