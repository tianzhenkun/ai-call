import pytest

from app.services.ai_call.offline_asr_service import (
    DashScopeParaformerAsrProvider,
    DashScopeQwenFileTranscriptionAsrProvider,
    OfflineAsrSegment,
    build_dashscope_offline_asr_provider,
)


def provider_kwargs() -> dict:
    return {
        "api_key": "test-key",
        "language_hints": ["zh"],
        "timeout_seconds": 30,
        "poll_interval_seconds": 1,
    }


def test_qwen_filetrans_builds_single_file_request() -> None:
    provider = DashScopeQwenFileTranscriptionAsrProvider(
        model="qwen3-asr-flash-filetrans",
        **provider_kwargs(),
    )

    assert provider._submit_payload("https://files.test/customer.ogg") == {
        "model": "qwen3-asr-flash-filetrans",
        "input": {"file_url": "https://files.test/customer.ogg"},
        "parameters": {
            "channel_id": [0],
            "language": "zh",
            "enable_itn": True,
            "enable_words": True,
        },
    }


def test_qwen_filetrans_parses_nested_result_url_and_sentences() -> None:
    task = {
        "output": {
            "result": {
                "transcription_url": "https://results.test/qwen.json",
            }
        }
    }
    transcript = {
        "transcripts": [
            {
                "sentences": [
                    {
                        "text": "转人工。",
                        "begin_time": 1200,
                        "end_time": 1880,
                    }
                ]
            }
        ]
    }

    assert (
        DashScopeQwenFileTranscriptionAsrProvider._transcription_url(task)
        == "https://results.test/qwen.json"
    )
    assert DashScopeQwenFileTranscriptionAsrProvider._parse_segments(transcript) == [
        OfflineAsrSegment(
            text="转人工。",
            begin_time_ms=1200,
            end_time_ms=1880,
        )
    ]


def test_paraformer_keeps_multiple_file_request_contract() -> None:
    provider = DashScopeParaformerAsrProvider(
        model="paraformer-v2",
        **provider_kwargs(),
    )

    assert provider._submit_payload("https://files.test/customer.ogg") == {
        "model": "paraformer-v2",
        "input": {"file_urls": ["https://files.test/customer.ogg"]},
        "parameters": {"language_hints": ["zh"]},
    }


@pytest.mark.parametrize(
    ("provider_name", "model", "provider_type"),
    [
        (
            "dashscope_qwen_filetrans",
            "qwen3-asr-flash-filetrans",
            DashScopeQwenFileTranscriptionAsrProvider,
        ),
        (
            "dashscope_paraformer",
            "paraformer-v2",
            DashScopeParaformerAsrProvider,
        ),
    ],
)
def test_provider_factory_selects_configured_provider(
    provider_name: str,
    model: str,
    provider_type: type,
) -> None:
    provider = build_dashscope_offline_asr_provider(
        provider_name=provider_name,
        model=model,
        **provider_kwargs(),
    )

    assert isinstance(provider, provider_type)


def test_provider_factory_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="不支持的离线 ASR provider: unknown"):
        build_dashscope_offline_asr_provider(
            provider_name="unknown",
            model="unknown",
            **provider_kwargs(),
        )
