import pytest

from app.services.ai_call.providers.aliyun_qwen_realtime import AliyunQwenRealtimeProvider
from app.services.ai_call.realtime_model import validate_realtime_model
from app.services.ai_call.voice_profile import builtin_voice_profile_values

MODEL = "qwen3.8-omni-flash-realtime"
WORKSPACE_URL = "wss://test.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime"


@pytest.mark.parametrize("model", ["qwen3.5-omni-plus-realtime", "qwen3.5-omni-flash-realtime", MODEL])
def test_provider_routes_each_model_without_mutating_legacy_endpoint(model):
    legacy = "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"
    provider = AliyunQwenRealtimeProvider(
        realtime_url=legacy, api_key="test-key", model=model,
        workspace_realtime_url=WORKSPACE_URL,
    )
    assert provider._build_connect_url() == f"{WORKSPACE_URL if model == MODEL else legacy}?model={model}"


@pytest.mark.parametrize("endpoint", [
    "", "wss://dashscope.aliyuncs.com/api-ws/v1/realtime",
    "ws://test.cn-beijing.maas.aliyuncs.com/api-ws/v1/realtime",
    "wss://test.cn-beijing.maas.aliyuncs.com.evil.test/api-ws/v1/realtime",
    WORKSPACE_URL + "?model=other",
])
def test_omni38_requires_workspace_endpoint_before_call_allocation(endpoint):
    with pytest.raises(ValueError, match="业务空间"):
        validate_realtime_model(
            MODEL, entry_type="web", configured_model="qwen3.5-omni-plus-realtime",
            vad_type="server_vad", customer_speech_review_enabled=False,
            workspace_realtime_url=endpoint,
        )


def test_omni38_voices_follow_its_own_catalog():
    voices = {profile["voice"] for profile in builtin_voice_profile_values(MODEL)}
    assert {"Tina", "Zane", "Cici", "Eliška", "longanlingxin"} <= voices
    assert not voices & {"Sunnybobi", "Ethan", "Harvey"}
    legacy = {profile["voice"] for profile in builtin_voice_profile_values("qwen3.5-omni-plus-realtime")}
    assert {"Sunnybobi", "Ethan", "Harvey"} <= legacy
