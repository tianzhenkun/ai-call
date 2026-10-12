from typing import Literal, get_args

OmniRealtimeModel = Literal[
    "qwen3.5-omni-plus-realtime",
    "qwen3.5-omni-flash-realtime",
]
OMNI_REALTIME_MODELS = get_args(OmniRealtimeModel)


def validate_realtime_model(
    model: object,
    *,
    entry_type: str,
    configured_model: str,
    vad_type: str,
    customer_speech_review_enabled: bool,
) -> None:
    if model is None:
        return
    if entry_type not in {"web", "linphone", "direct_sip", "outbound", "sip_outbound"}:
        raise ValueError("会话级 model 仅支持浏览器或 SIP 通话")
    if model not in OMNI_REALTIME_MODELS:
        raise ValueError("model 仅支持 qwen3.5-omni-plus-realtime 或 qwen3.5-omni-flash-realtime")
    if (
        configured_model not in OMNI_REALTIME_MODELS
        or vad_type != "server_vad"
        or customer_speech_review_enabled
    ):
        raise ValueError("模型对照需使用 Omni、server_vad 且关闭客户发言审核的服务基线")
