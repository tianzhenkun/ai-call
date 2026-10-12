import re
from typing import Literal, get_args
from urllib.parse import urlsplit

OmniRealtimeModel = Literal[
    "qwen3.5-omni-plus-realtime",
    "qwen3.5-omni-flash-realtime",
    "qwen3.8-omni-flash-realtime",
]
OMNI_REALTIME_MODELS = get_args(OmniRealtimeModel)


def resolve_realtime_url(model: str, realtime_url: str, workspace_realtime_url: str = "") -> str:
    if model != "qwen3.8-omni-flash-realtime":
        return realtime_url
    url = workspace_realtime_url.strip()
    parts = urlsplit(url)
    if (
        parts.scheme != "wss"
        or not re.fullmatch(r"[a-z0-9-]+\.(cn-beijing|ap-southeast-1)\.maas\.aliyuncs\.com", parts.netloc)
        or parts.path != "/api-ws/v1/realtime"
        or parts.query or parts.fragment
    ):
        raise ValueError("Omni3.8 Flash 需配置有效的业务空间地址 DASHSCOPE_WORKSPACE_REALTIME_URL")
    return url


def validate_realtime_model(
    model: object,
    *,
    entry_type: str,
    configured_model: str,
    vad_type: str,
    customer_speech_review_enabled: bool,
    workspace_realtime_url: str = "",
) -> None:
    if model is None:
        resolve_realtime_url(configured_model, "", workspace_realtime_url)
        return
    if entry_type not in {"web", "linphone", "direct_sip", "outbound", "sip_outbound"}:
        raise ValueError("会话级 model 仅支持浏览器或 SIP 通话")
    if model not in OMNI_REALTIME_MODELS:
        raise ValueError("model 仅支持 " + "、".join(OMNI_REALTIME_MODELS))
    if (
        configured_model not in OMNI_REALTIME_MODELS
        or vad_type != "server_vad"
        or customer_speech_review_enabled
    ):
        raise ValueError("模型对照需使用 Omni、server_vad 且关闭客户发言审核的服务基线")
    resolve_realtime_url(model, "", workspace_realtime_url)
