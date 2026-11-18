from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING

import httpx
from lingchen_knowledge.integrations.media import DashScopeASR, PaddleOCR
from lingchen_knowledge.models import Claim, KnowledgeError, MediaProvider, MediaType

from app.core.logger import log

if TYPE_CHECKING:
    from app.config.setting import Settings
    from app.services.ai_call.knowledge import KnowledgeObjectStore


def _audio_key(claim: Claim) -> str:
    return f"knowledge-asr/{claim.item.tenant_id}/{claim.item.id}-{claim.run_no}.mp3"


async def discard_knowledge_audio(store: KnowledgeObjectStore, claim: Claim) -> None:
    if claim.item.media_type in {"audio", "video"}:
        try:
            await store.delete(_audio_key(claim))
        except Exception as exc:
            # 回收发生在解析状态提交后，保留已保存的结果和可定位的失败日志。
            log.warning(
                "知识库转写临时文件回收失败 version={} run={} error={}",
                claim.item.id,
                claim.run_no,
                type(exc).__name__,
            )


def build_knowledge_media_providers(
    settings: Settings, store: KnowledgeObjectStore, *, client: httpx.AsyncClient
) -> dict[MediaType, MediaProvider]:
    providers: dict[MediaType, MediaProvider] = {}
    if key := settings.AI_CALL_KNOWLEDGE_OCR_API_KEY.strip():
        ocr = PaddleOCR(client, key=key)
        providers.update(document=ocr, image=ocr)

    if key := settings.EFFECTIVE_ASR_API_KEY.strip():

        async def stage_audio(claim: Claim, _request_key: str, speech: bytes) -> str:
            object_key = _audio_key(claim)
            try:
                url = await store.presigned_get_url(object_key, expires_seconds=3600)
                if not url.startswith("https://"):
                    raise KnowledgeError(
                        "ASR_STAGING_UNAVAILABLE", "转写音频需要可访问的 HTTPS 对象存储地址"
                    )
                await store.put(
                    object_key,
                    BytesIO(speech),
                    content_type="audio/mpeg",
                    expected_size=len(speech),
                )
            except KnowledgeError:
                raise
            except Exception as exc:
                raise KnowledgeError(
                    "ASR_STAGING_UNAVAILABLE", "暂时无法准备转写音频", retryable=True
                ) from exc
            return url

        asr = DashScopeASR(
            client,
            key=key,
            result_hosts=("dashscope-result-bj.oss-cn-beijing.aliyuncs.com",),
            stage_audio=stage_audio,
        )
        providers.update(audio=asr, video=asr)
    return providers
