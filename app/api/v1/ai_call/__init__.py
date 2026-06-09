from fastapi import APIRouter

AiCallRouter = APIRouter(prefix="/ai-call", tags=["智能外呼"])


@AiCallRouter.get("/health", summary="智能外呼模块健康检查")
async def ai_call_health() -> dict[str, str]:
    return {"status": "ok"}


__all__ = ["AiCallRouter"]
