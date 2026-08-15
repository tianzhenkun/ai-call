from __future__ import annotations

import json
import re
from typing import Any, Protocol

import httpx

PROMPT_VARIABLE_RE = re.compile(r"\{\{([A-Za-z][A-Za-z0-9_]*)\}\}")
UNSUPPORTED_PROMISE_RE = re.compile(
    r"(已(?:经)?(?:添加|加了).*微信|已(?:经)?为您预约|"
    r"(?:稍后|之后).{0,8}(?:发|联系)|"
    r"(?:安排|会有).{0,8}(?:顾问|工作人员).{0,8}联系)"
)


class PromptOptimizerProtocol(Protocol):
    async def optimize(self, payload: dict[str, Any]) -> dict[str, Any]: ...


class OpenAICompatiblePromptOptimizer:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.model = model.strip()
        self.timeout_seconds = max(1.0, timeout_seconds)
        self.transport = transport

    async def optimize(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.base_url or not self.api_key or not self.model:
            raise ValueError("提示词 AI 优化服务未配置")
        request = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是 AI 外呼提示词编辑器。只输出 JSON 对象："
                        '{"candidateContent":"候选内容","warnings":[]}。'
                        "只能使用 allowedVariables 中的变量，保留原内容已有变量；"
                        "currentContent 非空时必须产生实质改进，不得原样返回；"
                        "不得虚构微信、预约、发送资料或安排顾问等系统未提供动作；"
                        "开场白不要生成停顿标记。"
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                },
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "enable_thinking": False,
        }
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds,
            transport=self.transport,
        ) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=request,
            )
        response.raise_for_status()
        result = self._parse_response(response.json())
        return self._validate_result(payload, result)

    @staticmethod
    def _parse_response(data: Any) -> dict[str, Any]:
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("AI 优化响应格式错误") from exc
        if not isinstance(content, str) or not content.strip():
            raise ValueError("AI 优化响应为空")
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()[1:]
            if lines and lines[-1].startswith("```"):
                lines.pop()
            text = "\n".join(lines).strip()
        result = json.loads(text)
        if not isinstance(result, dict):
            raise ValueError("AI 优化响应不是 JSON 对象")
        return result

    @staticmethod
    def _validate_result(
        payload: dict[str, Any],
        result: dict[str, Any],
    ) -> dict[str, Any]:
        candidate = result.get("candidateContent")
        if not isinstance(candidate, str) or not candidate.strip():
            raise ValueError("AI 优化候选内容为空")
        candidate = candidate.strip()
        current = str(payload.get("currentContent") or "").strip()
        if current and candidate == current:
            raise ValueError("AI 优化未产生有效变化")
        allowed = set(payload.get("allowedVariables") or [])
        current_variables = set(
            PROMPT_VARIABLE_RE.findall(str(payload.get("currentContent") or ""))
        )
        candidate_variables = set(PROMPT_VARIABLE_RE.findall(candidate))
        if not candidate_variables.issubset(allowed):
            raise ValueError("AI 优化生成了未定义变量")
        if not current_variables.issubset(candidate_variables):
            raise ValueError("AI 优化删除或修改了原有变量")
        if payload.get("targetType") == "opening" and "[停顿" in candidate:
            raise ValueError("开场白暂不支持精确停顿")
        if UNSUPPORTED_PROMISE_RE.search(candidate):
            raise ValueError("AI 优化生成了系统未支持的动作承诺")
        warnings = result.get("warnings")
        return {
            "candidateContent": candidate,
            "warnings": [str(item) for item in warnings] if isinstance(warnings, list) else [],
        }


def build_prompt_optimizer(
    *,
    base_url: str,
    api_key: str,
    model: str,
    timeout_seconds: float = 30.0,
) -> OpenAICompatiblePromptOptimizer | None:
    if not base_url.strip() or not api_key.strip() or not model.strip():
        return None
    return OpenAICompatiblePromptOptimizer(
        base_url=base_url,
        api_key=api_key,
        model=model,
        timeout_seconds=timeout_seconds,
    )
