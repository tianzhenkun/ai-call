# AI Call Qwen3 离线转写实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 新通话默认使用 Qwen3 文件转写处理客户与人工坐席分轨，同时保留 Paraformer 配置回退能力。

**架构：** 在现有 DashScope Paraformer provider 上抽出可覆盖的提交载荷方法，新增 Qwen3 provider 复用轮询、结果下载和句子解析。通过一个纯工厂函数按配置创建单一 provider，启动 worker 不承担协议分支；现有 ASR job 和对话分段数据流保持不变。

**技术栈：** Python 3.13、FastAPI、httpx、Pydantic Settings、pytest

---

## 文件结构

- 创建 `tests/test_ai_call_offline_asr_provider.py`：隔离验证 DashScope provider 请求、响应解析和工厂选择。
- 修改 `app/services/ai_call/offline_asr_service.py`：新增 Qwen3 provider、嵌套结果地址解析和 provider 工厂。
- 修改 `app/config/setting.py`：将 Qwen3 provider/model 设置为默认值，并保留 Paraformer 字面量。
- 修改 `app/plugin/init_app.py`：启动时通过工厂创建配置指定的单一 provider。
- 修改 `tests/test_ai_call_phase_b1_records.py`：锁定默认 provider/model 配置。

### 任务 1：用测试定义 Qwen3 provider 契约

**文件：**
- 创建：`tests/test_ai_call_offline_asr_provider.py`
- 测试：`tests/test_ai_call_offline_asr_provider.py`

- [ ] **步骤 1：编写失败的 Qwen3 请求载荷测试**

```python
from app.services.ai_call.offline_asr_service import (
    DashScopeQwenFileTranscriptionAsrProvider,
)


def test_qwen_filetrans_builds_single_file_request() -> None:
    provider = DashScopeQwenFileTranscriptionAsrProvider(
        api_key="test-key",
        model="qwen3-asr-flash-filetrans",
        language_hints=["zh"],
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
```

- [ ] **步骤 2：编写失败的 Qwen3 结果解析测试**

```python
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
```

- [ ] **步骤 3：运行测试验证红灯**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_offline_asr_provider.py -q
```

预期：测试收集阶段因 `DashScopeQwenFileTranscriptionAsrProvider` 尚不存在而失败。

### 任务 2：实现 Qwen3 provider 和可回退工厂

**文件：**
- 修改：`app/services/ai_call/offline_asr_service.py`
- 修改：`tests/test_ai_call_offline_asr_provider.py`

- [ ] **步骤 1：为现有 Paraformer provider 抽出请求载荷方法**

```python
def _submit_payload(self, audio_url: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": self.model_name,
        "input": {"file_urls": [audio_url]},
    }
    if self.language_hints:
        payload["parameters"] = {"language_hints": self.language_hints}
    return payload
```

`_submit()` 只负责 HTTP 请求，并调用 `self._submit_payload(audio_url)`。

- [ ] **步骤 2：新增 Qwen3 provider**

```python
class DashScopeQwenFileTranscriptionAsrProvider(DashScopeParaformerAsrProvider):
    provider_name = "dashscope_qwen_filetrans"

    def __init__(self, *, api_key: str, model: str, language_hints=None, **kwargs) -> None:
        super().__init__(
            api_key=api_key,
            model=model.strip() or "qwen3-asr-flash-filetrans",
            language_hints=language_hints,
            **kwargs,
        )

    def _submit_payload(self, audio_url: str) -> dict[str, Any]:
        parameters: dict[str, Any] = {
            "channel_id": [0],
            "enable_itn": True,
            "enable_words": True,
        }
        if self.language_hints:
            parameters["language"] = self.language_hints[0]
        return {
            "model": self.model_name,
            "input": {"file_url": audio_url},
            "parameters": parameters,
        }
```

- [ ] **步骤 3：兼容 Qwen3 嵌套结果地址**

在 `_transcription_url()` 中先检查：

```python
result = output.get("result")
if isinstance(result, dict):
    value = result.get("transcription_url")
    if value:
        return str(value)
```

- [ ] **步骤 4：新增 provider 工厂及测试**

```python
def build_dashscope_offline_asr_provider(*, provider_name: str, **kwargs):
    provider_types = {
        "dashscope_paraformer": DashScopeParaformerAsrProvider,
        "dashscope_qwen_filetrans": DashScopeQwenFileTranscriptionAsrProvider,
    }
    try:
        provider_type = provider_types[provider_name]
    except KeyError as exc:
        raise ValueError(f"不支持的离线 ASR provider: {provider_name}") from exc
    return provider_type(**kwargs)
```

测试分别断言两个合法 provider 的实例类型，并断言未知 provider 抛出 `ValueError`。

- [ ] **步骤 5：运行定向测试验证绿灯**

运行：

```bash
.venv/bin/pytest tests/test_ai_call_offline_asr_provider.py -q
```

预期：全部通过。

### 任务 3：切换默认配置和启动选择

**文件：**
- 修改：`app/config/setting.py`
- 修改：`app/plugin/init_app.py`
- 修改：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：先修改默认配置测试并验证红灯**

```python
def test_offline_asr_defaults_to_qwen_filetrans_with_chinese_language() -> None:
    settings = Settings(_env_file=None)

    assert settings.AI_CALL_OFFLINE_ASR_PROVIDER == "dashscope_qwen_filetrans"
    assert settings.AI_CALL_OFFLINE_ASR_MODEL == "qwen3-asr-flash-filetrans"
    assert settings.AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS == "zh"
```

运行：

```bash
.venv/bin/pytest tests/test_ai_call_phase_b1_records.py::test_offline_asr_defaults_to_qwen_filetrans_with_chinese_language -q
```

预期：provider/model 断言失败，显示当前仍为 Paraformer。

- [ ] **步骤 2：修改默认配置**

```python
AI_CALL_OFFLINE_ASR_PROVIDER: Literal[
    "dashscope_paraformer",
    "dashscope_qwen_filetrans",
] = "dashscope_qwen_filetrans"
AI_CALL_OFFLINE_ASR_MODEL: str = "qwen3-asr-flash-filetrans"
```

- [ ] **步骤 3：启动 worker 改用 provider 工厂**

`_start_ai_call_offline_asr_worker()` 调用：

```python
provider = build_dashscope_offline_asr_provider(
    provider_name=settings.AI_CALL_OFFLINE_ASR_PROVIDER,
    api_key=settings.EFFECTIVE_ASR_API_KEY,
    model=settings.AI_CALL_OFFLINE_ASR_MODEL or settings.ASR_MODEL,
    language_hints=parse_language_hints(settings.AI_CALL_OFFLINE_ASR_LANGUAGE_HINTS),
    timeout_seconds=settings.AI_CALL_OFFLINE_ASR_TIMEOUT_SECONDS,
    poll_interval_seconds=settings.AI_CALL_OFFLINE_ASR_POLL_INTERVAL_SECONDS,
)
```

- [ ] **步骤 4：运行配置测试和 provider 测试**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_offline_asr_provider.py \
  tests/test_ai_call_phase_b1_records.py::test_offline_asr_defaults_to_qwen_filetrans_with_chinese_language \
  -q
```

预期：全部通过。

### 任务 4：回归验证和运行态验收

**文件：**
- 验证：`app/services/ai_call/offline_asr_service.py`
- 验证：`app/plugin/init_app.py`
- 验证：`app/config/setting.py`
- 验证：`tests/test_ai_call_offline_asr_provider.py`
- 验证：`tests/test_ai_call_phase_b1_records.py`

- [ ] **步骤 1：运行离线 ASR 及通话记录回归**

运行：

```bash
.venv/bin/pytest \
  tests/test_ai_call_offline_asr_provider.py \
  tests/test_ai_call_phase_b1_records.py \
  -q
```

预期：全部通过。

- [ ] **步骤 2：运行静态检查**

运行：

```bash
.venv/bin/ruff check \
  app/services/ai_call/offline_asr_service.py \
  app/plugin/init_app.py \
  app/config/setting.py \
  tests/test_ai_call_offline_asr_provider.py \
  tests/test_ai_call_phase_b1_records.py
```

预期：退出码为 0。

- [ ] **步骤 3：检查改动边界**

运行：

```bash
git diff --check
git diff -- \
  app/services/ai_call/offline_asr_service.py \
  app/plugin/init_app.py \
  app/config/setting.py \
  tests/test_ai_call_offline_asr_provider.py \
  tests/test_ai_call_phase_b1_records.py
```

预期：无空白错误；diff 只包含设计范围内的增量，不覆盖已有预签名 URL 修改。

- [ ] **步骤 4：运行态验收**

仅在不会中断用户当前通话、且确认 listener cwd 为本工作树时重启 `19011`。新通话结束后查询：

```text
GET /ai-call/records/{callId}/recording
```

验收条件：

- 新 ASR job 的 `provider` 为 `dashscope_qwen_filetrans`；
- `model` 为 `qwen3-asr-flash-filetrans`；
- customer 和 human_agent job 均为 `completed`；
- 对话记录能正确保留“转人工、下面条、喝水、挂了吧”等实际口语。

如果当前不适合重启或没有新通话，明确记录“代码回归已完成、真实新通话验收待执行”，不得把手工提交旧录音等同于运行态闭环。
