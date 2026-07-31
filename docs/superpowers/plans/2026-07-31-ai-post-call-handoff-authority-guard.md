# AI 话后跟进 handoff 权威保护实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 正式 SIP 外呼只在全程 AI 且未发起过 handoff 时允许创建 `ai_post_call`，避免与 `handoff_unanswered` 或坐席 `after_call_work` 重复建单。

**架构：** 保留现有语义分析和严格自动跟进门槛，在 `AiCallPostCallFollowUpService.apply()` 真正创建任务前查询该通话的 handoff。只要存在任意 handoff 记录就停止 AI 自动建单；未接回访和人工话后跟进继续由现有服务负责，不新增服务、数据表或场景规则。

**技术栈：** Python 3.12、FastAPI、SQLAlchemy AsyncIO、pytest

---

## 文件结构

- 修改：`app/services/ai_call/post_call_follow_up_service.py`
  - 在现有 AI 自动跟进创建入口增加 handoff 权威保护。
- 修改：`tests/test_ai_call_post_call_follow_up.py`
  - 构造不同状态的 handoff，验证同一通话不再创建 `ai_post_call`。

### 任务 1：锁定 handoff 权威并防止 AI 重复建单

**文件：**
- 修改：`app/services/ai_call/post_call_follow_up_service.py:62-80`
- 测试：`tests/test_ai_call_post_call_follow_up.py:14-21`
- 测试：`tests/test_ai_call_post_call_follow_up.py:476-510`

- [x] **步骤 1：编写失败的测试**

在模型导入中加入 `AiCallHandoffModel`，并添加参数化测试：

```python
@pytest.mark.anyio
@pytest.mark.parametrize("handoff_status", ["requested", "expired", "completed"])
async def test_post_call_follow_up_skips_calls_with_any_handoff(
    session_factory,
    handoff_status: str,
) -> None:
    call_id = f"call-with-handoff-{handoff_status}"
    await _seed_post_call_analysis(
        session_factory,
        call_id=call_id,
        entry_type="sip_outbound",
        with_formal_attempt=True,
    )
    now = datetime.now(timezone.utc)
    async with session_factory() as db, db.begin():
        db.add(
            AiCallHandoffModel(
                id=500,
                tenant_id="tenant-a",
                handoff_id=f"handoff-{handoff_status}",
                call_id=call_id,
                room_name=f"room-{call_id}",
                scene_code="intro_geo",
                status=handoff_status,
                request_source="ai",
                requested_at=now,
            )
        )

    async with session_factory() as db, db.begin():
        repository = AiCallRecordRepository(db)
        analysis = await repository.get_semantic_analysis(call_id=call_id)
        assert analysis is not None

        created = await post_call_follow_up_service.AiCallPostCallFollowUpService(
            repository
        ).apply(analysis)

        assert created is None
        count = await db.scalar(
            select(func.count())
            .select_from(AiCallFollowUpTaskModel)
            .where(AiCallFollowUpTaskModel.source_call_id == call_id)
        )
        assert count == 0
```

- [x] **步骤 2：运行测试并确认正确失败**

运行：

```bash
uv run pytest tests/test_ai_call_post_call_follow_up.py::test_post_call_follow_up_skips_calls_with_any_handoff -q
```

预期：3 个参数用例均在 `assert created is None` 失败，证明当前代码仍会为存在 handoff 的正式 SIP 通话创建 `ai_post_call`。

- [x] **步骤 3：编写最少实现**

在正式 SIP 记录和 attempt 校验通过后、创建跟进任务前增加：

```python
handoffs = await self.repository.list_handoffs(analysis.call_id)
if handoffs:
    return None
```

不修改语义分析结果，不改变 `handoff_unanswered` 和 `after_call_work` 的现有创建逻辑。

- [x] **步骤 4：运行新增测试并确认通过**

运行：

```bash
uv run pytest tests/test_ai_call_post_call_follow_up.py::test_post_call_follow_up_skips_calls_with_any_handoff -q
```

预期：`3 passed`。

- [x] **步骤 5：运行相关回归测试**

运行：

```bash
uv run pytest tests/test_ai_call_post_call_follow_up.py tests/test_ai_call_follow_up.py -q
```

预期：全部通过，现有 AI-only 自动跟进、人工话后跟进和幂等行为不回归。

- [x] **步骤 6：运行静态检查**

运行：

```bash
uv run ruff check app/services/ai_call/post_call_follow_up_service.py tests/test_ai_call_post_call_follow_up.py
```

预期：退出码为 0，无 lint 错误。

- [x] **步骤 7：检查并提交本次独立变更**

```bash
git diff --check
git diff -- app/services/ai_call/post_call_follow_up_service.py tests/test_ai_call_post_call_follow_up.py docs/superpowers/plans/2026-07-31-ai-post-call-handoff-authority-guard.md
git add app/services/ai_call/post_call_follow_up_service.py tests/test_ai_call_post_call_follow_up.py docs/superpowers/plans/2026-07-31-ai-post-call-handoff-authority-guard.md
git commit -m "fix(ai-call): 避免转人工通话重复创建AI跟进"
```
