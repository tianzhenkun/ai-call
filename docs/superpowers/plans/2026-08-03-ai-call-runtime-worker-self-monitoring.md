# AI Call Runtime Worker 自监控实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** Runtime 主循环意外退出时立即 fail-closed 本地媒体并把进程健康状态变为 503，避免“HTTP 进程存活但 Runtime Worker 已死”。

**架构：** 新增进程内 `RuntimeWorkerHealth`，由 Runtime lifecycle 注入 `RuntimeControlService`。Runtime task 的 done callback 只做失败定级和启动有界 fail-closed 清理；不在同一进程自动创建第二个 Runtime。`/ai-call/health` 读取该状态，交给外部进程监督器执行重启。

**技术栈：** Python 3.11、asyncio、FastAPI、pytest/anyio、SQLAlchemy async

---

## 文件结构

- 创建：`app/services/ai_call/runtime_control/health.py`——保存进程内 Runtime task 健康快照，不保存异常原文。
- 修改：`app/services/ai_call/runtime_control/runtime_service.py`——监控主任务意外退出并 fail-closed 本地 handle。
- 修改：`app/services/ai_call/runtime_control/lifecycle.py`——向 Runtime service 注入本进程默认健康对象。
- 修改：`app/api/v1/ai_call/controller.py`——健康端点在 Runtime 失败时返回 503。
- 创建：`tests/test_ai_call_runtime_health.py`——覆盖健康响应、异常退出、正常停止和 lifecycle 注入。

### 任务 1：健康状态与 HTTP 暴露

**文件：**
- 创建：`app/services/ai_call/runtime_control/health.py`
- 修改：`app/api/v1/ai_call/controller.py:121-123`
- 测试：`tests/test_ai_call_runtime_health.py`

- [ ] **步骤 1：编写失败的健康端点测试**

```python
import json
from importlib import import_module
from inspect import signature

import pytest

from app.api.v1.ai_call.controller import ai_call_health


def test_runtime_worker_health_records_sanitized_failure() -> None:
    module = import_module("app.services.ai_call.runtime_control.health")
    health_type = getattr(module, "RuntimeWorkerHealth", None)
    assert health_type is not None, "RuntimeWorkerHealth is not implemented"
    health = health_type()
    health.mark_failed("runtime-a:uuid", "runtime_task_exited")

    snapshot = health.snapshot()

    assert snapshot.worker_id == "runtime-a:uuid"
    assert snapshot.state.value == "failed"
    assert snapshot.error_code == "runtime_task_exited"


@pytest.mark.anyio
async def test_ai_call_health_returns_503_when_runtime_task_failed() -> None:
    assert "runtime_health" in signature(ai_call_health).parameters
    module = import_module("app.services.ai_call.runtime_control.health")
    health_type = getattr(module, "RuntimeWorkerHealth", None)
    assert health_type is not None, "RuntimeWorkerHealth is not implemented"
    health = health_type()
    health.mark_failed("runtime-a:uuid", "runtime_task_exited")

    response = await ai_call_health(health)

    assert response.status_code == 503
    assert json.loads(response.body) == {
        "status": "error",
        "runtime": "failed",
        "errorCode": "runtime_task_exited",
    }
```

- [ ] **步骤 2：运行测试验证因功能缺失而失败**

运行：

```bash
uv run pytest \
  tests/test_ai_call_runtime_health.py::test_runtime_worker_health_records_sanitized_failure \
  tests/test_ai_call_runtime_health.py::test_ai_call_health_returns_503_when_runtime_task_failed -q
```

预期：两个测试均 FAIL：一个明确报告 health 模块不存在，另一个明确报告健康端点尚未
接收 `runtime_health`；失败原因都是目标功能缺失，而不是测试夹具错误。

- [ ] **步骤 3：实现最小健康对象和端点**

```python
class RuntimeTaskState(StrEnum):
    NOT_CONFIGURED = "not_configured"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class RuntimeHealthSnapshot:
    state: RuntimeTaskState
    worker_id: str | None
    error_code: str | None


class RuntimeWorkerHealth:
    def __init__(self) -> None:
        self._snapshot = RuntimeHealthSnapshot(
            state=RuntimeTaskState.NOT_CONFIGURED,
            worker_id=None,
            error_code=None,
        )

    def snapshot(self) -> RuntimeHealthSnapshot:
        return self._snapshot

    def mark_starting(self, worker_id: str) -> None:
        self._set(RuntimeTaskState.STARTING, worker_id)

    def mark_running(self, worker_id: str) -> None:
        self._set(RuntimeTaskState.RUNNING, worker_id)

    def mark_failed(self, worker_id: str, error_code: str) -> None:
        self._set(RuntimeTaskState.FAILED, worker_id, error_code)

    def mark_stopped(self, worker_id: str) -> None:
        self._set(RuntimeTaskState.STOPPED, worker_id)

    def _set(
        self,
        state: RuntimeTaskState,
        worker_id: str,
        error_code: str | None = None,
    ) -> None:
        self._snapshot = RuntimeHealthSnapshot(state, worker_id, error_code)
```

健康端点只返回稳定 `errorCode`，不返回异常消息、堆栈、号码或 Provider 数据。`failed`
返回 503；其他状态保持原有 200 响应。

```python
def get_runtime_worker_health() -> RuntimeWorkerHealth:
    return default_runtime_worker_health


@AiCallRouter.get("/health", summary="智能外呼模块健康检查")
async def ai_call_health(
    runtime_health: Annotated[
        RuntimeWorkerHealth,
        Depends(get_runtime_worker_health),
    ],
) -> dict[str, str] | JSONResponse:
    snapshot = runtime_health.snapshot()
    if snapshot.state == RuntimeTaskState.FAILED:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "error",
                "runtime": "failed",
                "errorCode": snapshot.error_code or "runtime_task_failed",
            },
        )
    return {"status": "ok"}
```

非失败响应必须保持原有 `{"status": "ok"}`，避免破坏现有健康探针合同。

- [ ] **步骤 4：运行健康端点测试验证通过**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_health.py::test_ai_call_health_returns_503_when_runtime_task_failed -q
```

预期：健康对象和端点两个测试均通过。

### 任务 2：Runtime task 异常退出 fail-closed

**文件：**
- 修改：`app/services/ai_call/runtime_control/runtime_service.py:131-212`
- 测试：`tests/test_ai_call_runtime_health.py`

- [ ] **步骤 1：编写失败的异常退出测试**

```python
import asyncio

from app.services.ai_call.runtime_control.health import (
    RuntimeTaskState,
    RuntimeWorkerHealth,
)
from app.services.ai_call.runtime_control.runtime_service import (
    RuntimeControlService,
    RuntimeRegistry,
)


WORKER_ID = "runtime-test:12345678-1234-5678-1234-567812345678"


class FakeLocalHandle:
    def __init__(self) -> None:
        self.fail_closed_count = 0

    async def fail_closed(self) -> None:
        self.fail_closed_count += 1


async def _return_unexpectedly() -> None:
    return None


@pytest.mark.anyio
async def test_runtime_task_exit_marks_failed_and_fail_closes_handles() -> None:
    health = RuntimeWorkerHealth()
    handle = FakeLocalHandle()
    registry = RuntimeRegistry(local_handles={"call-1": handle})
    service = RuntimeControlService(
        worker_id=WORKER_ID,
        registry=registry,
        session_factory=None,
        provider=None,
        health=health,
    )
    health.mark_running(WORKER_ID)

    task = asyncio.create_task(_return_unexpectedly())
    service._task = task
    service._monitor_runtime_task(task)
    await task
    await asyncio.sleep(0)
    assert service._supervision_task is not None
    await service._supervision_task

    assert health.snapshot().state == RuntimeTaskState.FAILED
    assert health.snapshot().error_code == "runtime_task_exited"
    assert handle.fail_closed_count == 1
    assert registry.local_handles == {}


@pytest.mark.anyio
async def test_expected_runtime_task_exit_is_not_marked_failed() -> None:
    health = RuntimeWorkerHealth()
    service = RuntimeControlService(
        worker_id=WORKER_ID,
        registry=RuntimeRegistry(),
        session_factory=None,
        provider=None,
        health=health,
    )
    health.mark_running(WORKER_ID)
    service._stopping = True
    task = asyncio.create_task(_return_unexpectedly())
    service._task = task
    service._monitor_runtime_task(task)

    await task
    await asyncio.sleep(0)

    assert health.snapshot().state == RuntimeTaskState.RUNNING
    assert service._supervision_task is None
```

- [ ] **步骤 2：运行测试验证正确失败**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_health.py -q
```

预期：异常退出测试 FAIL，原因是 `health` 注入和 `_monitor_runtime_task` 尚未实现。

- [ ] **步骤 3：实现最少 task 监控**

`RuntimeControlService.start()` 创建 `_run_loop` task 后注册 done callback 并标记
`running`。done callback 在 `_stopping` 为 false 时：

1. 消费 task 异常，映射为 `runtime_task_failed / runtime_task_cancelled / runtime_task_exited`；
2. 同步把健康状态写为 `failed`；
3. 创建并保存一个 supervision task，遍历当前 Owner/local handle 调用既有
   `_clear_owner_tracking()`；
4. 尝试把当前 worker 行标记为 `DRAINING`，数据库失败只记录安全日志；
5. 不自动创建新 Runtime task。

`stop()` 必须先写 `_stopping = True`，等待 Runtime task 和 supervision task，再标记
`stopped`；调用者取消 `stop()` 时仍传播 `CancelledError`。

```python
def _monitor_runtime_task(self, task: asyncio.Task[None]) -> None:
    task.add_done_callback(self._on_runtime_task_done)


def _on_runtime_task_done(self, task: asyncio.Task[None]) -> None:
    exception = None if task.cancelled() else task.exception()
    if self._stopping:
        return
    if task.cancelled():
        error_code = "runtime_task_cancelled"
    elif exception is None:
        error_code = "runtime_task_exited"
    else:
        error_code = "runtime_task_failed"
    self._health.mark_failed(self.worker_id, error_code)
    log.error(
        "AI Call Runtime 主任务意外退出: worker_id={}, error_code={}, error_type={}",
        self.worker_id,
        error_code,
        type(exception).__name__ if exception is not None else "none",
    )
    self._supervision_task = asyncio.create_task(
        self._fail_closed_after_runtime_exit(),
        name=f"ai-call-runtime-supervision:{self.worker_id}",
    )


async def _fail_closed_after_runtime_exit(self) -> None:
    call_ids = sorted(
        set(self.registry.owner_fencing_tokens)
        | set(self.registry.owner_watchdogs)
        | set(self.registry.local_handles)
    )
    for call_id in call_ids:
        await self._clear_owner_tracking(call_id)
    await self._mark_worker_draining_safely()
```

`start()` 在注册 Worker 前写 `starting`，创建 task 并挂监控后写 `running`；启动异常写
`runtime_start_failed`。`stop()` 使用 `asyncio.gather(task, return_exceptions=True)` 等待
已经失败的 task，避免关闭阶段再次抛出同一异常。

- [ ] **步骤 4：运行 Runtime 健康测试验证通过**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_health.py -q
```

预期：全部通过且无未消费 task 异常告警。

### 任务 3：Lifecycle 注入与回归验证

**文件：**
- 修改：`app/services/ai_call/runtime_control/lifecycle.py:51-85`
- 测试：`tests/test_ai_call_runtime_health.py`

- [ ] **步骤 1：编写失败的 lifecycle 注入测试**

```python
@pytest.mark.anyio
async def test_runtime_lifecycle_injects_process_health(monkeypatch) -> None:
    from app.services.ai_call.runtime_control import lifecycle

    captured: dict[str, object] = {}

    async def valid_database(_session_factory) -> tuple[str, str]:
        return "runtime-test", "public"

    class FakeService:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def start(self) -> None:
            return None

    monkeypatch.setattr(lifecycle, "RuntimeControlService", FakeService)
    monkeypatch.setattr(lifecycle, "validate_db_only_runtime_database", valid_database)
    settings = SimpleNamespace(
        AI_CALL_RUNTIME_INSTANCE_ID="runtime-test",
        AI_CALL_RUNTIME_CAPACITY=2,
        AI_CALL_RUNTIME_CLEANUP_CAPACITY=1,
        AI_CALL_RUNTIME_WORKER_LEASE_SECONDS=15,
        AI_CALL_RUNTIME_OWNER_LEASE_SECONDS=15,
        AI_CALL_RUNTIME_FAIL_CLOSED_MARGIN_SECONDS=3,
        AI_CALL_RUNTIME_END_SCAN_INTERVAL_SECONDS=0.5,
    )
    session_factory = SimpleNamespace(kw={"bind": object()})

    await lifecycle.start_runtime_control_lifecycle(settings, session_factory)

    assert captured["health"] is lifecycle.default_runtime_worker_health
```

- [ ] **步骤 2：运行测试验证正确失败**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_health.py::test_runtime_lifecycle_injects_process_health -q
```

预期：FAIL，缺少 `health` 构造参数。

- [ ] **步骤 3：注入默认健康对象并运行聚焦回归**

运行：

```bash
uv run pytest tests/test_ai_call_runtime_health.py tests/test_ai_call_runtime_lifecycle.py -q
```

预期：全部通过。

- [ ] **步骤 4：运行 Runtime/Owner 安全回归和静态检查**

运行：

```bash
uv run pytest \
  tests/test_ai_call_runtime_owner_repository.py \
  tests/test_ai_call_runtime_lifecycle.py \
  tests/test_ai_call_runtime_health.py \
  tests/test_main.py -q
uv run ruff check \
  app/services/ai_call/runtime_control/health.py \
  app/services/ai_call/runtime_control/runtime_service.py \
  app/services/ai_call/runtime_control/lifecycle.py \
  app/api/v1/ai_call/controller.py \
  tests/test_ai_call_runtime_health.py
git diff --check
```

预期：pytest、Ruff 和 `git diff --check` 全部通过；不启动业务服务，不连接 LiveKit、SIP、Redis 或 Provider。

- [ ] **步骤 5：仅提交本切片文件**

```bash
git add \
  app/services/ai_call/runtime_control/health.py \
  app/services/ai_call/runtime_control/runtime_service.py \
  app/services/ai_call/runtime_control/lifecycle.py \
  app/api/v1/ai_call/controller.py \
  tests/test_ai_call_runtime_health.py
git commit -m "fix(ai-call): monitor runtime worker health"
```

提交前必须确认 cached diff 只包含上述五个文件；不得暂存或改写工作树中的既有脏文件。
