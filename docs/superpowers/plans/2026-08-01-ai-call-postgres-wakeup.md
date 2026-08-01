# AI Call PostgreSQL 最小数据库唤醒实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 使用 PostgreSQL `LISTEN/NOTIFY` 唤醒现有 Dispatcher/Runtime 数据库扫描路径，在不改变执行权合同的前提下降低逐条 Web `START_CALL` 的发现延迟，并保留周期扫描完整兜底。

**架构：** 固定 channel `ai_call_runtime_control_wakeup` 只发送空通知；新 START、首次 Owner 分配和 END 终态屏障在各自事务内调用 `pg_notify`。Dispatcher 与 Runtime 各持有一条独立 LISTEN 连接，通知或原扫描周期到期后都只调用原 `run_once()`；所有 Owner、fencing、Command/Effect CAS 保持不变。

**技术栈：** Python 3.12、asyncio、SQLAlchemy AsyncEngine/AsyncSession、asyncpg 0.30、PostgreSQL 16、pytest、ruff。

---

## 文件与职责

- 新增 `app/services/ai_call/runtime_control/postgres_wakeup.py`：固定 channel、事务内 publisher、独立 LISTEN 连接、通知/停止/超时等待和断线重连。
- 修改 `app/services/ai_call/runtime_control/command_repository.py`：新 START 与 END 事实 flush 后发布事务内通知。
- 修改 `app/services/ai_call/runtime_control/owner_repository.py`：首次 Owner/容量分配 flush 后发布事务内通知。
- 修改 `app/services/ai_call/runtime_control/dispatcher_service.py`：可选 listener 驱动原 `run_once()`，周期扫描不变。
- 修改 `app/services/ai_call/runtime_control/runtime_service.py`：可选 listener 驱动原 `run_once()`，Owner/fencing 与 watchdog 不变。
- 修改 `app/services/ai_call/runtime_control/lifecycle.py`：为 Dispatcher 和 Runtime 构造各自独立的 PostgreSQL listener。
- 新增 `tests/test_ai_call_runtime_postgres_wakeup.py`：publisher、listener、通知合并、停止、断线与服务循环单元测试。
- 修改 `tests/test_ai_call_runtime_command_repository.py`：START/END 发布点单元合同。
- 修改 `tests/test_ai_call_runtime_owner_repository.py`：Owner 分配发布点单元合同。
- 修改 `tests/test_ai_call_runtime_lifecycle.py`：listener 注入和生命周期边界。
- 修改 `tests/postgres/test_ai_call_runtime_control_postgres.py`：提交/回滚、伪通知、扫描兜底、双实例和 20 条通知延迟。
- 新增 `docs/superpowers/reports/2026-08-01-ai-call-postgres-wakeup-latency.md`：新旧双基准结果、环境、命令和结论边界。

---

## Task 1：实现无业务语义的 PostgreSQL 唤醒原语

**合同：** `WAKE-02`、`WAKE-04`、`WAKE-05`、`WAKE-08`、`WAKE-09`。

**文件：**

- 新增：`app/services/ai_call/runtime_control/postgres_wakeup.py`
- 新增：`tests/test_ai_call_runtime_postgres_wakeup.py`

- [x] **步骤 1：写 publisher 和 listener 红灯测试**

测试固定接口：

```python
CONTROL_WAKEUP_CHANNEL = "ai_call_runtime_control_wakeup"
await publish_control_wakeup(session)

listener = PostgresWakeupListener(engine)
await listener.start()
woken = await listener.wait(timeout_seconds=1.0, stop_event=stop_event)
await listener.stop()
```

覆盖：固定 channel 与空 payload；非 PostgreSQL bind no-op；callback 不解析 payload；多次 callback 合并；stop 立即结束；timeout 返回 `False`；driver termination 后下一次 wait 重连；日志和公开状态不保存 DSN 或业务标识。

- [x] **步骤 2：运行红灯**

```bash
uv run pytest tests/test_ai_call_runtime_postgres_wakeup.py -q
```

预期：模块与符号不存在，测试收集失败。

- [x] **步骤 3：实现最小原语**

```python
class WakeupListener(Protocol):
    async def start(self) -> bool: ...
    async def wait(
        self,
        *,
        timeout_seconds: float,
        stop_event: asyncio.Event,
    ) -> bool: ...
    async def stop(self) -> None: ...
```

- publisher 只对 PostgreSQL bind 执行 `select pg_notify(:channel, '')`；
- listener 从 `AsyncEngine.connect()` 取得独立连接并使用 asyncpg `add_listener`；
- termination callback 只标记断线和设置 event；
- 重连失败时等待原 scan interval，禁止忙循环；
- `notification_count`、`timeout_count` 只用于可观测性；
- stop 移除 callback、关闭连接并唤醒 wait。

- [x] **步骤 4：运行绿灯并提交**

```bash
uv run pytest tests/test_ai_call_runtime_postgres_wakeup.py -q
uv run ruff check app/services/ai_call/runtime_control/postgres_wakeup.py tests/test_ai_call_runtime_postgres_wakeup.py
git diff --check
git add app/services/ai_call/runtime_control/postgres_wakeup.py tests/test_ai_call_runtime_postgres_wakeup.py
git diff --cached --check
git commit -m "feat(ai-call): 增加 PostgreSQL 数据库唤醒原语"
```

---

## Task 2：在权威事务中发布空唤醒

**合同：** `WAKE-01` 至 `WAKE-04`。

**文件：**

- 修改：`app/services/ai_call/runtime_control/command_repository.py`
- 修改：`app/services/ai_call/runtime_control/owner_repository.py`
- 修改：`tests/test_ai_call_runtime_command_repository.py`
- 修改：`tests/test_ai_call_runtime_owner_repository.py`

- [x] **步骤 1：写 Repository 发布点红灯测试**

覆盖：新 START flush 后发布一次；幂等读取与冲突不发布；END 屏障与命令 flush 后发布；首次 Owner、容量、fencing 和目标命令 flush 后发布；容量不足或竞争失败不发布。publisher 不接收 payload、tenant、call 或 owner 参数。

- [x] **步骤 2：运行红灯**

```bash
uv run pytest tests/test_ai_call_runtime_command_repository.py tests/test_ai_call_runtime_owner_repository.py -k 'wakeup' -q
```

预期：成功写入路径没有调用 publisher。

- [x] **步骤 3：实现事务内发布点**

- `create_start_call` 仅在新 Record/Command flush 成功后发布；
- `request_end` 在终态屏障、Evidence 与 END 命令 flush 后发布；
- `assign_initial_owner` 在 Record、Worker、Command 与可选 Reservation flush 后发布；
- 必须复用当前 session，禁止另开事务或连接外部系统；
- 不修改 `dispatch_*`、`published_at` 或 `stream_message_id`。

- [x] **步骤 4：运行绿灯并提交**

```bash
uv run pytest tests/test_ai_call_runtime_command_repository.py tests/test_ai_call_runtime_owner_repository.py -q
uv run ruff check app/services/ai_call/runtime_control/command_repository.py app/services/ai_call/runtime_control/owner_repository.py tests/test_ai_call_runtime_command_repository.py tests/test_ai_call_runtime_owner_repository.py
git diff --check
git add app/services/ai_call/runtime_control/command_repository.py app/services/ai_call/runtime_control/owner_repository.py tests/test_ai_call_runtime_command_repository.py tests/test_ai_call_runtime_owner_repository.py
git diff --cached --check
git commit -m "feat(ai-call): 在运行时事务发布数据库唤醒"
```

---

## Task 3：让 Dispatcher/Runtime 监听通知并保留扫描兜底

**合同：** `WAKE-03`、`WAKE-05` 至 `WAKE-08`。

**文件：**

- 修改：`app/services/ai_call/runtime_control/dispatcher_service.py`
- 修改：`app/services/ai_call/runtime_control/runtime_service.py`
- 修改：`app/services/ai_call/runtime_control/lifecycle.py`
- 修改：`tests/test_ai_call_runtime_postgres_wakeup.py`
- 修改：`tests/test_ai_call_runtime_lifecycle.py`

- [x] **步骤 1：写服务循环红灯测试**

使用 `FakeWakeupListener` 覆盖：Dispatcher/Runtime 首轮扫描后等待；signal 后不等待长 scan interval 就再次扫描；通知不向 `run_once()` 传任务标识；listener 不可用时周期扫描继续；stop 终止等待并关闭 listener；lifecycle 为两个角色构造独立 listener；Recovery 不注入；无 listener 和手动 `run_once()` 行为不变。

- [x] **步骤 2：运行红灯**

```bash
uv run pytest tests/test_ai_call_runtime_postgres_wakeup.py tests/test_ai_call_runtime_lifecycle.py -k 'wakeup or lifecycle' -q
```

预期：Service 构造器和循环尚不接受 listener。

- [x] **步骤 3：实现最小循环集成**

- Dispatcher/Runtime 构造器增加可选 listener；
- `start()` 启动 listener 后创建原循环，监听失败不阻止服务；
- 每轮仍先执行现有 `run_once()`，再等待通知、stop 或 timeout；
- Runtime 的 Worker 注册、heartbeat、Owner 续租、watchdog 和 fail-closed 顺序不变；
- stop 等待循环退出后关闭 listener；
- lifecycle 从 session factory 的 AsyncEngine 构造独立 listener；不读取 Redis 设置。

- [x] **步骤 4：同步 CodeGraph 并运行绿灯**

```bash
codegraph sync
codegraph callers DispatcherControlService.start
codegraph callers RuntimeControlService.start
codegraph impact DispatcherControlService
codegraph impact RuntimeControlService
uv run pytest tests/test_ai_call_runtime_postgres_wakeup.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_process_roles.py -q
uv run ruff check app/services/ai_call/runtime_control/dispatcher_service.py app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/lifecycle.py tests/test_ai_call_runtime_postgres_wakeup.py tests/test_ai_call_runtime_lifecycle.py
git diff --check
```

- [x] **步骤 5：独立提交**

```bash
git add app/services/ai_call/runtime_control/dispatcher_service.py app/services/ai_call/runtime_control/runtime_service.py app/services/ai_call/runtime_control/lifecycle.py tests/test_ai_call_runtime_postgres_wakeup.py tests/test_ai_call_runtime_lifecycle.py
git diff --cached --check
git commit -m "feat(ai-call): 接入 Dispatcher Runtime 数据库监听"
```

---

## Task 4：用隔离 PostgreSQL 证明通知不是执行权

**合同：** `WAKE-01` 至 `WAKE-10`；原 `CMD-01`、`OWN-02`、`EFF-02` 和终态屏障。

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`

- [x] **步骤 1：写 PostgreSQL 红灯测试**

新增名称包含 `postgres_wakeup` 的测试，覆盖：同事务通知 commit 后才收到、rollback 不收到；无业务事实的空通知不创建记录；listener 不可用时短周期扫描仍完成 START；双 Dispatcher 同时唤醒只有一个 Owner；双 Runtime 同时唤醒只有 Owner 处理；START/END 最终状态和 Stub 次数不变；dispatch/stream 字段为空。

- [x] **步骤 2：运行红灯**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k 'postgres_wakeup' -q
```

预期：事务通知和 listener 驱动合同尚未全部满足。

- [x] **步骤 3：只修复测试揭示的监听清理、重连或循环竞态**

禁止引入新状态、队列、并行执行或 Redis。

- [x] **步骤 4：运行绿灯并提交**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k 'postgres_wakeup or web_db_only_two_dispatchers or web_db_only_idempotency' -q
git diff --check
git add tests/postgres/test_ai_call_runtime_control_postgres.py
git diff --cached --check
git commit -m "test(ai-call): 验证 PostgreSQL 唤醒恢复合同"
```

---

## Task 5：测量 20 条逐提交唤醒延迟并保留批量基准

**合同：** 规格第 7 节；不得通过改写原批量测试制造达标。

**文件：**

- 修改：`tests/postgres/test_ai_call_runtime_control_postgres.py`
- 新增：`docs/superpowers/reports/2026-08-01-ai-call-postgres-wakeup-latency.md`

- [x] **步骤 1：写 20 条通知延迟测试**

新增 `test_postgres_wakeup_latency_measurement`：使用与原测试相同的 20 条 payload；启动真实 Dispatcher/Runtime loop 和两个 listener；scan interval 设为 30 秒；每条独立提交并等其有 `claimed_at` 后再提交下一条；PostgreSQL 计算延迟；硬断言样本完整、P95 `<1000 ms`、backlog 0、Worker 未饱和、dispatch/stream 字段 0；输出 P50/P95/max、通知/超时次数、PostgreSQL 版本。原批量测试不得删除或改成逐条模式。

- [x] **步骤 2：运行新旧双基准**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -k 'postgres_wakeup_latency or web_db_only_latency' -q -s
```

若新逐条 P95 仍 `>=1000 ms`，记录实际数据并停止继续优化；不得引入 Redis 或并行执行。原批量结果始终原样报告。

- [x] **步骤 3：编写现场报告**

报告包含新旧 sample_count/P50/P95/max、notification_count、timeout_count、backlog、Worker 使用量、dispatch/stream 字段、PostgreSQL 版本、隔离级别、复现命令和结论边界。

- [x] **步骤 4：检查并提交**

```bash
uv run ruff check tests/postgres/test_ai_call_runtime_control_postgres.py
git diff --check
git add tests/postgres/test_ai_call_runtime_control_postgres.py
git add -f docs/superpowers/reports/2026-08-01-ai-call-postgres-wakeup-latency.md
git diff --cached --check
git commit -m "test(ai-call): 记录 PostgreSQL 唤醒延迟"
```

---

## Task 6：执行完整安全门禁并交付

**文件：** 不新增生产行为，只验证 Task 1 至 Task 5 的已提交结果。

- [x] **步骤 1：同步 CodeGraph 并核对范围**

```bash
codegraph sync
codegraph status
rg -n 'redis|xadd|xread|xautoclaim|livekit|sip|linphone|egress' app/services/ai_call/runtime_control/postgres_wakeup.py app/services/ai_call/runtime_control/dispatcher_service.py app/services/ai_call/runtime_control/runtime_service.py
```

- [x] **步骤 2：运行完整相关单元测试**

```bash
uv run pytest tests/test_ai_call_process_roles.py tests/test_ai_call_runtime_postgres_wakeup.py tests/test_ai_call_runtime_command_repository.py tests/test_ai_call_runtime_entry_start_service.py tests/test_ai_call_runtime_entry_controller.py tests/test_ai_call_runtime_entry_legacy_guards.py tests/test_ai_call_runtime_bootstrap_service.py tests/test_ai_call_runtime_token_service.py tests/test_ai_call_runtime_start_readiness.py tests/test_ai_call_runtime_stub_handlers.py tests/test_ai_call_runtime_lifecycle.py tests/test_ai_call_runtime_owner_repository.py tests/test_ai_call_web_runtime_entry.py -q
```

- [x] **步骤 3：运行完整隔离 PostgreSQL 16 套件**

```bash
./tools/run_ai_call_runtime_postgres_tests.sh tests/postgres/test_ai_call_runtime_control_postgres.py -q
```

- [x] **步骤 4：运行静态与补丁门禁**

```bash
uv run ruff check .
git diff --check
git diff --cached --check
git status --short --branch
```

- [x] **步骤 5：最终范围核对**

最终汇报列出独立提交、测试数量、新旧延迟、通知/超时次数、backlog、容量、dispatch/stream 字段、lint/diff/CodeGraph、剩余既有脏文件和未连接的外部系统。只宣称通知降低发现等待，不宣称解决批量串行吞吐。

## 完成记录（2026-08-01）

- Task 1 至 Task 6 已按独立提交完成：`70ad36b`、`ca0a71f`、`049a701`、
  `7f05df7`、`59aab3c`。
- 逐条独立提交的 20 条唤醒基准：P50 `173.944 ms`、P95 `347.042 ms`、
  max `381.035 ms`，满足 P95 `< 1000 ms`。
- 原 20 条批量基准保持原语义：P50 `773.204 ms`、P95 `1197.724 ms`、
  max `1246.001 ms`；未用逐条模式替换或改写该基准。
- Dispatcher/Runtime listener 各收到 40 次通知、0 次周期超时；backlog 为 0，
  Worker 使用量 `20/64`，dispatch/stream 字段写入数为 0。
- 最终验证：相关单元测试 `126 passed`、隔离 PostgreSQL 16 全套 `54 passed`、
  `uv run ruff check .`、`git diff --check` 和 CodeGraph 同步全部通过。
- 未连接 Redis、LiveKit、SIP、Egress、Linphone 或真实 Provider，未拨号、未启动业务服务。
