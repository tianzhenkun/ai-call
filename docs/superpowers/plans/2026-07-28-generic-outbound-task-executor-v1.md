# 通用外呼任务执行器 V1 实现计划

> **执行要求：** 按 `test-driven-development` 逐项先写失败测试，再完成最小实现；完成前按 `verification-before-completion` 做全量验证。

**目标：** 让已创建的通用外呼任务具备可验证的执行状态流、拨打尝试、通话记录关联和重试能力；本阶段仅接入显式标记的本地模拟拨号器，不连接运营商或真实 SIP。

**成功标准：**

- 立即任务和到期定时任务可从 `SCHEDULED` 原子进入 `RUNNING`。
- 每个对象一次只会被一个执行器认领，并创建一条 attempt 和一条关联通话记录。
- 成功、可重试失败、不可重试失败和重试耗尽均按配置快照正确落库。
- 暂停、继续、停止、取消符合已确认状态机；停止不再认领新对象。
- worker 默认关闭；开启后仅使用 `outbound_mock` 模式，不会真实拨号。
- 单元测试、全量测试和静态检查通过，运行态验证不触碰现有真实号码任务。

**约束：**

- 不使用 OSS，不新增物理外键，不使用 `jsonb`。
- `bigint` 继续按现有接口统一转字符串。
- 不把全量任务或名单载入内存；每轮按任务、对象批次处理。
- 保留现有工作树中的用户改动，只提交本计划直接相关文件。

---

## 任务 1：锁定执行状态和重试字段

**文件：**

- 修改：`app/api/v1/ai_call/outbound/rule_task_model.py`
- 修改：`docs/livekit-ai-outbound/sql/phase-h2-outbound-rule-task-postgres.sql`
- 新增：`docs/livekit-ai-outbound/sql/phase-h3-outbound-task-executor-postgres.sql`
- 测试：`tests/test_ai_call_outbound_task_executor.py`

**步骤：**

1. 写失败测试，证明对象需要持久化 `next_attempt_at`，并且新表建表包含该列。
2. 在对象模型增加可空的 `next_attempt_at`，给任务状态查询和对象重试查询补必要索引。
3. 更新全量建表 SQL，并新增存量 PostgreSQL 的幂等迁移 SQL。
4. 运行目标测试，确认通过。

**可验证结果：** 新建库和存量 PostgreSQL 均可保存下一次重试时间，测试数据库模型一致。

## 任务 2：实现任务和对象的原子认领

**文件：**

- 新增：`app/api/v1/ai_call/outbound/task_executor.py`
- 测试：`tests/test_ai_call_outbound_task_executor.py`

**步骤：**

1. 写失败测试：未到期任务不执行、立即任务和到期任务进入 `RUNNING`、同一对象不能重复认领。
2. 实现 `OutboundTaskExecutor.run_once()`，按小批次查询候选任务，再用带旧状态条件的 `UPDATE` 原子认领。
3. 对 `PENDING` 或已到期 `RETRY_WAIT` 对象做条件更新，原子切换为 `DIALING` 并递增 `attempt_count`。
4. 运行目标测试，确认重复执行不会重复创建拨打。

**可验证结果：** 多次轮询不会重复处理同一对象，未来定时任务保持不变。

## 任务 3：创建 attempt、模拟通话记录并固化结果

**文件：**

- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 测试：`tests/test_ai_call_outbound_task_executor.py`

**步骤：**

1. 写失败测试：每次拨打必须生成唯一 attempt 和 `AiCallRecordModel`，并通过 `call_id` 关联。
2. 定义最小 `OutboundDialer` 协议和 `DialResult`；测试注入顺序拨号器。
3. 在拨号前落 attempt 与 `outbound_mock` 通话记录；拨号完成后统一更新 attempt、record、target。
4. 成功结果将对象置为 `COMPLETED`；失败结果根据任务配置快照决定 `RETRY_WAIT` 或 `FAILED`。
5. 重算任务计数；全部对象终态后将任务置为 `COMPLETED`。

**可验证结果：** 对象、attempt、通话记录、任务计数四者一致，且模拟记录不会伪装成真实 SIP。

## 任务 4：实现暂停、继续、停止和取消

**文件：**

- 修改：`app/api/v1/ai_call/outbound/rule_task_service.py`
- 修改：`tests/test_ai_call_outbound_rule_task.py`
- 测试：`tests/test_ai_call_outbound_task_executor.py`

**步骤：**

1. 先更新测试，覆盖合法动作、非法状态和动作幂等边界。
2. 实现 `RUNNING -> PAUSED`、`PAUSED -> RUNNING`、`RUNNING/PAUSED -> STOPPED`。
3. 停止时将 `PENDING`、`RETRY_WAIT` 对象批量置为 `CANCELLED`；`DIALING` 对象允许当前 attempt 收尾，但不得把任务改回 `COMPLETED`。
4. 保留 `SCHEDULED -> CANCELLED`，并同步刷新任务计数。

**可验证结果：** 暂停后不认领新对象，继续后恢复；停止后无新 attempt，当前拨打可正常收尾。

## 任务 5：接入默认关闭的后台 worker

**文件：**

- 修改：`app/config/setting.py`
- 修改：`app/plugin/init_app.py`
- 修改：`app/api/v1/ai_call/outbound/task_executor.py`
- 测试：`tests/test_ai_call_outbound_task_executor.py`

**步骤：**

1. 写失败测试，证明 worker 可启动、停止并周期调用 `run_once()`。
2. 增加 `AI_CALL_OUTBOUND_EXECUTOR_ENABLED=false` 和有限的轮询/批次配置。
3. 增加 `MockOutboundDialer`，仅返回配置好的模拟结果，不发起网络或 SIP 请求。
4. 在应用生命周期中按开关启动和停止 worker；日志明确标记为模拟执行器。

**可验证结果：** 默认启动不会处理任何任务；测试显式开启时，worker 只产生 `outbound_mock` 数据。

## 任务 6：回归、运行态隔离验证和代码审查

**文件：**

- 可能修改：上述实现文件和测试文件

**步骤：**

1. 运行执行器、规则任务、通话记录筛选相关目标测试。
2. 运行 Ruff 和后端全量测试。
3. 使用独立临时 SQLite 库启动一次 `run_once()`，验证成功和重试路径；不连接 `/tmp/ai_call_ed81_local.db`。
4. 检查现有 19011 运行库中任务、对象、attempt 数量未被改变。
5. 按 `requesting-code-review` 做独立代码审查，修复重要问题后重新验证。

**可验证结果：** 测试和静态检查通过，现有任务没有被执行或误拨，审查无未解决的重要问题。
