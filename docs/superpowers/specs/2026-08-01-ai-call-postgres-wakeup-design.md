# AI Call PostgreSQL 最小数据库唤醒设计

## 1. 状态与范围

本文定义 Web DB-only 控制面的下一独立实施切片：在不改变 PostgreSQL 单一事实源、Owner/fencing、Command CAS 和 Effect CAS 的前提下，使用 PostgreSQL `LISTEN/NOTIFY` 降低 Dispatcher 与 Runtime 的任务发现等待，并保留现有周期扫描作为完整恢复兜底。

本切片只覆盖：

- Web `START_CALL` 提交后的 Dispatcher 唤醒；
- 首次 Owner 分配后的 Runtime 唤醒；
- `END_CALL` 提交后的 Runtime 唤醒；
- 监听连接断开、通知丢失、通知合并和事务回滚时的扫描兜底；
- 20 条逐条提交样本的 `created_at -> claimed_at` 发现延迟测量；
- 原 20 条同时提交的批量一致性与吞吐基准保持不变。

本切片明确不包含：

- Redis Streams、Redis Pub/Sub、SSE 或任何新的消息中间件；
- `DISPATCHING`、`PUBLISHED`、`dispatch_token` 或 `stream_message_id` 状态流转；
- Runtime 并行处理、Dispatcher 批量事务合并或吞吐优化；
- LiveKit、SIP、Egress、Linphone、真实 Provider 或真实电话；
- Preview、Outbound、Handoff、Presence 和前端改造。

## 2. 现有延迟的证据解释

现有 20 条批量测试先在一个事务中依次创建全部命令，再同步调用一次 Dispatcher，最后同步调用一次 Runtime。Dispatcher 对候选 Record 逐条开启事务分配 Owner；Runtime 对所属 Record 逐条续租、认领命令并完成两个 Stub Effect。`claimed_at` 直到 Runtime 对命令执行数据库 CAS 时才写入。

因此原报告中的 P50 `1600.125 ms`、P95 `2495.995 ms` 和 max `2636.141 ms` 同时包含：

1. 20 条入口记录的串行创建时间；
2. Dispatcher 对 20 条 Record 的串行 Owner 分配时间；
3. Runtime 在当前命令前处理其他 Record 的排队时间。

该测试没有等待 1 秒 Dispatcher 扫描或 0.5 秒 Runtime 扫描，因此不能用它单独证明 `LISTEN/NOTIFY` 是否降低发现等待。通知优化不能被描述成批量吞吐优化，也不能通过删除原测试或放宽原口径制造达标结果。

## 3. 设计合同

| ID | 合同 |
| --- | --- |
| `WAKE-01` | PostgreSQL 中的 Record、Command、Owner、fencing、Effect 和处理租约仍是唯一执行权来源。 |
| `WAKE-02` | 通知只表示“数据库可能有新工作”；通知 payload 为空，不包含租户、call、command、Owner 或业务数据。 |
| `WAKE-03` | 收到通知后只能调用现有数据库扫描与 CAS 路径，不能从通知直接构造执行任务。 |
| `WAKE-04` | 通知必须在产生对应数据库事实的同一事务内调用 `pg_notify`；事务提交后才可见，回滚不产生通知。 |
| `WAKE-05` | 通知允许重复、合并、乱序或丢失；这些情况只能增加扫描次数或发现延迟，不能改变最终持久状态。 |
| `WAKE-06` | Dispatcher 保留 1 秒周期扫描，Runtime 保留 0.5 秒周期扫描；监听不可用时服务继续运行扫描路径。 |
| `WAKE-07` | 多 Dispatcher/Runtime 会同时收到广播，但仍由原有行锁、容量判断、Owner/fencing 和 CAS 决定唯一赢家。 |
| `WAKE-08` | 监听连接使用独立 PostgreSQL 连接；它不持有业务事务、行锁或连接内未提交状态。 |
| `WAKE-09` | 固定使用单一 channel `ai_call_runtime_control_wakeup`，避免按租户、Worker 或 call 动态拼接 channel。 |
| `WAKE-10` | 本切片不得写入休眠的 Redis 路由字段，也不得引入 Redis 或真实 Provider 依赖。 |

## 4. 架构与数据流

### 4.1 单通道数据库门铃

所有相关进程监听固定 channel：

```text
ai_call_runtime_control_wakeup
```

通知不携带业务 payload。Dispatcher 和 Runtime 都可以被同一通知唤醒；不相关的进程执行一次只读候选扫描后返回 0。少量无效扫描换取更少的路由逻辑，并避免通知内容逐渐演变成第二事实源。

### 4.2 START_CALL

```text
API transaction
  -> insert Record + START_CALL
  -> pg_notify(fixed_channel, '')
  -> COMMIT

Dispatcher listener wakes
  -> existing candidate query
  -> existing Owner/capacity/fencing transaction
  -> pg_notify(fixed_channel, '')
  -> COMMIT

Runtime listener wakes
  -> existing owner query
  -> renew exact lease
  -> existing command CAS to PROCESSING
  -> existing Stub Effect path
```

第一条通知可能同时唤醒 Runtime，但 Owner 尚未分配时 Runtime 只能返回 0。Owner 分配事务提交后的第二条通知负责再次唤醒 Runtime。两个通知如果被 PostgreSQL 或进程内事件合并，Runtime 扫描时只要 Owner 已提交仍可正常领取；若扫描早于 Owner 提交，周期扫描最终兜底。

### 4.3 END_CALL

`RuntimeCommandRepository.request_end` 在建立终态屏障和 END 命令的同一事务内发送空通知。当前 Owner 的 Runtime 收到通知后，仍通过 `claim_pending_end` 的 Owner、fencing、租约和处理 token CAS 领取。重复 END 证据允许重复通知，但数据库仍只保留一条 END 命令。

### 4.4 周期扫描兜底

Dispatcher 和 Runtime 的循环变为：

```text
run_once()
  -> 等待以下任一事件：
       a. PostgreSQL 通知
       b. stop_event
       c. 原 scan interval 到期
  -> 再次 run_once()
```

通知路径和超时路径调用完全相同的 `run_once()`，不创建第二套状态机。

## 5. 组件边界

### 5.1 `postgres_wakeup.py`

新增专用模块，职责限定为：

- 定义固定 channel 常量；
- 在现有业务事务中执行空 payload `pg_notify`；
- 使用 SQLAlchemy `AsyncEngine` 持有一条独立 asyncpg LISTEN 连接；
- 将 asyncpg callback 转换为可合并的 `asyncio.Event`；
- 提供带 timeout 和 stop event 的等待；
- 监听连接终止后唤醒等待者，下一轮尝试重连；重连失败时等待原扫描周期，避免忙循环。

该模块不查询业务表、不修改 Command/Owner/Effect，也不解释通知 payload。

### 5.2 Repository 发布点

只在实际形成以下事实时发布：

- 新 `START_CALL` 与 Record 已 flush；
- Dispatcher 首次 Owner 分配与容量计数已 flush；
- END 终态屏障和命令已 flush。

幂等重放可以不发布；即使发布重复通知也不得影响正确性。到期 `RETRY_WAIT`、Effect reconcile 和 Recovery 仍由周期扫描推进，避免本切片扩张为通用调度总线。

### 5.3 Dispatcher/Runtime

两个服务增加可选的 `WakeupListener` 依赖：

- 未注入时保持当前纯周期扫描行为，现有单元测试和手动 `run_once()` 不变；
- lifecycle 在 PostgreSQL DB-only 角色启动时注入各自独立的 listener；
- listener 连接失败只记录可观测错误并回退周期扫描，不能阻止控制面启动；
- stop 时先设置 `stop_event`，等待循环退出，再移除 listener 并归还连接。

Recovery 本切片不接 listener，继续使用既有 0.5 秒扫描。

## 6. 故障与竞争矩阵

| 场景 | 预期 |
| --- | --- |
| 事务执行 `pg_notify` 后回滚 | Listener 收不到通知，数据库也没有对应事实。 |
| 通知重复或多条合并 | 最多多执行一次扫描；同一 Command/Owner/Effect 仍只有一个 CAS 赢家。 |
| 伪造或无业务事实的通知 | Dispatcher/Runtime 扫描返回 0，不创建任何记录或 Effect。 |
| Listener 在通知前断开 | 周期扫描在原间隔内发现工作；重连后恢复低延迟唤醒。 |
| Listener 在通知后、扫描前断开 | 已置位的进程内事件或周期扫描推进工作。 |
| 两个 Dispatcher 同时被唤醒 | 共享 PostgreSQL 锁与容量合同决定唯一 Owner；分配成功总数为 1。 |
| 两个 Runtime 同时被唤醒 | 只有 Record 当前 Owner 能续租和领取；非 Owner 处理数为 0。 |
| START 通知早于 Owner 分配 | Runtime 首次扫描返回 0；Owner 提交通知或周期扫描再次唤醒。 |
| END 与普通处理竞争 | 原终态屏障和 `claim_pending_end` 优先级不变，通知不绕过状态机。 |

## 7. 延迟与测试口径

### 7.1 原批量基准保留

现有 `test_web_db_only_latency_measurement` 保持 20 条同时积压、手动 `run_once()` 的流程与字段。它继续衡量当前串行 Dispatcher/Runtime 的批量排队结果，不设置 `<1000 ms` 的硬断言，也不因增加通知而重写历史报告。

### 7.2 新通知发现基准

新增 20 条相同 payload 结构的逐条提交测试：

1. 启动一个真实 Dispatcher loop 和一个真实 Runtime loop；
2. 两者扫描间隔设置为 30 秒，确保 `<1000 ms` 结果不能来自周期扫描；
3. 每次在独立事务提交一条 Web `START_CALL`；
4. 等待该命令进入 `PROCESSING` 或 `SUCCEEDED` 后再提交下一条；
5. 使用 PostgreSQL `created_at` 与首次 `claimed_at` 计算 20 个样本；
6. 硬断言样本完整、P95 `<1000 ms`、backlog 为 0、无池饱和、无 dispatch/stream 字段写入；
7. 记录 P50/P95/max、通知接收次数、周期超时次数和 PostgreSQL 版本。

逐条基准只证明空闲或低积压时的通知发现延迟。若原批量基准仍超过 1 秒，必须继续报告为批量串行吞吐限制，不能描述成 LISTEN/NOTIFY 失败或整体延迟已解决。

### 7.3 正确性回归

必须重跑：

- 同事务提交可通知、回滚不通知；
- 断开 listener 后周期扫描仍完成 START；
- 无事实的通知不能产生 Owner、Command 或 Effect；
- 双 Dispatcher、双 Runtime 的 START/END 完整闭环；
- 幂等、Owner/fencing、终态屏障、Effect 恢复和 cleanup；
- 完整隔离 PostgreSQL 16 套件、相关单元测试、`ruff check .` 和 `git diff --check`。

## 8. 可观测性与安全

Listener 至少记录连接成功、连接丢失、重连失败和恢复成功；日志不得包含业务 payload、租户、call 或敏感字段。测试统计只记录通知次数和超时次数。

固定 channel 和空 payload 不是授权边界。数据库用户能够发送伪通知也只能触发扫描；没有合法 Record、Owner、fencing 和 Command CAS 时不能产生副作用。

## 9. 验收结论边界

通过本切片后只允许宣称：

> PostgreSQL LISTEN/NOTIFY 已作为 DB-only 控制面的非权威唤醒优化；通知丢失时周期扫描仍可恢复，逐条 Web START_CALL 的数据库发现延迟已按 20 条样本测量。

不得宣称：

- 20 条同时积压的批量吞吐已低于 1 秒；
- Redis Streams 或 16.2C 已实现；
- 浏览器实时语音、LiveKit、SIP 或真实 Provider 已接入；
- 已完成正式环境灰度或真实电话验收。
