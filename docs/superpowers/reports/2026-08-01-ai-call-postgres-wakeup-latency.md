# AI Call PostgreSQL 最小唤醒延迟报告

日期：2026-08-01

## 1. 结论

逐条独立提交的 PostgreSQL `LISTEN/NOTIFY` 唤醒基准满足本切片门槛：

- 样本数：20；
- `created_at -> claimed_at` P95：`347.042 ms`；
- 最大值：`381.035 ms`；
- 验收门槛：P95 `< 1000 ms`；
- 扫描 backlog：0；
- `dispatch_token`、`dispatch_expires_at`、`published_at`、`stream_message_id` 写入数：0。

通知只缩短 Dispatcher/Runtime 等待下一轮扫描的时间。Owner、fencing、Command/Effect CAS 和执行权仍由 PostgreSQL 业务表决定；本结果不代表批量串行吞吐已经消除。

## 2. 环境与命令

- PostgreSQL：16.14，`server_version_num=160014`；
- 事务隔离级别：`READ COMMITTED`；
- 数据库：测试脚本启动的一次性隔离 PostgreSQL 容器；
- Provider：`DeterministicWebProviderStub`；
- 未连接 Redis、LiveKit、SIP、Linphone 或真实 Provider；
- Dispatcher/Runtime 周期扫描间隔：30 秒，用于证明本组延迟来自通知唤醒而非短周期轮询。

复现命令：

```bash
./tools/run_ai_call_runtime_postgres_tests.sh \
  tests/postgres/test_ai_call_runtime_control_postgres.py \
  -k 'postgres_wakeup_latency or web_db_only_latency' -q -s
```

## 3. 新旧双基准

| 指标 | 旧批量基准 | PostgreSQL 唤醒基准 |
| --- | ---: | ---: |
| 样本数 | 20 | 20 |
| 提交方式 | 同一事务创建 20 条后统一处理 | 每条独立提交，观察到 `claimed_at` 后提交下一条 |
| P50 | 773.204 ms | 173.944 ms |
| P95 | 1197.724 ms | 347.042 ms |
| 最大值 | 1246.001 ms | 381.035 ms |
| backlog | 0 | 0 |
| Worker 使用量 | 20 / 64 | 20 / 64 |
| Provider Stub 调用数 | 40 | 40 |
| legacy dispatch/stream 字段写入数 | 0 | 0 |

旧批量基准保持原测试语义：先在一个事务内创建 20 条命令，再由单次 Dispatcher 和 Runtime 顺序处理。它同时包含批量顺序分配与 Runtime 串行处理等待，因此不用于判断数据库唤醒是否达标。

新基准复用相同形状的 20 条 Web `START_CALL` payload。每条命令独立提交，Dispatcher 和 Runtime 均运行真实后台 loop，并各自持有独立 LISTEN 连接。

## 4. 唤醒与一致性证据

- Dispatcher listener：40 次通知，0 次周期超时；
- Runtime listener：40 次通知，0 次周期超时；
- 40 次广播由 20 次 START 事实提交和 20 次首次 Owner 分配提交组成；
- 固定 channel 只发送空 payload；通知不包含 tenant、call、owner 或 fencing；
- 每次唤醒后仍调用原数据库 `run_once()` 扫描；
- 双 Dispatcher/Runtime PostgreSQL 测试证明最终只有一个 Owner/Command 赢家；
- 事务测试证明 commit 后才投递，rollback 不投递；
- 伪 payload 不创建 Record 或 Command；
- 无 listener 时，原周期扫描仍能完成 Owner 分配。

## 5. 结论边界

本切片只解决 DB-only Dispatcher/Runtime 的等待唤醒延迟，不引入队列执行权，也不改变业务吞吐模型。以下不属于本次结论：

- Redis Streams、Consumer Group 或 Pending 恢复；
- 真实 LiveKit、SIP、Egress、Linphone 或 Provider 延迟；
- 批量命令的并行执行或 Provider 并发；
- 16.2C 前端、SSE、浏览器和真实电话验收。
