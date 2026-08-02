# Phase I3 转人工媒体生命周期无拨号 Preflight

## 结论

2026-08-02 已完成一次隔离 PostgreSQL 和本分支五角色空载启动预检：**数据库 migration、DB-only Runtime/Dispatcher/Recovery、Jobs webhook 和 API listener 均通过；真人拨号门禁仍为 RED。**

剩余红项不是本次代码单测失败，而是尚未装配真人验收资源：隔离库没有 SIP Line 和坐席 Presence，Runtime 继续使用 Stub，真实 Provider、SIP Outbound、Outbound Executor 和 Linphone 测试开关均保持关闭。因此本次证据只能证明控制面能够在 PostgreSQL 16 上空载启动和安全退出，不能升级为真实 SIP、媒体或转人工闭环通过。

本次没有创建 Room、Participant、Task、Attempt、Record、Command、Effect 或 Handoff，没有拨号，没有调用真实 LiveKit、SIP、Egress、ASR、LLM、TTS 或 Provider。临时服务和一次性数据库已在检查后删除。

## 基线

| 项目 | 证据 | 结论 |
| --- | --- | --- |
| cwd | `/Users/liuhongli/.codex/worktrees/16d4/ai-call` | 通过，隔离实现工作树 |
| branch | `codex/ai-call-handoff-media-lifecycle` | 通过 |
| HEAD | `d296bc92edd88e305c97b713292b7f14671e5655` | 通过，本切片已提交基线 |
| dirty state | 启动前为 clean | 通过，未修改既有脏文件 |
| listener | PID `75372`，`127.0.0.1:19015` | 通过，进程 cwd 与目标工作树一致 |
| API health | `GET /ai-call/health` 返回 HTTP 200、`{"status":"ok"}` | 通过 |

FastAPI 的 `ROOT_PATH=/ai-call-api/v1` 是反向代理挂载信息，不是本次直连 Uvicorn 的路由前缀；因此本地 listener 的正确健康地址是 `/ai-call/health`。

## 隔离 PostgreSQL 与 migration

使用 compose project `ai-call-16-5-preflight-d296bc9` 创建一次性 `postgres:16`，随机映射到 `127.0.0.1:55142`。先从当前模型创建 39 张表，再以 `psql -v ON_ERROR_STOP=1` 依次执行：

1. `phase-i1-owner-command-db-control-plane.sql`
2. `phase-i2-direct-sip-db-only-plaintext.sql`
3. `phase-i3-handoff-media-lifecycle.sql`

迁移全部提交成功，重复列、表和索引只产生预期的 `already exists, skipping` notice。数据库现场快照：

```text
server_version=16.14 (Debian 16.14-1.pgdg13+1)
schema=public
transaction_isolation=read committed
table_count=39
```

已确认存在控制面和 I3 关键对象：

```text
ai_call_runtime_worker
ai_call_runtime_command
ai_call_runtime_effect
ai_call_record
ai_call_handoff
ai_call_handoff_media_evidence
ai_call_webhook_inbox
ai_call_webhook_quarantine
ai_call_outbound_task
ai_call_outbound_attempt
```

一次性 PostgreSQL 容器、network 和 volume 已在验证后通过明确 compose project 名删除；未触碰原有 `ai-voip-local-postgres`。

## 五角色空载启动

使用单一 standalone 进程装配以下角色，不装配 `legacy_runtime`：

```text
AI_CALL_PROCESS_ROLES=api,runtime,dispatcher,jobs,outbound
AI_CALL_OWNER_COMMAND_V1_ENTRIES=outbound
AI_CALL_RUNTIME_INSTANCE_ID=preflight-d296bc9
```

启动日志确认：

```text
AI Call DB-only Runtime 已启动
AI Call DB-only Dispatcher 已启动
AI Call DB-only Recovery 已启动
AI Call 通用外呼名单校验恢复扫描完成
AI Call Runtime webhook worker 已启动
AI Call standalone 模式启动
```

Runtime worker 运行态：

```text
status=READY
capacity=20
active_call_count=0
cleanup_capacity=4
active_cleanup_count=0
lease_live=true
```

空库不变量：

```text
record=0
command=0
effect=0
task=0
attempt=0
handoff=0
webhook_inbox=0
quarantine=0
```

收到本地 `Ctrl-C` 后，Recovery、Dispatcher、Runtime 和 webhook worker 均输出关闭完成，listener 消失，worker 写入 `DRAINING`，两个占用计数仍为 0。随后才删除隔离数据库。

## 安全开关

从实际 PID 环境中只提取非敏感配置，结果如下：

```text
environment=dev
database_type=postgres
redis_enable=false
standalone=true
process_roles=api,runtime,dispatcher,jobs,outbound
owner_entries=outbound
provider_mode=stub
real_provider_allowed=false
sip_outbound_enabled=false
outbound_executor_enabled=false
outbound_dialer_mode=mock
linphone_test_enabled=false
recording_enabled=false
recording_reconcile_enabled=false
offline_asr_enabled=false
semantic_analysis_enabled=false
handoff_auto_trigger_enabled=false
voice_worker_enabled=false
```

这些设置确保本次只运行数据库控制面和空队列 worker，不会拨号或调用外部模型、媒体和 Provider。

## Provider 与媒体基础设施

以下只读检查未通过业务 API 创建或变更任何资源：

| 组件 | 只读证据 | 结论 |
| --- | --- | --- |
| LiveKit | `ai-call-19011-livekit-server` running；本地 HTTP 返回 200 | 进程可达；未创建 Room 或 Participant |
| LiveKit SIP | `ai-call-19011-livekit-sip` running | 只确认进程；未发 SIP 请求 |
| LiveKit Egress | `ai-call-19011-livekit-egress` running | 只确认进程；未启动 Egress |
| FreeSWITCH | `sip_realtime_freeswitch` running/healthy | 容器健康 |
| Linphone | macOS Linphone 进程存在；FreeSWITCH 注册表有 1 个 contact，`Ping-Status=Reachable` | 注册和 OPTIONS 可达；未呼叫 |

容器存活、HTTP 200、注册和 Reachable 不等于 RTP、ASR、LLM、TTS 或转人工验收通过。

## 尚未关闭的真人拨号门禁

隔离库的只读查询结果：

```text
sip_line=0
handoff_agent=0
handoff=0
```

因此仍需在一次真人验收前完成：

1. 在新的可保留隔离库中装配一条启用且 `READY` 的 SIP Line，并把 `max_concurrency` 限制为 1 或提供等价单槽隔离证据。
2. 浏览器坐席登录后产生新鲜 `available` Presence，并核对其租户、场景权限与本次任务一致。
3. 仅在真人验收窗口临时把隔离 Runtime 切到 `livekit`、显式开启 real-provider opt-in、SIP Outbound 和 Outbound Executor；正式环境继续保持 Stub 和空 owner entries。
4. 只允许一个明确确认的白名单号码，并在创建任何 Task/Attempt 前再次向用户请求真实拨号确认。

## 真人验收检查点

用户再次明确确认后，只允许创建一个白名单号码任务，按顺序留证：

```text
task -> attempt -> record -> SIP ringing/answered -> RTP/track
-> AI ASR/LLM/TTS -> handoff requested/claimed
-> 15 秒内双向媒体 -> customer/agent hangup
-> Effect cleanup -> recording terminal -> Provider 无残留
```

在上述三类现场门禁全部关闭前，不进入真实拨号；健康检查、Stub、注册状态、页面观察或 `SCHEDULED` 都不能替代真实电话证据。
