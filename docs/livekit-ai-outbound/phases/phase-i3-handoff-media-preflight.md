# Phase I3 转人工媒体生命周期无拨号 Preflight

## 结论

2026-08-02 的本地只读检查结论为 **RED：自动实现可以继续验证，但当前运行环境不能进入真人拨号验收**。

红项不是代码单测失败，而是现场运行条件尚未就绪：没有本工作树的后端 listener，现有 `ai_voip` PostgreSQL 未安装 I1/I2/I3 控制面表，Runtime 仍使用 Stub 且 owner entry 为空，真实 SIP/Outbound 开关关闭，也没有可核验的浏览器坐席在线状态或线路快照。

本次检查没有创建 Room、Participant、Task、Attempt 或通话记录，没有启动或重启服务，没有拨号。

## 基线

| 项目 | 只读证据 | 结论 |
| --- | --- | --- |
| cwd | `/Users/liuhongli/.codex/worktrees/16d4/ai-call` | 通过，隔离实现工作树 |
| branch | `codex/ai-call-handoff-media-lifecycle` | 通过 |
| preflight 前 HEAD | `2901c6268f48b410967e8f0f4822170949e4f476` | 已记录 |
| dirty state | 只有本切片 PostgreSQL 测试改动 | 可解释，提交前需归零 |
| 后端 listener | 未发现 `uvicorn`、FastAPI 或 AI Call Python listener | **红项**；不能把其他工作树运行态当成本分支证据 |
| 前端 listener | `node` PID 31460，cwd 为 `/Users/liuhongli/Desktop/lingchen/recov-ai-web-react` | 范围外运行态，不属于本工作树 |

## PostgreSQL

本地容器 `ai-voip-local-postgres` 的只读查询结果：

```text
database=ai_voip
schema=public
transaction_isolation=read committed
server_version_num=160014
```

PostgreSQL 版本和隔离级别满足控制面合同；但该数据库中未找到以下 I1/I3 表：

```text
ai_call_runtime_command
ai_call_runtime_effect
ai_call_webhook_inbox
ai_call_handoff_media_evidence
ai_call_sip_line
ai_call_handoff_agent
```

因此它不是可用于本次 16.5 真人验收的已迁移隔离数据库。不得在该库上直接创建测试任务。

## Provider 与媒体基础设施

| 组件 | 只读证据 | 结论 |
| --- | --- | --- |
| LiveKit | `ai-call-19011-livekit-server` running；本地 HTTP 返回 200 | 进程可达；容器未配置 Docker healthcheck |
| LiveKit SIP | `ai-call-19011-livekit-sip` running | 只确认进程；未创建 SIP Participant |
| LiveKit Egress | `ai-call-19011-livekit-egress` running | 只确认进程；未启动 Egress |
| FreeSWITCH | `sip_realtime_freeswitch` running/healthy | 容器健康 |
| Linphone | macOS Linphone 进程存在；FreeSWITCH 只读注册表有 1 个 contact | 注册存在；未通过呼叫或 OPTIONS 主动探测 Reachable |

容器存活和注册存在不等于真实媒体闭环通过，不能升级为 SIP、RTP、ASR、LLM、TTS 或转人工验收证据。

## 当前安全开关

从本工作树 `Settings` 只读加载到的非敏感配置：

```text
environment=dev
database_type=postgres
process_roles=api,legacy_runtime,outbound,jobs
owner_entries=<empty>
provider_mode=stub
real_provider_allowed=false
linphone_test_enabled=false
single_callee=199****1001
sip_outbound_enabled=false
outbound_executor_enabled=false
outbound_dialer_mode=mock
```

这些值能够防止误拨，但不满足真实 Provider 门禁。正式环境实际配置没有在本机运行态中出现，故只能确认代码会拒绝正式环境 `livekit` mode 或非空 owner entries，不能宣称正式部署配置已经核验。

## 坐席、线路与权限

- 没有本工作树后端 listener，无法只读核验浏览器坐席登录、`available` 心跳、授权场景或 SSE 状态。
- 当前 PostgreSQL 没有 `ai_call_handoff_agent`，无法查询新鲜 `available` Presence。
- 当前 PostgreSQL 没有 `ai_call_sip_line`，无法核验启用线路、健康快照和 `max_concurrency`。
- 单号码配置只以脱敏形式记录；真实模式的 Provider Adapter 还会在调用 SIP 前执行精确号码相等校验。

## 进入一次真人验收前必须补齐

1. 在独立 PostgreSQL 16 数据库安装 I1、I2、I3 migration，并再次核对 schema 与 `READ COMMITTED`。
2. 从本提交后的工作树分别启动所需 API、Runtime、Dispatcher、Jobs、Outbound 角色，并逐 PID 核对 cwd；不得复用其他 worktree listener 作为证据。
3. 隔离环境 owner entries 只包含本次测试入口，Runtime 使用 `livekit` mode，显式 real-provider opt-in；正式环境保持 Stub 和空 entries。
4. 只读核验一条启用且 `READY` 的 SIP Line、`max_concurrency=1` 或明确的单槽隔离，以及唯一脱敏白名单号码。
5. 浏览器坐席登录后出现新鲜 `available` Presence，场景权限与本次任务一致。
6. 再核验 LiveKit/SIP/Egress/FreeSWITCH、Linphone registration 和 Reachable；仍不得创建业务对象。
7. 自动测试、PostgreSQL 故障矩阵、lint、CodeGraph 和 diff 全绿后，单独请求用户确认一次真实拨号。

## 真人验收检查点

用户确认后只允许创建一个白名单号码任务，按顺序留证：

```text
task -> attempt -> record -> SIP ringing/answered -> RTP/track
-> AI ASR/LLM/TTS -> handoff requested/claimed
-> 15 秒内双向媒体 -> customer/agent hangup
-> Effect cleanup -> recording terminal -> Provider 无残留
```

任何 preflight 红项都必须先关闭；健康检查、Mock、注册状态、页面观察或 `SCHEDULED` 均不能替代真实电话证据。
