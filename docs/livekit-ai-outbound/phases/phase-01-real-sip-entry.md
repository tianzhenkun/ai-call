# Phase 01：真实 SIP 入口接入

最后更新：2026-06-09

## 1. 阶段定位

本阶段目标是验证真实电话用户能作为 SIP Participant 接入 Phase 00 已跑通的 Web 版商业闭环。

当前已经验证了 SIP trunk 到 LiveKit Room 的基础链路，但尚未验证真实电话入口能否复用 Agent、状态、事件、消息、录音、语义分析和轻量转人工能力。因此本阶段是商业级生产路线中的关键事实门禁。

真实 SIP 拨测的操作步骤、FreeSWITCH 停止/恢复和抓包要求见：[../ops/real-sip-line-runbook.md](../ops/real-sip-line-runbook.md)。

本阶段涉及的数据表设计见：[../04-data-model.md](../04-data-model.md)。在 Phase 00 已有表基础上，本阶段建议增加或启用：

1. `ai_call_participant` 中的 `sip_user`
2. `ai_call_event` 中的 SIP 拨号、振铃、接听、挂机和失败事件

本阶段不要求建设 SIP 线路配置表。当前只有一条正式线路时，SIP proxy、主叫号、公网 IP、端口、鉴权、RTP 范围等线路参数先放在程序配置或环境变量中。具体拨测时使用的 LiveKit outbound trunk、SIP Call-ID、服务商返回 ID 等运行证据，分别记录到通话字段、事件和测试报告中。

如果 `ai_agent_config`、`ai_model_config`、`ai_script_config` 尚未建设，本阶段仍可通过接口参数或程序默认值传入配置，并在 `ai_call_session` 中保存本次执行快照。

## 2. 第一性原理

本阶段的本质不是重新实现一套电话 Agent，而是验证真实 SIP 入口能接入同一套 Room 内业务闭环：

```text
电话用户能听到 Agent
Agent 能听到电话用户
双方能在同一个 LiveKit Room 中完成交互
Phase 00 的消息、事件、录音、分析和轻量转人工能力能继续复用
SIP 状态和失败码能被正确记录
```

如果这个入口没有验证，Web 版商业闭环不能直接推导为真实电话可用。

## 3. 前置条件

进入本阶段前建议满足：

1. Phase 00 Web 版商业闭环已跑通。
2. LiveKit Server 可用。
3. LiveKit SIP 可用。
4. Outbound trunk 可创建或已存在。
5. 真实线路服务商白名单可用。
6. 被叫测试手机号已确认并脱敏记录。
7. Agent Worker 可启动。
8. 有基本抓包和日志能力。

如果 Phase 00 未完成，也可以单独做本阶段，但问题定位和排查效率会明显下降。

### 3.1 必须部署或复用的服务

Phase 01 不是只启动一个拨号脚本。真实 SIP 验证至少要让电话入口、LiveKit Room、Agent、状态落库和排障工具同时可用。

| 服务 | 是否必须 | 作用 | 验证重点 |
|---|---:|---|---|
| `LingChenAiCallBase` 后端服务 | 必须 | 创建真实外呼会话、生成 `call_id`、写状态、事件、消息、录音和分析记录 | 健康检查通过，能连接 PostgreSQL / Redis / LiveKit |
| PostgreSQL | 必须 | 保存 `ai_call_session`、`ai_call_participant`、`ai_call_event`、`ai_call_message`、`ai_call_recording`、`ai_call_analysis` | Phase 00 表已存在；本阶段不新增 SIP 配置表 |
| Redis | 必须 | LiveKit / LiveKit SIP / 后端状态协调和缓存 | LiveKit Server、SIP Server、后端使用同一可达 Redis 配置 |
| LiveKit Server | 必须 | 创建 Room，承载 SIP Participant、Agent Participant 和媒体轨道 | API / WebSocket 可达，API key / secret 与后端一致 |
| LiveKit SIP Server | 必须 | 将真实 SIP 通话接入 LiveKit Room，负责 SIP signaling 和 RTP 转 WebRTC | SIP signaling 端口和 RTP 范围已监听且公网可达 |
| Agent Worker | 必须 | 加入 Room，播放固定话术，接收电话侧音频，复用 ASR / LLM / TTS 链路 | 能被 explicit dispatch；日志能定位 `agent_job_id` |
| ASR / LLM / TTS 配置 | 必须 | 证明真实电话入口可以复用 Phase 00 的语音 Agent 链路 | 至少完成固定话术播放和电话侧音频识别或 VAD |
| 抓包和日志工具 | 必须 | 排查 SIP 失败、RTP 单通、codec 协商和挂机原因 | `tcpdump`、`sngrep`、`tshark` 至少具备一种可用链路 |
| LiveKit Egress 或等价录音服务 | 完整验收必须 | 复用 Phase 00 混音录音能力 | 完整 Phase 01 验收要求录音可追踪；只做最小媒体拨测时可暂缓 |
| OSS / `sys_oss` | 完整验收必须 | 保存真实 SIP 通话录音文件索引 | 完整 Phase 01 验收要求录音上传并可播放；只做最小媒体拨测时可暂缓 |
| Web 调试页面或查询工具 | 建议 | 查看 call、participant、event、message、recording、analysis | 可以先用接口和 SQL 替代页面，但阶段输出必须可复盘 |

最小媒体拨测只要求证明“手机接通、SIP Participant 入 Room、Agent 能播能收”。完整 Phase 01 验收还必须证明录音、消息、事件和分析链路能够复用 Phase 00。

### 3.2 不需要部署的内容

本阶段不需要部署：

1. 完整运营后台、登录体系、菜单权限和租户切换。
2. `ai_sip_trunk`、`ai_agent_config`、`ai_model_config`、`ai_script_config` 配置表。
3. 批量外呼调度器、CPS 控制器和压测系统。
4. 完整坐席台、技能组、排队、抢单。
5. FreeSWITCH 新链路；只有端口冲突时才按 runbook 临时停止既有 FreeSWITCH。

### 3.3 服务器和网络准备

拨测前必须完成：

1. 明确测试服务器公网 IP，并确认该公网 IP 已在线路服务商白名单中。
2. 明确 LiveKit SIP signaling 端口，当前测试口径为 `5080`。
3. 明确 LiveKit SIP RTP 范围，当前测试口径为 `16384-16484`。
4. 安全组、防火墙和宿主机端口已放行 SIP signaling 端口和 RTP 范围。
5. LiveKit Server HTTP / API 端口、RTC TCP fallback 端口和 ICE UDP 范围按 runbook 放行。
6. 如果服务商白名单绑定公网 IP + 源端口，必须确认本次 LiveKit SIP 使用的源端口与白名单一致。
7. 如果更换公网 IP、SIP signaling 端口、主叫号或部署多节点，必须先让服务商重新开放白名单和线路配置。
8. 如果 `5080` 被 FreeSWITCH 占用，且本次必须使用 `5080`，必须准备停止和恢复 FreeSWITCH 的维护窗口。

### 3.4 线路和号码准备

拨测前必须确认：

1. SIP proxy、传输协议、主叫号码、号码格式已经明确。
2. 当前线路是 IP + 端口白名单放行，还是还需要账号鉴权。
3. 如果需要账号鉴权，必须在受控环境变量中配置 auth username / password，不能写入仓库文档。
4. 被叫测试手机号已确认，且只在受控测试记录中保存脱敏值。
5. 服务商是否限制同一主叫号并发外呼；Phase 01 只做单路拨测，不做并发结论。
6. 服务商失败码、风控规则、CPS 和并发限制只记录事实，不作为本阶段压测目标。

### 3.5 配置准备

后端和服务部署前至少准备以下配置。密钥只写入 `.env`、服务器环境变量或密钥管理系统，不写入文档正文。

| 配置 | 用途 |
|---|---|
| `LIVEKIT_URL` | 后端、Agent、SIP Server 连接 LiveKit |
| `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` | 后端签发 token、调用 Room / SIP / Egress API |
| `DATABASE_*` | 后端连接 PostgreSQL |
| `REDIS_*` | 后端、LiveKit、LiveKit SIP 使用 Redis |
| `SIP_PROXY` | 服务商 SIP proxy，例如 `47.94.86.132:5089` |
| `SIP_CALLER_NUMBER` | 主叫号码 |
| `SIP_SIGNALING_PORT` | LiveKit SIP signaling 监听端口 |
| `SIP_RTP_RANGE` | LiveKit SIP RTP 端口范围 |
| `DASHSCOPE_API_KEY` 或等价模型密钥 | ASR / LLM / TTS / 通话后分析 |
| `TTS_VOICE` | 后端固定默认音色，不在页面选择 |

### 3.6 工具和证据准备

拨测前至少准备：

1. `tcpdump`：保存完整 SIP / RTP pcap。
2. `sngrep`：实时查看 INVITE、180、200 OK、ACK、BYE 和失败码。
3. `tshark`：从 pcap 中提取 SIP 信令和 RTP 流摘要。
4. `jq`：格式化 LiveKit API 返回的 trunk、participant、room 和事件结果。
5. 日志目录：保存后端、LiveKit Server、LiveKit SIP、Agent Worker、Egress 日志。
6. 证据目录：保存 pcap、接口响应、participant JSON、SQL 查询结果和测试结论。

## 4. 本阶段目标

通过本阶段后，应证明：

1. 可以从业务侧或测试脚本创建真实 SIP 外呼。
2. 手机可以震铃并接听。
3. SIP Participant 加入指定 LiveKit Room。
4. Agent Participant 加入同一个 Room。
5. Agent 可以复用 Phase 00 的对话链路。
6. 电话侧用户可以听到 Agent。
7. 电话侧说话后，Agent 可以收到音频、VAD 或 ASR 结果。
8. 用户挂机后，SIP call、Room、Agent job 和业务状态可以正常结束。
9. 真实 SIP 通话的消息、事件、混音录音和语义分析链路可复用。

## 5. 本阶段不做

本阶段明确不做：

1. 不做复杂话术。
2. 不做完整业务接口。
3. 不做批量外呼。
4. 不做并发压测。
5. 不做分轨录音和复杂质检。
6. 不做完整坐席台、技能组、排队、抢单。
7. 不做多模型供应商切换。
8. 不替换现有生产 FreeSWITCH 链路。

如果本阶段必须使用现有线路白名单对应的 `5080` 端口，且该端口被 FreeSWITCH 占用，则拨测前需要按 runbook 停止 FreeSWITCH，并在测试结束后恢复。

## 6. 推荐最小链路

```text
测试触发
  -> 创建 LiveKit Room
  -> explicit dispatch Agent
  -> 创建 SIP Participant
  -> 手机震铃
  -> 手机接听
  -> Agent 等待 SIP Participant active
  -> Agent 复用 Phase 00 对话链路
  -> 电话侧用户说一句话
  -> Agent 记录音频 / VAD / ASR
  -> 复用消息 / 事件 / 混音录音 / 语义分析
  -> 用户挂机
  -> 状态闭环
```

关键点：

1. 外呼场景不要在用户接听前播放完整开场白。
2. Agent 应等待 SIP Participant 加入并可用后再开始会话。
3. 本阶段可以使用固定话术和最小 ASR，不追求业务智能。

## 7. 建议任务拆分

| 序号 | 任务 | 说明 | 完成标准 |
|---:|---|---|---|
| 1 | 服务部署和自检 | 启动或确认后端、PostgreSQL、Redis、LiveKit Server、LiveKit SIP、Agent Worker | 所有必须服务健康检查通过 |
| 2 | 固化测试配置 | 明确 trunk、主叫号、目标端口、RTP 范围 | 配置不含密钥和完整被叫号 |
| 3 | 端口和白名单检查 | 检查公网 IP、SIP signaling、RTP、安全组、防火墙、FreeSWITCH 端口占用 | 服务商白名单和服务器端口一致 |
| 4 | 创建测试 Room | 每次测试生成唯一 Room | Room 可追踪到 `call_id` |
| 5 | explicit dispatch Agent | Agent 带 metadata 加入 Room | Agent job 可定位 |
| 6 | 创建 SIP Participant | 使用 outbound trunk 发起外呼 | 手机震铃 |
| 7 | 等待接听 | 等 SIP Participant active 后继续 | 避免开场白丢失 |
| 8 | Agent 固定话术 | Agent 播放一句固定语音 | 电话侧听到完整话术 |
| 9 | 电话侧说话 | 测试人说固定句子 | Agent 日志有音频/VAD/ASR 证据 |
| 10 | 用户挂机 | 测试人挂断电话 | LiveKit SIP 收到 BYE 或等价结束事件 |
| 11 | 状态落库 | 保存通话状态和关键 ID | 可查询完整链路 |
| 12 | 保存证据 | 保存日志、抓包、participant 信息 | 可复盘 |

## 8. 最小测试话术

建议 Agent 固定话术：

```text
您好，这是一通 LiveKit 智能外呼测试。听到后请说“我听到了”。
```

测试人回复：

```text
我听到了。
```

验收时只判断媒体和事件，不判断业务语义。

## 9. 验收标准

必须通过：

1. 手机震铃。
2. 手机接听成功。
3. SIP response 出现 200 OK 或 LiveKit 等价接通日志。
4. LiveKit Room 中有 SIP Participant。
5. LiveKit Room 中有 Agent Participant。
6. 电话侧能听到 Agent 固定话术。
7. Agent 能收到电话侧音频、VAD 或 ASR 结果。
8. 用户挂机后状态正常结束。
9. 保存 SIP Call-ID、LiveKit SIP call ID、Room name、participant ID、call_id。
10. 日志中不出现完整被叫手机号、密钥或模型 API key。

## 10. 失败场景验收

至少验证：

1. 被叫未接。
2. 被叫拒接。
3. 用户接听后立即挂机。
4. Agent 未启动。
5. Agent 加入 Room 超时。
6. LiveKit SIP 创建 participant 失败。
7. 电话侧听不到 Agent。
8. Agent 收不到电话侧音频。

每个失败场景必须输出可定位的错误原因。

## 11. 必须保存的证据

每次真实电话测试至少保存：

```text
测试时间
测试人
call_id
room_name
outbound_trunk_id
livekit_sip_call_id
sip_call_id
sip_participant_id
agent_participant_id
协商 codec
接通耗时
结束原因
LiveKit SIP 日志位置
Agent 日志位置
必要时的 pcap 路径
```

被叫手机号只保存脱敏值。

## 12. 关键日志字段

建议日志字段：

```text
call_id
request_id
business_type
business_id
room_name
sip_call_id
livekit_sip_call_id
sip_participant_identity
agent_participant_identity
agent_job_id
codec
event
latency_ms
error_code
hangup_reason
```

## 13. 本阶段风险

| 风险 | 影响 | 应对 |
|---|---|---|
| Agent 开场白在接听前播放 | 用户听不到完整话术 | 等 SIP Participant active 后再播放 |
| 电话侧听不到 Agent | 下行媒体未闭环 | 检查 Agent publish、Room subscription、codec、RTP |
| Agent 收不到电话侧音频 | 上行媒体未闭环 | 检查 SIP RTP、Room track、Agent subscription |
| 线路服务商选择非预期 codec | 媒体异常 | 以 SDP answer 为准，不写死 PCMA |
| 真实拨测影响现有 FreeSWITCH | 线上风险 | 使用固化 runbook 和回滚步骤 |
| 日志泄露手机号或密钥 | 合规风险 | 脱敏和密钥检查作为验收项 |

## 14. 阶段完成输出

阶段完成后应产出：

1. 一次成功的真实 SIP 入口接入测试记录。
2. 一份脱敏测试证据。
3. 一份失败场景验证记录。
4. 未解决问题清单。
5. 是否进入 Phase 02 的结论。

## 15. 进入下一阶段门禁

只有满足以下条件，才建议进入 AI 模型、话术和语义分析配置体系阶段：

1. 至少一次真实电话用户作为 SIP Participant 接入 Room 并完成 Agent 交互。
2. 至少覆盖未接、拒接或立即挂机中的两个失败场景。
3. SIP 用户加入、Agent 加入、播放、收音和退出都有日志。
4. 状态能够正常闭环。
5. 不存在会阻断真实电话体验的媒体问题。
