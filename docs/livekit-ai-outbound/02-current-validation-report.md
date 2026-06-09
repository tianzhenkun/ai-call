# 当前验证情况说明

最后更新：2026-06-08

## 1. 文档定位

本文档记录 `LiveKit SIP + AI Agent` 智能外呼项目当前已经验证的事实、真实拨测证据、尚未验证的边界和下一步测试建议。

本文档只记录事实，不替代架构设计。架构目标以 [01-architecture.md](01-architecture.md) 为准。

## 2. 当前结论

当前已完成一次真实 SIP 外呼基础链路拨测：

```text
线路服务商
  -> LiveKit SIP
  -> LiveKit Server / Room
  -> SIP Participant
  -> 手机真实接听
```

结论：

1. 真实 SIP 外呼基础链路已经成立。
2. 手机可以震铃并接听。
3. 电话侧媒体可以进入 LiveKit Room。
4. 真实协商 codec 为 `PCMU/8000`。
5. 当前没有接入 Agent Worker，不能说明 AI 双向通话已跑通。

## 3. 已知线路信息

当前服务商给出的线路信息：

| 项目 | 当前记录 |
|---|---|
| SIP proxy | `47.94.86.132:5089` |
| 主叫号码 | `037123124845` |
| 传输协议 | UDP |
| 号码格式 | 国内原始手机号，不加 `+86` / `86` / `0` / `9` |
| Codec | 历史预期为 `PCMA/8000`，真实拨测选择 `PCMU/8000` |
| DTMF | `telephone-event` / RFC2833，payload `101` |
| RTP profile | `RTP/AVP` |

基于 2026-06-08 真实拨测和当前已确认线路规则，可以先形成以下判断：

1. 本次 LiveKit outbound trunk 未使用账号密码鉴权，且拨测成功，因此当前测试链路可以按 IP 白名单线路理解。
2. 本次可用测试服务器公网 IP 为 `111.229.146.182`，LiveKit SIP signaling 端口为 `5080`。
3. 当前正式线路需要绑定公网 IP 和 SIP 端口。
4. 后续如果更换公网 IP、SIP 端口、主叫号或部署多节点，需要服务商重新开放白名单并调整线路配置。

生产上线前仍需要确认的事项应聚焦实际生产影响：

1. 服务商要求的 From、Contact、Via、rport 和号码格式。
2. 是否允许同一主叫号并发外呼。
3. CPS、并发、失败码和风控规则。

## 4. 真实拨测记录

2026-06-08 已在公网测试服务器完成一次真实线路拨测。该记录只证明 SIP trunk 到 LiveKit Room 的基础链路成立，不等同于完整生产验收。

| 项目 | 记录 |
|---|---|
| 测试时间 | 2026-06-08 11:36 CST |
| 测试服务器公网 IP | `111.229.146.182` |
| 线路服务商地址 | `47.94.86.132:5089` |
| 传输协议 | SIP UDP |
| 主叫号码 | `037123124845` |
| 被叫号码 | 已脱敏，不写入仓库文档 |
| LiveKit SIP 端口 | `5080` |
| LiveKit SIP RTP 范围 | `16384-16484` |
| LiveKit Server 版本 | `1.12.0` |
| LiveKit SIP 版本 | `v1.3.1` |
| Outbound trunk ID | `ST_PKaqydpfaCDn` |
| LiveKit SIP call ID | `SCL_7T8xb59wczAK` |
| SIP participant ID | `PA_qTRL8eX8Ha4r` |
| Room | `stage1-real-room-20260608-113609` |
| SIP Call-ID | `f54lmIfY2pAGQdiUEQbxwME0bLs` |
| 结果 | 手机震铃、人工接听、说话后由远端挂机 |
| 结束日志 | `result=success, reason=bye` |
| 呼叫建立耗时 | `inviteToAcceptMs=12319`，约 12.3 秒 |
| 远端 SDP answer | `c=IN IP4 47.94.86.132`，`m=audio 18926 RTP/AVP 0 101` |
| 实际协商编码 | `PCMU/8000`，payload `0` |
| DTMF | `telephone-event/8000`，payload `101` |
| ptime | `20` |
| RTP 统计 | `streams=1`，`audio_packets=619`，`audio_bytes=99040` |
| 丢包统计 | `jitter_buffer_packets_lost=0`，`jitter_buffer_packets_dropped=0` |

测试产物保留在服务器：

```text
/opt/livekit-stage1/state/real-call-direct-20260608-113609.pcap
/opt/livekit-stage1/state/real-call-direct-20260608-113609.log
/opt/livekit-stage1/state/real-participant-direct-20260608-113609.json
/opt/livekit-stage1/state/real-participant-direct-20260608-113609.out
```

## 5. 本次测试使用的组件

| 组件 / 能力 | 本次是否使用 | 作用 | 验证结果 |
|---|---:|---|---|
| LiveKit Server | 是 | 提供 Room、Participant、媒体路由、API | Room 创建成功，SIP participant 加入成功 |
| LiveKit SIP | 是 | 向线路商发起 SIP INVITE，桥接 SIP/RTP 到 LiveKit Room | 外呼、接听、RTP 收发成功 |
| SIP Outbound Trunk | 是 | 保存线路商地址、主叫号码、传输协议等出局线路配置 | trunk 创建成功 |
| SIP Participant | 是 | 表示真实电话侧用户 | 成功加入 Room |
| LiveKit Room | 是 | 承载本次通话会话 | 正常创建和关闭 |
| LiveKit API / Twirp | 是 | 创建 outbound trunk 和 SIP participant | `CreateSIPParticipant` 返回成功 |
| Redis | 是 | LiveKit Server / SIP 的运行依赖和状态协调 | 未出现 Redis 相关错误 |
| Agent Worker | 否 | AI 对话、ASR、LLM、TTS、打断 | 本次未验证 |
| LiveKit Egress | 否 | Room 录音、分轨、导出 | 本次未验证 |
| WebRTC 坐席 / Web 入口 | 否 | 浏览器入口和人工接管 | 本次未验证 |
| 国内 ASR / LLM / TTS | 否 | AI 语音对话 | 本次未验证 |

## 6. 已验证边界

已验证：

1. 当前公网 IP、端口和线路服务商之间可以完成真实 SIP 外呼。
2. LiveKit SIP 能与该线路商完成 SDP 协商。
3. 电话侧媒体能接入 LiveKit Room。
4. 服务商实际选择 `PCMU/8000`。
5. 本次未观察到 RTP 丢包。

## 7. 未验证边界

尚未验证：

1. Agent Worker 加入同一个 Room。
2. Agent 播放音频后，电话侧能稳定听到。
3. 电话侧说话后，Agent 能稳定收到并完成 ASR。
4. 用户打断、TTS 取消、LLM 取消和播放尾音清理。
5. Egress 录音、分轨、落库和 `sys_oss` 展示。
6. WebRTC 坐席接管。
7. 并发、CPS、失败码和服务商风控规则。
8. 被叫未接、拒接、忙线、空号、异常号等失败场景。

## 8. 与现有 FreeSWITCH 链路的关系

真实拨测期间曾临时让出现有 FreeSWITCH 使用的 `5080` 端口。拨测结束后，LiveKit 测试容器已停止，现有 `sip_realtime_freeswitch` 和 `recov-ten-gateway.service` 已恢复。

这段临时操作不应作为生产部署方案复用。后续真实拨测必须固化 runbook，至少包含：

1. 现场快照。
2. 端口占用检查。
3. 服务切换步骤。
4. 健康检查。
5. 回滚步骤。
6. 操作人和操作时间记录。

当前真实 SIP 拨测操作说明见：[ops/real-sip-line-runbook.md](ops/real-sip-line-runbook.md)。

## 9. 现有系统可复用内容

当前 Python 网关已有可复用的业务经验：

```text
app/main.py
app/realtime_phone_gateway.py
app/call_control.py
app/freeswitch_event_socket.py
app/freeswitch_media.py
app/doubao_s2s_client.py
app/doubao_s2s_realtime.py
app/playout_engine.py
app/playout_controller.py
app/voice_activity.py
```

可复用的是业务语义和状态经验，不是 FreeSWITCH 绑定本身：

1. 外呼请求参数格式。
2. 业务回调语义。
3. 开场白、话术和变量填充。
4. 播放状态和打断处理经验。
5. 通话状态机。
6. 失败码映射。
7. 日志字段和排障经验。
8. `request_id`、`business_type`、`business_id`、号码等业务关联信息贯穿链路。

## 10. 下一步验证建议

下一步优先做最小 Agent 验证：

```text
电话接通
  -> Agent 加入同一个 Room
  -> Agent 播放固定话术
  -> 电话侧确认能听到
  -> Agent 记录电话侧音频 / VAD / ASR 结果
```

通过标准：

1. 手机能震铃并接通。
2. SIP Participant 加入目标 Room。
3. Agent Participant 加入同一个 Room。
4. 电话侧能听到 Agent 的固定话术。
5. Agent 能收到电话侧音频。
6. 至少能记录 VAD 或 ASR 事件。
7. 用户挂机后 Room、SIP call、Agent job 状态正常结束。

只有这个最小 Agent 验证通过后，才能说 `电话 <-> LiveKit <-> Agent` 双向媒体闭环成立。
