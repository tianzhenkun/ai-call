# Owner Runtime 客户分轨 19011 真实验收证据

日期：2026-08-03

## 结论

本轮结论为 **GO**。

ed81 Owner Runtime 经现有 19011 LiveKit / SIP / Egress 和 FreeSWITCH 向白名单 Linphone
完成一次真实外呼。通话接通、双向对话、客户挂断、Owner 终态、主录音、客户分轨、OSS
回查、离线客户轨 ASR 和话后语义分析均已形成可审计证据。

本结论只覆盖本次白名单 19011 链路，不外推到其他号码、环境或线路。

## 基线与范围

- worktree：`/Users/liuhongli/.codex/worktrees/ed81/ai-call`
- branch：`codex/ai-call-workflow-split`
- 起始 HEAD：`6f2370f90a23409e3a9cba2ed64a55bca344461d`
- call_id：`call_342621986868490240`
- command_id：`342621986868490242`
- 被叫白名单：`199****1001`
- 入口：`direct_sip`
- 隔离租户：`rt19011`
- 另一个 cf0a 测试进程未停止、未清理、未纳入验收
- `.playwright-cli/` 与
  `env/.env.dev.bak-before-local-outbound-20260727` 保持不变

## No-dial 预检

- 19011 API health：`200`；
- 19011 LiveKit HTTP：`200`；
- Owner Runtime READY worker：`1`；
- Linphone `1000@192.168.0.111`：`Registered(UDP-NAT)`、`Reachable`；
- active OSS config：`1`；
- 提交前：`record=0`、`command=0`、`effect=0`、`target_key=0`；
- 真实 Provider、`direct_sip` Owner entry、单号码 allowlist 和录音开关显式启用。

## 通话与 Owner 终态

- `START_CALL=SUCCEEDED`；
- `END_CALL=SUCCEEDED`；
- Record：`completed`；
- end reason：`sip_participant_left`；
- answered_at：`2026-08-03 10:57:32.474289+00`；
- ended_at：`2026-08-03 10:58:41.208211+00`；
- duration_ms：`68733`；
- dialogue persistence：`complete`；
- resource cleanup：`clean`。

十个生命周期 Effect 全部 `APPLIED`：

```text
CREATE_ROOM
ATTACH_AGENT_PARTICIPANT
CREATE_SIP_PARTICIPANT
START_EGRESS
START_TRACK_EGRESS
DISCONNECT_AGENT_PARTICIPANT
HANGUP_SIP
STOP_EGRESS
STOP_TRACK_EGRESS
DELETE_ROOM
```

## 对话与媒体

持久化对话共 11 段：

- 客户实时 final：5 段；
- AI final：3 段；
- AI interrupted：2 段；
- 客户离线 ASR final：1 段。

客户实时语音形成连续问答；客户挂断话术触发 `sip_participant_left`。离线客户轨 ASR 任务
使用 `qwen3-asr-flash-filetrans`，状态 `completed`，结果只含客户语音，没有混入 AI 文本。
话后语义分析状态为成功（`2`）。

## 主录音、客户分轨与 OSS

主录音：

- Egress：`EG_SoiL3MexpC2B`；
- 状态：`completed`；
- OSS ID：`342622295251996672`；
- 对象：`ai-call/recordings/call_342621986868490240.mp3`；
- DB duration：`64129 ms`；
- 鉴权 `ffprobe`：MP3，`64.052245 s`，`1024573 bytes`。

客户分轨：

- role：`customer`；
- participant：`sip-call_342621986868490240`；
- Egress：`EG_CXBEfUr8fGeq`；
- 状态：`completed`；
- OSS ID：`342622316856856576`；
- 对象：`ai-call/recordings/tracks/call_342621986868490240/customer-sip-call_342621986868490240.ogg`；
- DB duration：`73043 ms`；
- 鉴权 `ffprobe`：OGG，`60.548250 s`，`984777 bytes`。

LiveKit StopEgress 返回已完成态的 HTTP 412 后，服务通过鉴权 OSS HEAD 回查恢复：主录音与
客户分轨均为 `200`，最终状态均收敛为 `completed` 且无 failure。

公开域名直接 HEAD 为 `403`，符合私有桶访问策略；使用项目已有短时签名后，两份媒体均可被
`ffprobe` 读取，不需要修改桶权限。

## 非阻断后续项

- 客户分轨 DB duration `73043 ms` 与媒体容器 duration `60.548250 s` 存在约 12.5 秒差异，
  登记为 Provider 时长口径一致性后续项；不扩大本 Task。
- 客户实时 final 为 5 段，离线文件 ASR 仅输出 1 段；当前证据证明客户轨可读且没有 AI
  文本混入，但不把本次单样本升级为离线 ASR 完整率结论。需要完整率门禁时再做录音回放集。

## 清理

- ed81 19011 临时 API 已优雅关闭；
- 本轮临时 PostgreSQL 容器已停止并删除；
- 现有 19011 LiveKit/SIP/Egress/Redis 与 FreeSWITCH 保持运行；
- 未再次拨号，未触碰另一个测试环境或受保护文件。
