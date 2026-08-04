# AGENTS.md

本文件记录 AI Call 项目的长期协作约束。具体环境值、当前 IP、Linphone 注册状态、临时端口占用等易变信息，不放在这里；优先放到对应阶段的本地基线文档。

## 当前重点

- 当前只推进 SIP barge-in P1：本地快速打断门控。
- P2/P3 暂停，除非用户明确要求恢复。
- P1 目标是解决“本地 SIP 已检测到用户插话，但系统还等 Qwen user_speech_started，导致 AI 停播慢”的问题。
- 不要把 P1 调试扩散成供应商 AEC、深度降噪、ASR 语义相似度、字幕展示、dialogue 表结构调整等方案。

## 19011 P1 本地联调

- 排查或重启 19011 P1 前，必须先阅读 `docs/livekit-ai-outbound/p1-local-test-baseline.md`。
- 19011 用于 P1 SIP barge-in 本地联调。
- 19012 用于语义分析测试；除非用户明确要求，不要停 19012。
- 19011 必须使用隔离 LiveKit 栈，不要让 19011 和 19012 共用 LiveKit webhook、SIP service 或 Redis 状态。
- 19011 的数据库类型、容器名和动态端口属于易变运行态，必须以 `p1-local-test-baseline.md` 和现场核对为准；不要回退到历史 SQLite，也不要误连线上业务数据库。
- 如果需要重启 19011，优先按基线确认启动环境变量里包含：

```text
SERVER_PORT=19011
DATABASE_TYPE
DATABASE_HOST
DATABASE_PORT
DATABASE_NAME
DATABASE_USERNAME
DATABASE_PASSWORD
```

## SIP / Linphone 链路

- Linphone 不是直连 LiveKit。
- 当前本地软电话链路是：19011 AI Call API -> 19011 LiveKit SIP -> FreeSWITCH -> Linphone。
- 遇到 Linphone 不响铃、AI 听不到人声、突然挂断、LiveKit SIP Participant 创建失败时，先排查环境链路，再分析 P1 VAD/打断策略。
- 本机 IP 变化可能影响 Linphone 注册、FreeSWITCH 路由、SIP_PUBLIC_IP、SIP_EXTERNAL_RTP_IP 和浏览器访问地址。

## 配置和密钥

- 不要提交 `.env.dev`、`*.local.yaml`、本地 SQLite 数据库、录音文件、OSS access_key/secret_key、LiveKit API secret、DashScope API key。
- 文档中需要说明配置时，只写配置项名称和用途，不写真实密钥。
- 19011 的隔离本地数据库可以包含 active OSS 配置用于录音上传；这不等于可以写线上业务数据库。

## 通话分析原则

- 分析 P1 通话时，优先结合事件日志、分轨录音、LiveKit SIP 日志、FreeSWITCH/Linphone 注册状态。
- 不要只凭主观体验直接调 VAD 阈值。
- 打断不再按业务场景单独授权；如果全局开关关闭但仍发生打断，先查 effective config 和事件链路。
- 如果出现打断慢、误打断、漏打断，要先区分环境链路、媒体上行、客户轨隔离、generation gate、confirmed/rejected 逻辑，再决定是否改代码。

## 修改策略

- 不做补丁式场景堆叠；优先定位根因和系统边界。
- P1 改动应围绕输入隔离、VAD/噪声证据、candidate/pre-stop、generation gate、clean window confirmed/rejected、rejected 恢复、事件和测试闭环。
- 每次改动后应尽量跑相关工程类回归测试；无法运行时要说明原因。
- 真实通话样本不足时，优先沉淀可复放的录音、事件和 shadow/replay 分析，而不是靠一次次手动拨号猜阈值。
