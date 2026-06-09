# 智能外呼模块数据模型设计

最后更新：2026-06-09

## 1. 文档定位

本文档定义智能外呼模块的数据表结构、字段语义、业务场景和枚举值说明。

本文档只说明“表应该如何设计、每张表解决什么问题、字段值分别对应什么场景”。具体哪个阶段创建哪张表，以 `phases/` 下对应阶段文档为准。

## 2. 设计原则

### 2.1 独立模块原则

智能外呼模块作为独立功能模块建设，拥有自己的通话、事件、参与者、消息、录音、分析结果、线路和配置数据模型。

外部业务系统通过以下字段与本模块建立弱关联：

```text
request_id
business_type
business_id
request_payload
```

本模块不直接依赖业务侧任务表、客户表、批次表或催收表。

### 2.2 低延迟优先原则

实时通话链路优先保证低延迟：

```text
实时音频
  -> ASR
  -> LLM
  -> TTS
  -> 播放和打断
```

实时链路不等待数据库、OSS、质检或外部回调。

数据写入原则：

1. 通话主状态必须可靠更新。
2. 数据库只记录稳定事件、最终文本、录音索引和分析结果。
3. 消息、事件、录音、分析结果允许异步写入。
4. 写库失败不能阻塞实时通话。
5. 通话结束后允许通过日志、录音和后台任务补偿分析结果。

### 2.3 数据库兼容规范

1. 不使用 `jsonb` 等强 PostgreSQL 绑定类型。
2. JSON 数据优先用 `text` / `varchar` 存字符串，方便后续换库。
3. 不创建物理外键。
4. 业务关联靠代码校验、普通索引和唯一约束维护。
5. 能用普通索引和业务唯一约束表达关系的，不用数据库外键强绑定。
6. `bigint` 主键或业务 ID 返回前端时统一按字符串处理，避免 JavaScript 精度丢失。

### 2.4 公共字段规范

智能外呼模块自有表必须包含以下公共字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

说明：

1. 下文每张智能外呼模块自有表的字段清单均显式列出公共字段。
2. 前端接收 `id`、`call_id`、`oss_id` 等大整数或业务 ID 时，统一按字符串处理。
3. 不通过物理外键约束 `call_id`、`participant_id`、`oss_id` 等关系，关系由代码和索引保证。
4. `tenant_id`、`create_dept`、`create_by`、`update_by` 不进入智能外呼模块自有表；`sys_oss` 是既有基座表，保留其原有字段结构。

## 3. 表清单

| 表名 | 作用 | 核心程度 |
|---|---|---|
| `ai_call_session` | 外呼会话主表，一次 Web 业务会话或真实外呼对应一条记录 | 核心 |
| `ai_call_participant` | 通话参与者表，记录用户、Agent、人工坐席等 Participant | 核心 |
| `ai_call_event` | 通话事件流水表，用于审计、排障和状态追踪 | 核心 |
| `ai_call_message` | 轻量对话明细表，记录稳定文本和打断标识 | 核心 |
| `ai_call_recording` | 录音表，记录混音和分轨录音文件业务信息 | 生产必需 |
| `ai_call_analysis` | 通话语义分析结果表，记录通话结束后的 JSON 分析结果 | 生产增强 |
| `ai_agent_config` | Agent 行为配置表 | 后续配置模块草案，当前阶段不创建 |
| `ai_model_config` | ASR / LLM / TTS / 通话后分析模型配置表 | 后续配置模块草案，当前阶段不创建 |
| `ai_script_config` | 话术配置表，可由业务侧传参替代 | 后续配置模块草案，当前阶段不创建 |

说明：

1. `ai_agent_config`、`ai_model_config`、`ai_script_config` 当前只保留方向设计，不作为 Phase 00 / Phase 01 的建表要求。
2. Phase 00 / Phase 01 通过程序默认值和环境变量完成 Agent、模型和话术配置，并把实际执行参数写入 `ai_call_session` 的配置快照字段。
3. `agentConfig`、`modelConfig`、`script`、`variables` 等高级入参不在 Phase 00 创建接口中开放，统一放到 Phase 02 配置体系评审。
4. 音色配置与自定义音色作为独立阶段建设，当前不在 `ai_model_config` 中提前设计音色字段。
5. 后续真正建设配置模块时，应根据当时的运营台、模型供应商、提示词管理、灰度发布和权限边界重新评审字段，不要求机械沿用本稿。

## 4. ai_call_session

### 4.1 表作用

`ai_call_session` 是外呼会话主表。一次外呼请求、一次 Web 业务会话或一次真实电话 Agent 通话，都对应一条 session。

外部接口异步创建外呼时，应该立即返回 `call_id`，调用方后续通过 `call_id` 查询状态。

### 4.2 业务场景

1. 外部业务系统调用接口发起真实外呼。
2. 外部界面或运营台发起真实外呼。
3. 浏览器创建 Web 业务会话。
4. 请求创建成功但受线路并发、CPS、时间窗口或 Worker 资源限制，进入 `QUEUED`。
5. Worker 异步执行拨号。
6. 记录 LiveKit Room、SIP Call-ID、失败原因和结束原因。
7. 外部业务通过 `business_type + business_id` 关联自己的业务记录。
8. 后续录音、消息、事件、分析结果都通过 `call_id` 关联本表。

### 4.2.1 业务用例

**用例 1：业务系统发起真实外呼**

业务侧传入 `request_id`、`business_type`、`business_id`、主叫号、被叫号和必要配置参数。模块校验幂等后生成 `call_id`，使用程序配置中的 SIP 线路参数执行拨号，落库后返回 `call_id` 和 `status=QUEUED`。

**用例 2：同一业务对象重试外呼**

第一次外呼失败后，业务侧用新的 `request_id` 再次发起，`business_type + business_id` 保持不变。这样同一个业务对象下可以查询到多次外呼记录。

**用例 3：同一次请求重复提交**

如果 HTTP 超时或前端重复点击，业务侧使用相同 `request_id` 再次提交。模块根据 `request_id` 返回已有 `call_id`，不重复拨号。

**用例 4：Web 业务会话**

Web 业务会话也创建 `ai_call_session`，但 `channel=web_call`，SIP Call-ID 等真实线路字段为空。配置快照记录本次实际使用的 Agent、模型和话术参数。

**用例 5：后续阶段业务侧使用配置参数**

Phase 00 不支持业务侧直接传 Agent、模型、话术参数。后续进入 Phase 02 后，再评审是允许业务侧传高级参数，还是只允许传 `agent_config_id`、`model_config_id`、`script_config_id` 等配置引用。无论采用哪种方式，模块都应将最终实际执行参数保存到 `agent_config_snapshot`、`model_config_snapshot`、`script_snapshot`，便于复盘。

### 4.3 字段结构

| 字段 | 类型 | 说明 | 来源 | 是否直接存入参 |
|---|---|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 | 模块生成 | 否 |
| `call_id` | `varchar(64)` | 通话业务 ID，对外返回的唯一标识 | 模块生成 | 否 |
| `request_id` | `varchar(128)` | 调用方请求 ID，用于幂等；同一次请求重试必须相同 | 业务侧传入 | 是 |
| `channel` | `varchar(32)` | 通话渠道 | 业务侧传入或接口决定 | 是 |
| `status` | `varchar(32)` | 通话状态 | 模块维护 | 否 |
| `caller_number` | `varchar(32)` | 主叫号码 | 业务侧传入或线路默认值 | 是 |
| `callee_number_cipher` | `varchar(512)` | 被叫号码密文，用于异步拨号前解密 | 模块由被叫明文加密派生 | 否 |
| `callee_number_mask` | `varchar(32)` | 被叫号码脱敏值，用于页面展示和日志排查 | 模块由被叫明文脱敏派生 | 否 |
| `business_type` | `varchar(64)` | 外部业务类型，例如 collection、notify | 业务侧传入 | 是 |
| `business_id` | `varchar(128)` | 外部业务对象 ID，不建物理外键 | 业务侧传入 | 是 |
| `request_payload` | `text` | 本次请求参数快照，JSON 字符串 | 业务侧请求整体 | 是 |
| `agent_config_snapshot` | `text` | 本次 Agent 实际执行配置快照，JSON 字符串 | 业务侧传入或模块默认配置解析 | 条件是 |
| `model_config_snapshot` | `text` | 本次 ASR / LLM / TTS 实际执行配置快照，JSON 字符串 | 业务侧传入或模块默认配置解析 | 条件是 |
| `script_snapshot` | `text` | 本次话术实际执行快照，JSON 字符串 | 业务侧传入或模块默认话术解析 | 条件是 |
| `livekit_room_name` | `varchar(128)` | LiveKit Room 名称 | 模块生成 | 否 |
| `sip_call_id` | `varchar(128)` | SIP 信令 Call-ID，可空 | SIP 运行产生 | 否 |
| `provider_call_id` | `varchar(128)` | 服务商侧可关联 ID，可空 | 服务商或 SIP 侧返回 | 否 |
| `livekit_sip_call_id` | `varchar(128)` | LiveKit SIP call ID，可空 | LiveKit SIP 返回 | 否 |
| `queued_at` | `timestamp` | 进入待执行时间 | 模块维护 | 否 |
| `dialing_at` | `timestamp` | 开始拨号时间 | 模块维护 | 否 |
| `ringing_at` | `timestamp` | 被叫振铃时间 | 模块维护 | 否 |
| `answered_at` | `timestamp` | 被叫接听时间 | 模块维护 | 否 |
| `ended_at` | `timestamp` | 通话结束时间 | 模块维护 | 否 |
| `fail_code` | `varchar(64)` | 失败码 | 模块归一 | 否 |
| `fail_reason` | `varchar(500)` | 失败原因 | 模块归一 | 否 |
| `hangup_reason` | `varchar(64)` | 挂机原因 | 模块归一 | 否 |
| `create_time` | `timestamp` | 创建时间 | 模块维护 | 否 |
| `update_time` | `timestamp` | 更新时间 | 模块维护 | 否 |

### 4.4 多值字段说明

`channel`：

| 值 | 场景 |
|---|---|
| `sip_outbound` | 真实 SIP 外呼，消耗运营商线路 |
| `web_call` | 浏览器 WebRTC 业务会话，不拨真实电话 |

`status`：

| 值 | 场景 |
|---|---|
| `CREATED` | 请求已创建，尚未进入执行队列 |
| `QUEUED` | 已进入待执行状态，等待线路、CPS、Worker 或时间窗口 |
| `DIALING` | 已开始创建 SIP Participant 或发起 INVITE |
| `RINGING` | 被叫侧正在振铃 |
| `ANSWERED` | 被叫已接听 |
| `IN_PROGRESS` | 通话进行中，AI 或人工正在沟通 |
| `HUMAN_TRANSFER_REQUESTED` | AI 或规则触发转人工请求 |
| `HUMAN_TRANSFER_CONNECTED` | 人工坐席已加入并接管 |
| `COMPLETED` | 通话正常结束 |
| `FAILED` | 通话失败，例如线路失败、号码异常、Agent 启动失败 |
| `CANCELED` | 拨号前被主动取消 |
| `TIMEOUT` | 超时结束，例如未接超时、排队超时、静音超时 |

### 4.5 配置快照字段说明

`agent_config_snapshot`：

1. 保存本次实际使用的 Agent 行为参数。
2. 可来自业务侧入参，也可来自模块默认配置。
3. 常见内容包括 Agent 名称、打断开关、静音超时、最大通话时长、是否允许转人工。
4. 如果参数来自 `ai_agent_config`，快照中应包含 `agent_config_id` 和 `config_version`。
5. 用于复盘“这通电话当时用的是什么 Agent 行为”，不依赖后续配置是否被修改。

`model_config_snapshot`：

1. 保存本次实际使用的 ASR / LLM / TTS 参数。
2. 可来自业务侧入参，也可来自模块默认配置。
3. 常见内容包括 ASR provider、ASR model、LLM provider、LLM model、TTS provider、TTS model、超时、温度、流式开关。
4. 密钥不写入快照，只保存 `secret_ref` 或供应商配置引用。
5. 如果参数来自 `ai_model_config`，快照中应按 ASR / LLM / TTS 分别包含 `model_config_id` 和 `config_version`。
6. 用于排查识别差、回复慢、播放异常、模型成本异常等问题。

`script_snapshot`：

1. 保存本次实际使用的话术参数。
2. 可来自业务侧入参，也可来自模块默认话术。
3. 常见内容包括开场白、system prompt、话术正文、变量填充值。
4. 如果参数来自 `ai_script_config`，快照中应包含 `script_config_id` 和 `script_version`。
5. 用于复盘 AI 为什么这么说，以及配置变更后还原当时执行环境。

## 5. ai_call_participant

### 5.1 表作用

`ai_call_participant` 记录一次通话中的参与者。LiveKit Room 中的 SIP 用户、Web 用户、Agent 和人工坐席都应有参与者记录。

### 5.2 业务场景

1. 真实电话用户作为 SIP Participant 加入 Room。
2. 浏览器 Web 用户作为 WebRTC Participant 加入 Room。
3. Agent Worker 作为 Agent Participant 加入 Room。
4. 人工坐席转人工时加入同一个 Room。
5. 排查谁在什么时候加入、离开、静音、发布音轨。
6. 录音分轨时定位用户、Agent、人工坐席的 track。
7. 转人工时判断 AI 是否已退出或静音。

### 5.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `participant_id` | `varchar(64)` | 模块内参与者 ID |
| `call_id` | `varchar(64)` | 通话 ID |
| `participant_type` | `varchar(32)` | 参与者类型 |
| `participant_identity` | `varchar(128)` | LiveKit participant identity |
| `livekit_participant_sid` | `varchar(128)` | LiveKit participant SID |
| `audio_track_sid` | `varchar(128)` | 主要音频 track SID，可空 |
| `display_name` | `varchar(128)` | 展示名称 |
| `join_time` | `timestamp` | 加入时间 |
| `leave_time` | `timestamp` | 离开时间 |
| `status` | `varchar(32)` | 参与者状态 |
| `muted` | `smallint` | 是否静音，0 否，1 是 |
| `metadata` | `text` | 附加信息，JSON 字符串 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 5.4 多值字段说明

`participant_type`：

| 值 | 场景 |
|---|---|
| `sip_user` | 真实电话用户，通过 LiveKit SIP 加入 |
| `web_user` | 浏览器 Web 用户，通过 WebRTC 加入 |
| `agent` | AI Agent Worker |
| `human` | 人工坐席，通过 WebRTC 加入 |

`status`：

| 值 | 场景 |
|---|---|
| `JOINING` | 正在加入 Room |
| `JOINED` | 已加入 Room |
| `LEFT` | 已离开 Room |
| `FAILED` | 加入失败或异常断开 |

## 6. ai_call_event

### 6.1 表作用

`ai_call_event` 是通话事件流水表，用于记录关键状态变化、媒体事件、Agent 事件、录音事件和错误事件。

主表只保存当前状态，事件表保存“状态为什么变成这样”。

### 6.2 业务场景

1. 排查外呼为什么失败。
2. 审计通话状态变化。
3. 记录 SIP、Agent、录音、转人工的关键过程。
4. 前端展示运行日志。
5. 通话结束后复盘用户是否接听、是否打断、是否转人工。
6. 后台任务补偿时根据事件判断处理进度。

### 6.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `event_id` | `varchar(64)` | 事件 ID |
| `call_id` | `varchar(64)` | 通话 ID |
| `participant_id` | `varchar(64)` | 关联参与者 ID，可空 |
| `event_type` | `varchar(64)` | 事件类型 |
| `event_level` | `varchar(16)` | 事件级别 |
| `event_time` | `timestamp` | 事件发生时间 |
| `event_payload` | `text` | 事件详情，JSON 字符串 |
| `error_code` | `varchar(64)` | 错误码，可空 |
| `event_message` | `varchar(1000)` | 事件说明 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 6.4 多值字段说明

`event_level`：

| 值 | 场景 |
|---|---|
| `INFO` | 正常流程事件 |
| `WARN` | 可恢复异常或降级事件 |
| `ERROR` | 导致失败或需要人工排查的错误 |

`event_type`：

| 值 | 场景 |
|---|---|
| `call_created` | 外呼请求创建成功 |
| `call_queued` | 进入待执行队列 |
| `room_created` | LiveKit Room 准备完成，主要用于 Web 会话和统一 Room 编排 |
| `web_token_issued` | 签发 Web 用户 LiveKit 入会 token |
| `web_user_joined` | 浏览器 Web 用户加入 Room |
| `web_user_left` | 浏览器 Web 用户离开 Room |
| `sip_invite_sent` | 已发起 SIP INVITE |
| `sip_ringing` | 被叫振铃 |
| `sip_answered` | 被叫接听 |
| `agent_dispatch_started` | 开始调度 Agent |
| `agent_joined` | Agent 加入 Room |
| `agent_left` | Agent 离开 Room |
| `asr_final` | 用户一句话 ASR final 已生成 |
| `agent_text_created` | Agent 回复文本已生成 |
| `tts_started` | TTS 开始 |
| `tts_finished` | TTS 播放完成 |
| `barge_in` | AI 播放过程中被中断，例如用户说话打断 |
| `human_transfer_requested` | 触发转人工；转人工来源写入 `event_payload.transfer_source` |
| `human_joined` | 人工坐席加入 |
| `agent_muted` | 转人工后 AI 静音 |
| `recording_started` | 录音开始 |
| `recording_finished` | 录音完成 |
| `recording_failed` | 录音失败 |
| `analysis_generated` | 通话语义分析结果生成 |
| `analysis_failed` | 通话语义分析失败 |
| `call_completed` | 通话正常结束 |
| `call_failed` | 通话失败 |
| `call_canceled` | 通话取消 |
| `call_timeout` | 通话超时 |

`human_transfer_requested` 的 `event_payload` 建议包含：

| 字段 | 说明 |
|---|---|
| `transfer_source` | 转人工来源 |
| `trigger_message_id` | 触发转人工的消息 ID，可空 |
| `reason_text` | 转人工原因说明 |
| `rule_code` | 系统规则编码，可空 |

`transfer_source`：

| 值 | 场景 |
|---|---|
| `user_intent` | 用户明确说“转人工”“找客服”“人工服务”等 |
| `agent_decision` | AI 根据上下文判断需要转人工 |
| `system_rule` | 系统规则触发，例如连续识别失败、异常兜底 |
| `manual_console` | 运营台或坐席台人工点击接管 |

如果用户在 AI 播放过程中说“转人工”，应同时记录：

1. `barge_in`：表示 AI 播放被用户说话打断。
2. `human_transfer_requested`：表示用户意图触发转人工，`transfer_source=user_intent`。

## 7. ai_call_message

### 7.1 表作用

`ai_call_message` 是轻量对话明细表，保存稳定、低频、对排障和人工接管有价值的文本。

本表当前只承载用户最终识别文本、AI 回复文本和必要的打断标识。

### 7.2 业务场景

1. 人工接管时查看 AI 和用户前面说了什么。
2. 通话后查看关键对话文本。
3. 判断 AI 回复是否被用户打断。
4. 通话结束后为摘要、质检提供基础文本输入。
5. 验证阶段查看 ASR final 和 Agent 回复是否符合预期。

### 7.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `message_id` | `varchar(64)` | 消息 ID |
| `call_id` | `varchar(64)` | 通话 ID |
| `participant_id` | `varchar(64)` | 说话参与者 ID，可空 |
| `speaker_type` | `varchar(32)` | 说话方 |
| `message_type` | `varchar(32)` | 消息类型 |
| `seq` | `bigint` | 通话内消息顺序 |
| `content_text` | `text` | 文本内容 |
| `start_time_ms` | `bigint` | 相对通话开始的起始时间，可空 |
| `end_time_ms` | `bigint` | 相对通话开始的结束时间，可空 |
| `confidence` | `varchar(32)` | ASR 置信度，可空；用字符串避免数据库差异 |
| `interrupted` | `smallint` | AI 回复是否被打断，0 否，1 是 |
| `played_duration_ms` | `bigint` | AI 回复被打断前已播放时长，可空 |
| `interrupt_reason` | `varchar(64)` | 打断原因，可空 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 7.4 多值字段说明

`speaker_type`：

| 值 | 场景 |
|---|---|
| `user` | 电话用户或 Web 用户说话 |
| `agent` | AI Agent 生成回复 |

`message_type`：

| 值 | 场景 |
|---|---|
| `asr_final` | 用户一句话的最终识别文本 |
| `agent_text` | AI 计划回复给用户的文本；如果被打断，标记 `interrupted=1` |

`interrupt_reason`：

| 值 | 场景 |
|---|---|
| `user_speech` | 用户说话打断 AI |
| `hangup` | 用户挂机导致 AI 播放中断 |
| `system_cancel` | 系统取消或超时导致播放中断 |

说明：

1. `agent_text` 表示 AI 准备说的话；实际播放情况通过 `interrupted` 和 `played_duration_ms` 辅助判断。
2. 如果被打断，记录 `interrupted=1` 和 `played_duration_ms`。

## 8. ai_call_recording

### 8.1 表作用

`ai_call_recording` 记录通话录音文件的业务信息。文件本体和文件索引继续复用基座 `sys_oss`，本表负责说明这个文件在智能外呼业务里是什么意思。

### 8.2 业务场景

1. 保存整通电话的混音录音。
2. 保存用户、AI、人工坐席分轨录音。
3. 通话回放。
4. 合规审计。
5. 质检和 ASR 重跑。
6. 排查用户是否听到 AI、AI 是否抢话、转人工录音是否连续。
7. 录音失败时记录失败原因。

### 8.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `recording_id` | `varchar(64)` | 录音业务 ID |
| `call_id` | `varchar(64)` | 通话 ID |
| `participant_id` | `varchar(64)` | 分轨录音对应参与者，可空 |
| `recording_type` | `varchar(32)` | 录音类型 |
| `recording_status` | `varchar(32)` | 录音状态 |
| `egress_id` | `varchar(128)` | LiveKit Egress ID，可空 |
| `track_sid` | `varchar(128)` | 分轨录音对应 track SID，可空 |
| `oss_id` | `bigint` | `sys_oss.oss_id`，不建物理外键 |
| `duration_seconds` | `bigint` | 录音时长，秒 |
| `fail_reason` | `varchar(500)` | 失败原因 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 8.4 多值字段说明

`recording_type`：

| 值 | 场景 |
|---|---|
| `mixed` | 混音录音，把用户、AI、人工声音混成一个文件；用于普通回放、合规审计、下载 |
| `user_track` | 用户分轨，只录电话用户或 Web 用户；用于 ASR 重跑、用户情绪、投诉和敏感词分析 |
| `agent_track` | AI 分轨，只录 AI 声音；用于检查 AI 是否按话术说、是否被打断、是否漏播 |
| `human_track` | 人工坐席分轨，只录人工坐席；用于转人工后的坐席质检和责任区分 |

`recording_status`：

| 值 | 场景 |
|---|---|
| `PENDING` | 已计划录音，但 Egress 尚未开始 |
| `RECORDING` | 正在录音 |
| `COMPLETED` | 录音完成并已关联 `oss_id` |
| `FAILED` | 录音失败 |
| `CANCELED` | 通话取消或录音任务被取消 |

说明：

1. 表结构支持混音和分轨，但阶段实现可以先只落 `mixed`。
2. 分轨录音必须依赖 `participant_id` 或 `track_sid`，否则后续无法判断录的是谁。
3. `sys_oss` 只负责文件索引，本表负责录音业务语义。
4. 文件名、原名、后缀、URL、存储服务等文件索引字段由 `sys_oss` 承载。

## 9. ai_call_analysis

### 9.1 表作用

`ai_call_analysis` 记录通话结束后的结构化语义分析结果。它主要在通话结束后异步生成，不进入实时通话链路。

当前阶段不把它设计成通用结果容器，只承载一类结果：语义分析 JSON 字符串。

### 9.2 业务场景

1. 通话结束后，基于 `ai_call_message`、关键事件和必要录音信息生成结构化分析结果。
2. 识别用户意向、标签、承诺信息、是否要求转人工、是否接通等业务语义。
3. 外部业务系统按 `call_id` 查询通话分析结果。
4. 分析失败时记录失败原因，便于后台任务重试。
5. 同一通电话分析重试时更新同一条记录，避免一通电话产生多条“当前结果”。

### 9.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `call_id` | `varchar(64)` | 通话 ID；同一 `call_id` 在本表只保留一条当前分析结果 |
| `analysis_status` | `varchar(32)` | 分析状态 |
| `analysis_result` | `text` | 语义分析 JSON 字符串 |
| `generated_at` | `timestamp` | 分析成功生成时间 |
| `fail_reason` | `varchar(500)` | 失败原因 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 9.4 多值字段说明

`analysis_status`：

| 值 | 场景 |
|---|---|
| `PENDING` | 通话已结束，等待后台任务生成分析结果 |
| `PROCESSING` | 后台任务正在生成分析结果 |
| `COMPLETED` | 分析成功，`analysis_result` 已写入 |
| `FAILED` | 分析失败，`fail_reason` 记录失败原因 |

说明：

1. 本表主要用于通话后异步分析结果，不参与实时通话。
2. 当前仅支持 JSON 语义分析结果，结果内容固定写入 `analysis_result`。
3. 一通电话在本表最多保留一条当前分析结果，建议对 `call_id` 建业务唯一约束。
4. 分析失败后重试时更新同一条记录，不新增多条结果记录。
5. 生成分析使用的实时模型配置可从 `ai_call_session.model_config_snapshot` 追溯；如果后续引入独立的通话后分析模型，再补充独立快照字段。
6. 人工接管实时上下文由 `ai_call_message` 承载。

## 10. ai_agent_config

### 10.1 表作用

`ai_agent_config` 记录 Agent 行为配置。调用方可以直接传参数，也可以引用配置表。本表用于模块自有运营台、配置复用和灰度发布。

当前阶段不创建本表。本节是后续配置模块的预设计草案，真正落地前需要结合实际 Agent 参数、运营台能力和灰度策略重新评审。

### 10.2 业务场景

1. 设置最大通话时长。
2. 设置静音超时。
3. 设置是否允许打断。
4. 设置是否允许转人工。
5. 设置 Agent 名称和执行参数。
6. Web 入口验证或运营配置时切换不同 Agent 行为。
7. 生产外呼时按配置版本复盘。

### 10.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `agent_config_id` | `varchar(64)` | Agent 配置业务 ID |
| `config_name` | `varchar(128)` | 配置名称 |
| `config_version` | `varchar(32)` | 配置版本 |
| `agent_name` | `varchar(128)` | LiveKit Agent 名称 |
| `max_call_seconds` | `int` | 最大通话时长 |
| `silence_timeout_ms` | `int` | 静音超时时间 |
| `barge_in_enabled` | `smallint` | 是否允许打断，0 否，1 是 |
| `human_transfer_enabled` | `smallint` | 是否允许转人工，0 否，1 是 |
| `params_text` | `text` | 其他 Agent 参数，JSON 字符串 |
| `status` | `varchar(32)` | 配置状态 |
| `remark` | `varchar(500)` | 备注 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 10.4 多值字段说明

`status`：

| 值 | 场景 |
|---|---|
| `DRAFT` | 草稿，未发布 |
| `ENABLED` | 可用于 Web 会话或外呼 |
| `DISABLED` | 禁用 |
| `ARCHIVED` | 历史归档，不再使用 |

## 11. ai_model_config

### 11.1 表作用

`ai_model_config` 记录 ASR、LLM、TTS 和通话后分析模型配置。密钥不存明文，只保存 `secret_ref`。

调用方可以直接传模型参数，也可以引用配置表。本表用于 Web 入口验证、模型切换和生产复盘。

当前阶段不创建本表。本节是后续配置模块的预设计草案，真正落地前需要结合实际模型供应商、密钥管理、降级策略、成本统计和延迟要求重新评审。

音色配置不放入本表。内置音色、自定义音色、试听、训练状态、供应商 `voice_id` 映射和音色权限，应在独立音色阶段重新评审数据模型。

### 11.2 业务场景

1. 配置流式 ASR。
2. 配置 LLM。
3. 配置流式 TTS。
4. Web 入口验证时切换模型。
5. 生产外呼时记录使用哪个模型版本。
6. 通话结束后使用独立模型生成语义分析结果。
7. 模型超时、降级和成本分析。

### 11.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `model_config_id` | `varchar(64)` | 模型配置业务 ID |
| `config_name` | `varchar(128)` | 配置名称 |
| `config_version` | `varchar(32)` | 配置版本 |
| `model_type` | `varchar(32)` | 模型类型 |
| `provider_name` | `varchar(64)` | 供应商 |
| `model_name` | `varchar(128)` | 模型名 |
| `base_url` | `varchar(500)` | 接口地址 |
| `stream_enabled` | `smallint` | 是否流式，0 否，1 是 |
| `timeout_ms` | `int` | 超时时间 |
| `temperature` | `varchar(32)` | LLM 温度，用字符串保持兼容 |
| `secret_ref` | `varchar(256)` | API key / token 引用，不存明文 |
| `params_text` | `text` | 其他模型参数，JSON 字符串 |
| `status` | `varchar(32)` | 配置状态 |
| `remark` | `varchar(500)` | 备注 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 11.4 多值字段说明

`model_type`：

| 值 | 场景 |
|---|---|
| `ASR` | 语音转文字 |
| `LLM` | 对话和决策 |
| `TTS` | 文本转语音 |
| `POST_ANALYSIS` | 通话结束后的 JSON 语义分析；不参与实时通话链路 |

`status`：

| 值 | 场景 |
|---|---|
| `DRAFT` | 草稿 |
| `ENABLED` | 可使用 |
| `DISABLED` | 禁用 |
| `ARCHIVED` | 归档 |

## 12. ai_script_config

### 12.1 表作用

`ai_script_config` 记录话术配置。该表是可选能力。

如果话术由业务侧维护，外呼模块可以只接收本次调用参数，并在 `ai_call_session.script_snapshot` 保存快照，不强制使用本表。

当前阶段不创建本表。本节是后续配置模块的预设计草案，真正落地前需要结合提示词来源、变量填充方式、业务侧话术管理边界和版本发布方式重新评审。

### 12.2 业务场景

1. 外呼模块自带运营台时维护话术。
2. Web 入口验证时选择不同话术版本。
3. 生产外呼时固定话术版本，方便复盘。
4. 多业务线复用同一套外呼模块时，保留模块内默认话术。
5. 业务侧不想传完整 prompt 时，只传 `script_config_id`。

### 12.3 字段结构

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | `bigint` | 主键，建议使用雪花 ID 或统一 ID 服务生成 |
| `script_config_id` | `varchar(64)` | 话术配置业务 ID |
| `script_name` | `varchar(128)` | 话术名称 |
| `script_version` | `varchar(32)` | 话术版本 |
| `business_type` | `varchar(64)` | 适用业务类型 |
| `opening_text` | `text` | 开场白 |
| `system_prompt` | `text` | 系统 prompt |
| `script_body` | `text` | 话术正文或流程描述 |
| `variables_schema` | `text` | 变量定义，JSON 字符串 |
| `params_text` | `text` | 其他话术参数，JSON 字符串 |
| `status` | `varchar(32)` | 配置状态 |
| `remark` | `varchar(500)` | 备注 |
| `create_time` | `timestamp` | 创建时间 |
| `update_time` | `timestamp` | 更新时间 |

### 12.4 多值字段说明

`status`：

| 值 | 场景 |
|---|---|
| `DRAFT` | 草稿 |
| `ENABLED` | 可用于 Web 会话或外呼 |
| `DISABLED` | 禁用 |
| `ARCHIVED` | 归档 |

## 13. 索引和唯一约束建议

不创建物理外键，但建议创建以下业务索引和唯一约束：

| 表 | 建议 |
|---|---|
| `ai_call_session` | 唯一约束：`call_id` |
| `ai_call_session` | 唯一约束：`request_id`，用于请求幂等 |
| `ai_call_session` | 普通索引：`status + create_time` |
| `ai_call_session` | 普通索引：`business_type + business_id` |
| `ai_call_participant` | 普通索引：`call_id` |
| `ai_call_event` | 普通索引：`call_id + event_time` |
| `ai_call_message` | 业务唯一约束：`call_id + seq` |
| `ai_call_recording` | 普通索引：`call_id + recording_type` |
| `ai_call_analysis` | 业务唯一约束：`call_id` |
| `ai_agent_config` | 后续创建时建议唯一约束：`agent_config_id` |
| `ai_model_config` | 后续创建时建议唯一约束：`model_config_id` |
| `ai_script_config` | 后续创建时建议唯一约束：`script_config_id` |

## 14. 与 sys_oss 的关系

`sys_oss` 继续作为基座文件存储能力使用。

当前基座 `sys_oss` 表为既有表，不由智能外呼模块创建。其结构如下：

| 字段 | 类型 | 说明 |
|---|---|---|
| `oss_id` | `bigint` | 对象存储主键，主键 |
| `tenant_id` | `varchar(20)` | 租户编码，默认 `000000` |
| `file_name` | `varchar(255)` | 文件名 |
| `original_name` | `varchar(255)` | 原名 |
| `file_suffix` | `varchar(10)` | 文件后缀名 |
| `url` | `varchar(500)` | URL 地址 |
| `ext1` | `varchar(500)` | 扩展字段 |
| `create_dept` | `bigint` | 创建部门 |
| `create_by` | `bigint` | 上传人 |
| `create_time` | `timestamp` | 创建时间 |
| `update_by` | `bigint` | 更新者 |
| `update_time` | `timestamp` | 更新时间 |
| `service` | `varchar(20)` | 服务商，默认 `minio` |

智能外呼模块不把录音文件的业务语义塞进 `sys_oss`，而是通过自己的录音表关联 `sys_oss.oss_id`：

```text
ai_call_recording.oss_id -> sys_oss.oss_id
```

关系不建物理外键，由代码校验。

使用约束：

1. `sys_oss` 只作为文件索引和基座存储入口。
2. 录音业务语义放在 `ai_call_recording` 中，通话后语义分析结果放在 `ai_call_analysis.analysis_result` 中。
3. 不依赖 `sys_oss.ext1` 保存 `call_id`、录音类型、质检状态等外呼业务字段。
4. `ai_call_recording.oss_id` 返回前端时按字符串处理，避免 JavaScript 精度丢失。
5. 上传录音文件前，需要确认基座存储链路允许 `.wav`、`.mp3` 等文件后缀和对应 MIME。

基座 OSS 复用方式：

1. 浏览器主动上传文件时，可以复用既有 `/system/oss/upload` multipart 接口。
2. 智能外呼录音是服务端生成文件，不走浏览器上传接口，应在后端任务中直接复用 `OssService.upload_service`。
3. `OssService.upload_service` 上传文件到 MinIO 后写入 `sys_oss`，返回 `oss_id`。
4. 外层业务事务可能回滚但文件证据仍需保留时，可以使用 `OssService.upload_committed_service`。
5. 当前基座 URL 查询能力是按 `oss_id` 返回 `sys_oss.url`；如果生产使用私有桶，需要补充短期签名 URL 或后端代理流能力。
6. 录音播放必须先通过智能外呼模块校验 `call_id` 访问权限，再根据 `ai_call_recording.oss_id` 查询文件地址。

## 15. 阶段落地原则

表结构可以一次性完整设计，但阶段实践时用到哪张表就创建和验证哪张表。

`ai_agent_config`、`ai_model_config`、`ai_script_config` 当前只作为后续配置模块草案保留。Phase 00 / Phase 01 不要求创建这三张表，也不要求按本节字段提前开发管理页面。

后续进入配置模块建设前，应重新评审这三张表的字段、枚举、索引和业务边界。如果实际实现发现字段需要拆分、合并或删除，应以当时的工程事实为准。

如果实践中发现字段不够或语义不准确，应修改对应表设计和阶段文档，不为了保持初版设计而牺牲工程事实。
