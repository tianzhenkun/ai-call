# Phase 00：Web 版商业闭环落地说明

最后更新：2026-06-09

## 0. 开工前输入

本文档是 Phase 00 的实施说明书。研发拿到本文档后，应能直接开始实现 Web 版智能外呼闭环；需要用户或运维提供的信息全部集中在本节。

### 0.1 必须已经具备

| 输入项 | 用途 | 当前处理方式 |
|---|---|---|
| PostgreSQL 连接 | 创建和读写智能外呼表、复用 `sys_oss` | 写入本地 `LingChenAiCallBase/env/.env.dev`，文档不写密码 |
| Redis 连接 | 基座启动、LiveKit 相关组件状态协调 | 写入本地 `.env.dev`，文档不写密码 |
| LiveKit Server URL | 浏览器 WebRTC 入会、Agent 入会 | 配置到 `LIVEKIT_URL` |
| LiveKit API key / secret | 后端签发 LiveKit token、创建 Room、调度 Agent、控制 Egress | 配置到 `LIVEKIT_API_KEY`、`LIVEKIT_API_SECRET` |
| 至少一套真实流式 ASR / LLM / TTS 配置 | Phase 00 最终验收必须验证真实低延迟语音对话链路 | 配置到 `.env.dev` 或服务端密钥配置中，不能写入仓库文档 |
| OSS 活跃配置 | 录音文件上传 MinIO 并写入 `sys_oss` | 数据库 `sys_oss_config` 必须存在 `status='0'` 的配置 |
| Web 调试入口 | 浏览器创建会话、入会、说话、结束、查看结果 | 可先由基座静态页或现有前端页面实现 |

### 0.2 本阶段默认值

| 配置 | Phase 00 固定口径 |
|---|---|
| `channel` | 固定为 `web_call` |
| 真实电话 | 不拨真实电话，不创建 SIP Participant |
| 被叫号码 | 不要求传入，`callee_number_cipher`、`callee_number_mask` 可为空 |
| SIP 线路 | 不使用 `ai_sip_trunk`，不读取 SIP 线路配置 |
| 音色 | 不暴露音色参数；如果供应商 API 要求必填音色，后端使用固定内置音色 |
| 录音 | 只做服务端混音录音，`recording_type=mixed` |
| 分轨录音 | 不做 |
| webhook | 不做 |
| 登录体系 | 不做完整登录、菜单、权限和租户切换；使用开发态免登录身份 |
| 物理外键 | 不创建 |
| 配置表 | 不创建 `ai_agent_config`、`ai_model_config`、`ai_script_config` |

### 0.3 不能用 mock 作为最终验收

工程早期可以用 mock ASR / LLM / TTS 打通接口、落库和页面流程，但 Phase 00 验收必须至少跑通一套真实流式级联 ASR / LLM / TTS 链路。

非流式 ASR / LLM / TTS 只能作为降级 adapter 或故障排查手段，不作为 Phase 00 最终主链路验收口径。

如果使用 mock，接口返回、页面日志、服务端日志必须显式出现：

```text
mock=true
```

否则容易把“工程链路已通”误判成“真实语音体验已达标”。

## 1. 阶段定位

Phase 00 的目标是建设一套不依赖真实 SIP 线路的 Web 版商业闭环。

它不是临时调试玩具，而是后续真实 SIP 外呼的核心能力底座。Phase 01 接入真实 SIP 时，理想状态只是把用户入口从浏览器 WebRTC 替换为 LiveKit SIP Participant：

```text
Phase 00:
浏览器 WebRTC 用户 -> LiveKit Room -> Agent -> 录音 -> 分析 -> 页面复盘

Phase 01:
真实电话 SIP 用户 -> LiveKit Room -> Agent -> 录音 -> 分析 -> 页面复盘
```

Room 内的 Agent、事件、消息、录音、语义分析、轻量转人工和查询展示能力应复用。

Phase 00 的语音主方案采用业内更常见、更适合业务审计的第一档语音助手形态：

```text
WebRTC 音频
  -> VAD / endpointing
  -> streaming ASR
  -> streaming LLM
  -> streaming TTS
  -> LiveKit 音频播放
```

这里的第一档不是“录完一句再识别、再生成、再合成”的全非流式批处理，而是低延迟流式级联管线。原生 realtime speech-to-speech 模型增长很快，但不作为 Phase 00 主链路，因为本阶段更看重 ASR 文本落库、业务规则可控、语义分析可复盘、转人工判断可解释，以及后续 SIP 入口复用。

本阶段使用数据表：

1. `ai_call_session`
2. `ai_call_participant`
3. `ai_call_event`
4. `ai_call_message`
5. `ai_call_recording`
6. `ai_call_analysis`
7. 既有 `sys_oss`

表字段、枚举和索引设计以 [../04-data-model.md](../04-data-model.md) 为准。Phase 00 实现时只创建上述六张智能外呼自有表，不创建后续配置草案表。

## 2. 第一性原理

智能外呼的核心风险不是“能不能拨出一个号码”，而是实时会话是否成立：

1. 用户音频能低延迟进入 Room。
2. Agent 能收到用户音频。
3. ASR / LLM / TTS 能以流式级联方式完成真实语音对话。
4. 打断、结束和转人工不会破坏会话状态。
5. 稳定事件、最终文本、录音和分析结果能落库。
6. 页面能按 `call_id` 复盘全过程。

真实 SIP 线路会额外带来白名单、端口、codec、RTP、失败码和运营商风控问题。先用浏览器模拟用户入口，可以把 Room 内核心能力提前做完整，后续接 SIP 时只验证电话入口差异。

## 3. 本阶段必须完成

1. 浏览器页面调用创建通话接口，后端固定写入 `channel=web_call`。
2. 后端生成 `call_id`、`request_id` 幂等记录、LiveKit Room 名称和浏览器入会 token。
3. 浏览器加入 LiveKit Room 并发布麦克风音频。
4. Agent 通过 explicit dispatch 或等价机制加入同一个 Room。
5. Agent 完成真实流式级联 ASR / LLM / TTS 语音对话。
6. `ai_call_session` 记录主状态。
7. `ai_call_participant` 记录 Web 用户、Agent、人工坐席。
8. `ai_call_event` 记录关键事件。
9. `ai_call_message` 记录用户 ASR final 和 Agent final text。
10. 用户打断 AI 播放时，记录 `barge_in` 事件，并在对应 Agent 消息上标记 `interrupted=1`。
11. 用户意图或 Web 端主动接管触发转人工时，区分 `transfer_source`。
12. 人工坐席通过 WebRTC 加入同一个 Room，并能看到接管前 `ai_call_message`。
13. 通话结束后生成服务端混音录音。
14. 录音上传到 OSS，写入 `sys_oss`，并在 `ai_call_recording` 关联 `oss_id`。
15. 通话结束后异步生成 JSON 语义分析，写入 `ai_call_analysis`。
16. 页面可按 `call_id` 查看状态、参与者、事件、消息、录音、分析和转人工结果。
17. 页面和事件能展示麦克风输入质量、远端音频播放状态、ASR 空结果原因和每轮耗时指标。
18. 每轮对话能记录从用户停顿到 Agent 首包音频播放的延迟归因，至少区分 endpoint、ASR、LLM、TTS 和播放阶段。

## 4. 本阶段不做

1. 不拨真实电话。
2. 不接真实 SIP trunk。
3. 不停止 FreeSWITCH。
4. 不做 SIP REFER 转人工。
5. 不做技能组、排队、抢单、坐席排班。
6. 不做完整呼叫中心坐席台。
7. 不做分轨录音。
8. 不做音色配置、音色选择、自定义音色、试听或音色训练。
9. 不做完整 Agent / 模型 / 话术配置后台。
10. 不接 RocketMQ。
11. 不做 webhook。
12. 不做大规模并发压测。
13. 不让浏览器连接 Redis、数据库、MinIO 密钥或 LiveKit API secret。
14. 不做完整登录页、菜单权限、租户切换和运营后台框架。
15. 不把原生 realtime speech-to-speech 模型作为 Phase 00 主链路；如后续评估引入，应作为独立方案验证。

## 5. 基座实现约定

### 5.1 项目位置

Phase 00 基于裁剪后的 FastAPI 基座实现：

```text
LingChenAiCallBase/
```

当前已存在模块入口：

```text
app/api/v1/ai_call/__init__.py
```

代码内路由前缀：

```text
/ai-call
```

环境中的对外前缀：

```text
ROOT_PATH=/ai-call-api/v1
```

实现和联调时统一按以下口径理解：

| 场景 | 路径口径 |
|---|---|
| 代码内 router prefix | `/ai-call` |
| 对外 API 基础路径 | `{API_BASE}/ai-call` |
| 默认 `API_BASE` | `/ai-call-api/v1` |
| 健康检查 | `{API_BASE}/ai-call/health` |

本地直连调试时，如果没有网关剥离 `ROOT_PATH`，也可以访问 `/ai-call/health`。业务文档、前端和接口联调用 `{API_BASE}/ai-call` 表达，避免把网关前缀写死到每个 router 中。

### 5.2 代码目录

Phase 00 按单模块实现，不拆成多个业务域：

```text
app/api/v1/ai_call/
  __init__.py
  controller.py
  schema.py
  model.py
  crud.py
  service.py
  table_name.py
  livekit_service.py
  agent_service.py
  recording_service.py
  analysis_service.py
```

文件职责：

| 文件 | 职责 |
|---|---|
| `__init__.py` | 创建并导出 `AiCallRouter`，注册 controller |
| `controller.py` | HTTP API，负责参数接收、依赖注入、响应包装 |
| `schema.py` | Pydantic 请求和响应模型，请求/响应使用 camelCase |
| `model.py` | SQLAlchemy ORM 模型，对应 Phase 00 六张表 |
| `crud.py` | 数据库读写，负责幂等查询、列表查询和状态更新 |
| `service.py` | 通话编排服务，串联创建、结束、转人工、查询 |
| `table_name.py` | 表名常量，避免字符串散落 |
| `livekit_service.py` | Room、token、participant、Egress 相关封装 |
| `agent_service.py` | Agent dispatch、Agent 状态和 Agent 事件回写 |
| `recording_service.py` | 录音创建、停止、上传 OSS、状态补偿 |
| `analysis_service.py` | 通话后语义分析任务、重试和结果写入 |

拆分原则：

1. `ai_call` 不放入原 `business` 业务域。
2. 不依赖原催收、诉讼、资产包、画像等业务模块。
3. 与外部业务只通过 `request_id`、`business_type`、`business_id`、`request_payload` 弱关联。
4. 文件存储只通过基座 `system/oss` 能力和 `sys_oss` 关联。
5. 实时链路不等待 OSS 上传、语义分析或页面查询。

### 5.3 响应格式

复用基座响应包装。

非分页接口使用：

```json
{
  "code": 200,
  "msg": "操作成功",
  "data": {}
}
```

分页或列表接口使用：

```json
{
  "total": 1,
  "rows": [],
  "code": 200,
  "msg": "查询成功"
}
```

前端字段使用 camelCase，数据库字段使用 snake_case。

示例：

| 数据库字段 | API 字段 |
|---|---|
| `call_id` | `callId` |
| `room_name` / `livekit_room_name` | `roomName` |
| `business_type` | `businessType` |
| `recording_status` | `recordingStatus` |

`bigint` 主键和业务 ID 返回前端时按字符串返回。

## 6. 配置和依赖

### 6.1 `.env.dev`

本地开发配置文件：

```text
LingChenAiCallBase/env/.env.dev
```

该文件已被 `.gitignore` 忽略，可以保存本地真实连接信息，但不能提交到仓库。

Phase 00 需要读取的关键配置：

| 变量 | 用途 |
|---|---|
| `SERVER_HOST` | 后端监听地址 |
| `SERVER_PORT` | 后端监听端口 |
| `ROOT_PATH` | 对外 API 前缀 |
| `DATABASE_TYPE` | 数据库类型，当前为 PostgreSQL |
| `DATABASE_HOST` | 数据库 host |
| `DATABASE_PORT` | 数据库端口 |
| `DATABASE_USER` | 数据库用户 |
| `DATABASE_PASSWORD` | 数据库密码 |
| `DATABASE_NAME` | 数据库实例 |
| `REDIS_ENABLE` | 是否启用 Redis |
| `REDIS_HOST` | Redis host |
| `REDIS_PORT` | Redis 端口 |
| `REDIS_PASSWORD` | Redis 密码 |
| `REDIS_DB_NAME` | Redis database |
| `LIVEKIT_URL` | 浏览器和后端连接 LiveKit 的 URL |
| `LIVEKIT_API_KEY` | LiveKit API key |
| `LIVEKIT_API_SECRET` | LiveKit API secret |
| `DASHSCOPE_API_KEY` | 阿里云百炼统一 API Key，Phase 00 默认由 ASR / LLM / TTS / 通话后分析共用 |
| `DASHSCOPE_REGION` | 阿里云百炼地域，默认 `cn-beijing` |
| `DASHSCOPE_BASE_URL` | 阿里云百炼 HTTP / OpenAI-compatible 接口地址 |
| `DASHSCOPE_WEBSOCKET_URL` | 阿里云百炼实时 ASR / TTS WebSocket 地址 |
| `ASR_PROVIDER` | ASR 供应商 |
| `ASR_MODEL` | ASR 模型名 |
| `ASR_API_KEY` | ASR 独立密钥；为空时使用 `DASHSCOPE_API_KEY` |
| `LLM_PROVIDER` | LLM 供应商 |
| `LLM_BASE_URL` | LLM OpenAI-compatible 地址或供应商地址 |
| `LLM_API_KEY` | LLM 独立密钥；为空时使用 `DASHSCOPE_API_KEY` |
| `LLM_MODEL` | LLM 模型名 |
| `TTS_PROVIDER` | TTS 供应商 |
| `TTS_MODEL` | TTS 模型名 |
| `TTS_API_KEY` | TTS 独立密钥；为空时使用 `DASHSCOPE_API_KEY` |
| `TTS_VOICE` | TTS 默认系统音色；Phase 00 不在页面暴露 |
| `POST_ANALYSIS_MODEL` | 通话后 JSON 语义分析模型名 |

文档中不写数据库密码、Redis 密码、LiveKit secret、模型 API key。

### 6.2 Phase 00 推荐模型组合

Phase 00 默认采用业内主流的 STT-LLM-TTS 级联管线，而不是 realtime speech-to-speech 模型作为第一版主链路。

本阶段必须按低延迟流式级联管线设计：

```text
Browser WebRTC
  -> LiveKit Room
  -> Agent Session
  -> streaming ASR / STT
  -> streaming LLM
  -> streaming TTS
  -> LiveKit audio track playout
```

取舍依据：

1. 业务外呼、客服、催收和坐席接管场景更需要稳定文本、可审计消息、可解释转人工和可复盘分析，因此第一档级联管线更适合作为 Phase 00 主链路。
2. realtime speech-to-speech 适合更自然的实时对话体验，但不利于本阶段优先沉淀 ASR final、业务规则、消息落库、语义分析和供应商可替换边界。
3. 级联管线不等于非流式批处理。Phase 00 主链路必须支持 streaming ASR、streaming LLM 和 streaming TTS；非流式只允许作为降级、排障或模型不可用时的临时兼容。
4. 创建 Room、Agent 入会、STT/TTS WebSocket 连接、模型配置读取应尽量在通话开始或开场白期间完成，不能等用户说完后才临时初始化。

推荐模型组合：

| 环节 | 推荐供应商 | 推荐模型 | 用途 |
|---|---|---|---|
| ASR / STT | 阿里云百炼 | `fun-asr-realtime` | 流式语音识别，输出 interim / final，并配合 endpointing 生成用户 ASR final |
| LLM | 阿里云百炼 | `qwen-plus` | 流式对话主链路默认模型，优先选择账号已确认可用的稳定模型 |
| TTS | 阿里云百炼 | `cosyvoice-v3-flash` | 流式语音合成，Phase 00 使用后端固定默认音色 |
| 通话后语义分析 | 阿里云百炼 | `qwen3.7-plus` | 通话结束后异步生成 JSON 语义分析 |

模型可用性边界：

1. `fun-asr-realtime`、`cosyvoice-v3-flash`、`qwen3.7-plus` 已按阿里云百炼公开文档做过可用性方向核对。
2. `qwen-plus` 作为当前主链路默认 LLM，因为已提供的配置明确指向该模型。
3. `qwen3.6-flash` 仍可作为低延迟候选模型，但开工前必须用实际 `DASHSCOPE_API_KEY` 做一次最小 API 调用验证，通过后再切换。

推荐 `.env.dev` 配置：

```text
DASHSCOPE_API_KEY="CHANGE_ME_DASHSCOPE_API_KEY"
DASHSCOPE_REGION="cn-beijing"
DASHSCOPE_BASE_URL="https://dashscope.aliyuncs.com/compatible-mode/v1"
DASHSCOPE_WEBSOCKET_URL="wss://dashscope.aliyuncs.com/api-ws/v1/inference"

ASR_PROVIDER="aliyun-bailian"
ASR_MODEL="fun-asr-realtime"
ASR_API_KEY=""

LLM_PROVIDER="aliyun-bailian"
LLM_BASE_URL="https://dashscope.aliyuncs.com/compatible-mode/v1"
LLM_MODEL="qwen-plus"
LLM_API_KEY=""

TTS_PROVIDER="aliyun-bailian"
TTS_MODEL="cosyvoice-v3-flash"
TTS_API_KEY=""
TTS_VOICE="longanyang"

POST_ANALYSIS_MODEL="qwen3.7-plus"
```

Phase 00 需要用户实际提供的信息：

1. 一个阿里云百炼 API Key，也就是 `DASHSCOPE_API_KEY`。
2. 确认该 API Key 所属账号和地域已经开通或实际可调用 `fun-asr-realtime`、`qwen-plus`、`cosyvoice-v3-flash`、`qwen3.7-plus`。
3. 如果账号使用的不是华北 2 北京地域，需要提供对应地域和接口地址，因为阿里云百炼不同地域的 API Key 和 URL 可能不同。
4. 如果 TTS API 实际要求必填音色，Phase 00 由后端配置一个默认系统音色，例如 `longanyang`；页面不做音色选择。

已提供配置的取舍：

| 配置 | Phase 00 是否使用 | 说明 |
|---|---|---|
| `DASHSCOPE_API_KEY` | 使用 | 百炼统一 API Key，默认供 ASR / LLM / TTS / 通话后分析共用 |
| `LLM_BASE_URL` | 使用 | 对应 `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| `LLM_MODEL=qwen-plus` | 使用 | `qwen-plus` 是当前更稳妥的主链路 LLM 默认值；如果后续实测 `qwen3.6-flash` 可用且延迟更优，再切换 |
| `ALIYUN_TTS_MODEL=cosyvoice-v3-flash` | 使用 | 对应 `TTS_MODEL` |
| `ALIYUN_TTS_VOICE=longanyang` | 使用 | 对应 `TTS_VOICE`，页面不暴露 |
| `ALIYUN_TTS_WS_URL` | 使用 | 对应 `DASHSCOPE_WEBSOCKET_URL` |
| `ALIYUN_NLS_APPKEY` | 默认不使用 | 属于阿里云智能语音交互 NLS 实时 ASR 方案，不是当前默认的百炼 `fun-asr-realtime` 方案 |
| `ALIYUN_NLS_TOKEN` | 默认不使用 | NLS token 通常有有效期，不适合作为生产长期配置；如果改走 NLS，需要设计 token 刷新机制 |
| `ALIYUN_NLS_URL` | 默认不使用 | 只有切换到 NLS ASR adapter 时才需要 |

Phase 00 不需要前端传 ASR / LLM / TTS 配置。创建通话接口默认读取服务端配置。

选型原则：

1. 实时主链路优先选择低延迟、流式能力稳定、成本可控的模型。
2. 通话后语义分析不进入实时链路，可以使用能力更强但稍慢的模型。
3. Phase 00 不做音色配置后台和页面选择，但允许服务端写死一个供应商要求的默认音色。
4. 模型 API key 只放 `.env`、服务器环境变量或受控密钥管理，不写入仓库文档。
5. 如果实际账号未开通上述模型，可使用同供应商等价实时模型替代，但必须在阶段执行记录中写明替代模型和原因。
6. 开始正式联调前，必须完成 ASR、LLM、TTS 和通话后分析的最小 smoke test，避免到通话链路里才发现模型名、地域或权限不可用。

### 6.2.1 低延迟目标

Phase 00 的延迟目标按“用户停止说话后，到浏览器听到 Agent 第一段声音”为准。

建议门槛：

| 指标 | 目标 |
|---|---|
| 首段回复延迟 P50 | 800ms - 1200ms |
| 首段回复延迟 P95 | 1800ms - 2500ms |
| 打断停止播放 P50 | 300ms - 700ms |
| ASR endpoint delay | 默认 300ms - 500ms，可按业务话术调优 |
| LLM TTFT | 每轮必须记录，超过阈值要能定位 |
| TTS TTFB | 每轮必须记录，超过阈值要能定位 |

说明：

1. 这些指标是 Phase 00 低延迟方向目标，不等同于生产 SLA。
2. 不建议为了极限低延迟把 endpointing 调得过激，否则 Agent 容易抢话，催收、客服等业务场景体验会变差。
3. 如果模型账号、网络或供应商能力暂时达不到目标，必须在验收记录中说明耗时瓶颈，而不是只写“回复慢”。

### 6.2.2 Turn-taking 和 endpointing

Phase 00 不能只靠固定 RMS 阈值判断用户是否说完。推荐组合：

1. 浏览器侧启用 `echoCancellation`、`noiseSuppression`、`autoGainControl`，但最终判断以服务端收到的音频质量为准。
2. Agent 侧维护 VAD，用于识别 `user_speech_started`、`user_speech_ended` 和打断。
3. ASR 侧使用 streaming interim / final / endpoint 结果，最终文本以稳定 final 为准。
4. endpointing 默认按 300ms - 500ms 静音窗口起步；短指令可更激进，长句业务话术应更保守。
5. 低音量、纯噪声、ASR 空文本必须产生可解释事件，不能表现成“Agent 没回复”。
6. 用户轻微应答，例如“嗯”“对”“好”，是否视为打断，要结合当前 Agent 播放状态和文本长度判断，避免误停播。

### 6.2.3 LLM 和 TTS 流式播放

LLM 和 TTS 的低延迟策略：

1. LLM 必须使用流式输出，记录 `llm_stream_started`、`llm_first_token`、`llm_finished`。
2. Prompt 和上下文必须短而稳定，避免每轮塞入大段无关历史。
3. LLM 输出达到自然短句、标点或语义完整片段后，立即送入 TTS。
4. TTS 必须优先使用 WebSocket / streaming 合成，记录 `tts_stream_started`、`tts_first_audio`、`tts_finished`。
5. TTS chunk 不宜过短，避免听感机械；也不宜等完整回复，避免首包过慢。
6. 用户打断时，必须取消当前播放、未完成 TTS chunk 和未完成 LLM 生成。

### 6.2.4 行业资料参考

本阶段方案参考以下公开资料形成工程口径：

1. LiveKit Agents voice pipeline / turn-taking / latency metrics 文档。
2. OpenAI Realtime 文档中对 browser WebRTC 和 chained voice pipeline 的边界说明。
3. Deepgram endpointing / interim results 文档中对流式 ASR 端点检测的说明。
4. ElevenLabs streaming TTS latency optimization 文档中对 WebSocket TTS 首包延迟的建议。

这些资料只作为方案依据。具体模型、地域、账号权限和延迟表现必须以本项目测试环境实测为准。

### 6.3 Redis

当前测试 Redis host：

```text
118.89.137.44:6379
```

Redis 只给服务端和 LiveKit 相关组件使用，浏览器不能直连 Redis。

### 6.4 数据库

当前测试数据库：

```text
PostgreSQL
118.89.137.44:15432
database: recov
schema: public
```

Phase 00 建表只创建智能外呼自有表，不修改 `sys_oss` 结构。

### 6.5 OSS

基座已保留 OSS 能力：

| 能力 | 现有入口 |
|---|---|
| 浏览器上传 | `POST /system/oss/upload` |
| 服务端上传 | `OssService.upload_service(...)` |
| 独立事务上传 | `OssService.upload_committed_service(...)` |
| 按 `oss_id` 查 URL | `GET /system/oss/url/{oss_id}` 或 `OssService.get_url_by_oss_id_service(...)` |

智能外呼录音是服务端生成文件，不走浏览器上传接口。

### 6.6 LiveKit

Phase 00 使用 LiveKit Server 的 WebRTC 能力，不使用 LiveKit SIP。

至少需要：

1. 后端可用 LiveKit API key / secret 签发 token。
2. 浏览器能访问 `LIVEKIT_URL`。
3. Agent Worker 能加入指定 Room。
4. Egress Worker 或等价服务端录音能力可用。

`LIVEKIT_API_KEY` 和 `LIVEKIT_API_SECRET` 必须与 LiveKit Server 配置中的 API key / secret 完全一致。本地 `.env.dev` 中生成或填写后，还需要在 LiveKit Server 配置中使用同一组值，否则浏览器拿到的入会 token 无法通过 LiveKit 校验。

生产环境必须使用 HTTPS / WSS。测试环境可用 HTTP / WS，但不能作为生产结论。

### 6.7 实施授权和操作边界

Phase 00 实施时，允许实施者在用户已提供连接信息并明确授权的开发或测试环境中直接执行必要的数据库和服务器操作，目标是减少无意义等待，让阶段实现可以闭环推进。

允许直接执行：

1. 在测试数据库中创建、修改和查询 Phase 00 自有表：`ai_call_session`、`ai_call_participant`、`ai_call_event`、`ai_call_message`、`ai_call_recording`、`ai_call_analysis`。
2. 执行 Phase 00 迁移 SQL、索引、唯一约束和测试数据写入。
3. 启动、停止和重启 `LingChenAiCallBase` 本地或测试环境服务。
4. 查询 PostgreSQL、Redis、LiveKit、Agent Worker、Egress、OSS 相关日志和运行状态。
5. 调用健康检查、创建会话、入会 token、结束通话、录音查询、分析查询等接口完成验收。
6. 在测试环境中上传录音文件到 OSS，并写入 `sys_oss` 文件索引。

必须先向用户确认：

1. 修改 `sys_oss`、`sys_oss_config` 等既有基座表结构。
2. 删除、截断或覆盖已有数据。
3. 在生产库、共享业务库或无法确认用途的数据库中执行 DDL。
4. 停止 FreeSWITCH、网关、生产 Worker 或其他非 Phase 00 专属服务。
5. 修改服务器安全组、防火墙、公网 IP、SIP 端口、LiveKit SIP 配置。
6. 发起真实 SIP 外呼、批量外呼或压测。
7. 把测试配置提升为生产配置。

执行记录要求：

1. 每次阶段性数据库变更都要记录涉及的库、schema、表和 SQL 摘要。
2. 非空表执行破坏性变更前必须先备份或说明为什么不需要备份。
3. 每次服务器操作都要记录目标服务器、服务名、操作动作和结果。
4. 所有执行记录不得包含数据库密码、Redis 密码、LiveKit secret、模型 API key 或完整 token。
5. 阶段验收时必须沉淀可复查的 `call_id`、日志片段、查询结果或截图。

## 7. 数据库落地规则

### 7.1 建表范围

Phase 00 只创建：

```text
ai_call_session
ai_call_participant
ai_call_event
ai_call_message
ai_call_recording
ai_call_analysis
```

不创建：

```text
ai_sip_trunk
ai_agent_config
ai_model_config
ai_script_config
ai_voice_config
```

### 7.2 ID 生成规则

使用基座统一雪花 ID 工具生成主键和业务 ID。

业务 ID 建议格式：

| 字段 | 格式 |
|---|---|
| `call_id` | `CALL` + 雪花 ID |
| `participant_id` | `PART` + 雪花 ID |
| `event_id` | `EVT` + 雪花 ID |
| `message_id` | `MSG` + 雪花 ID |
| `recording_id` | `REC` + 雪花 ID |

`request_id` 由调用方生成并传入。Web 页面使用 `crypto.randomUUID()` 生成。相同请求重试必须复用同一个 `request_id`。

### 7.3 幂等规则

`request_id` 建业务唯一约束。

`POST {API_BASE}/ai-call/calls` 收到重复 `request_id` 时：

| 场景 | 处理 |
|---|---|
| 请求参数与已有记录一致 | 返回已有 `call_id`，不重复创建 Room，不重复调度 Agent |
| 请求参数与已有记录不一致 | 返回业务错误，提示 `request_id` 已被不同请求使用 |
| 已有通话已结束 | 仍返回已有记录；业务重试应使用新的 `request_id` |

同一个 `business_type + business_id` 可以有多通电话或多次 Web 会话，不做唯一约束。

### 7.4 配置快照

Phase 00 不创建配置表，创建通话接口也不接收 `agentConfig`、`modelConfig`、`script`、`variables` 等高级配置参数。

本阶段必须保存本次实际执行快照，但快照来源是服务端默认值和 `.env.dev`，不是前端入参：

| 字段 | 内容 |
|---|---|
| `request_payload` | 本次请求快照，JSON 字符串，过滤密钥和 token |
| `agent_config_snapshot` | Agent 行为参数、开场白、最大通话时长、打断和转人工开关 |
| `model_config_snapshot` | ASR / LLM / TTS 的 provider、model、timeout、stream 配置，不写密钥 |
| `script_snapshot` | system prompt、开场白和默认话术，不写密钥 |

如果后续需要由业务侧传入 Agent、模型、话术或变量，应放到 Phase 02 配置体系中重新设计，不在 Phase 00 接口中提前开放。

默认 Agent 行为：

```json
{
  "agentName": "ai-call-web-agent",
  "maxCallSeconds": 600,
  "silenceTimeoutMs": 10000,
  "bargeInEnabled": true,
  "humanTransferEnabled": true
}
```

默认话术：

```json
{
  "openingText": "您好，我是智能语音助手，请问现在方便沟通吗？",
  "systemPrompt": "你是一个语音通话助手，回答要简洁、自然、适合口语播报。"
}
```

## 8. API 契约

### 8.1 接口清单

Phase 00 必须实现以下接口：

```text
POST {API_BASE}/ai-call/calls
GET  {API_BASE}/ai-call/calls/{call_id}
GET  {API_BASE}/ai-call/calls?businessType={businessType}&businessId={businessId}
POST {API_BASE}/ai-call/calls/{call_id}/end
POST {API_BASE}/ai-call/calls/{call_id}/cancel
GET  {API_BASE}/ai-call/calls/{call_id}/messages
GET  {API_BASE}/ai-call/calls/{call_id}/events
GET  {API_BASE}/ai-call/calls/{call_id}/participants
GET  {API_BASE}/ai-call/calls/{call_id}/recordings
GET  {API_BASE}/ai-call/calls/{call_id}/recordings/{recording_id}/play-url
GET  {API_BASE}/ai-call/calls/{call_id}/analysis
POST {API_BASE}/ai-call/calls/{call_id}/human-transfer
POST {API_BASE}/ai-call/calls/{call_id}/web-token
POST {API_BASE}/ai-call/calls/{call_id}/human-token
```

`/api/ai-call/web-sessions` 不作为正式接口。早期如果需要兼容，只能内部转发到 `POST {API_BASE}/ai-call/calls`。

### 8.2 创建通话

```text
POST {API_BASE}/ai-call/calls
```

请求体：

```json
{
  "requestId": "d9cfe95a-785a-4f4c-9b48-fbb0b2e3d6d5",
  "businessType": "web_phase00",
  "businessId": "demo-001"
}
```

前端页面默认不需要让使用者填写上述字段：

1. `requestId` 由浏览器 `crypto.randomUUID()` 自动生成。
2. `businessType` 默认 `web_phase00`。
3. `businessId` 可以由页面自动生成测试编号，也可以允许测试人员手动填一个业务编号。

请求规则：

| 字段 | 是否必填 | 规则 |
|---|---:|---|
| `requestId` | 是 | 调用方幂等 ID，重复提交必须相同 |
| `businessType` | 否 | 为空时后端使用 `web_phase00` |
| `businessId` | 否 | 为空时后端使用 `requestId` |

Phase 00 创建接口固定按以下规则处理：

1. `channel` 由后端固定写入 `web_call`，前端不传。
2. Agent 行为配置由后端默认值生成，并写入 `agent_config_snapshot`。
3. ASR / LLM / TTS 配置由 `.env.dev` 解析，并写入 `model_config_snapshot`。
4. 话术由后端默认话术生成，并写入 `script_snapshot`。
5. `agentConfig`、`modelConfig`、`script`、`variables` 不作为 Phase 00 创建接口参数。
6. 如果请求体携带这些高级字段，后端应返回参数错误或忽略并记录警告；推荐返回参数错误，避免调用方误以为已生效。

成功返回：

```json
{
  "code": 200,
  "msg": "创建通话成功",
  "data": {
    "callId": "CALL123456789",
    "requestId": "d9cfe95a-785a-4f4c-9b48-fbb0b2e3d6d5",
    "channel": "web_call",
    "status": "CREATED",
    "roomName": "ai_call_CALL123456789",
    "livekitUrl": "ws://111.229.146.182:7880",
    "webUserToken": "短期 LiveKit JWT",
    "webUserIdentity": "web_user_CALL123456789",
    "expiresIn": 600,
    "agentDispatch": {
      "agentName": "ai-call-web-agent",
      "dispatchStatus": "REQUESTED"
    }
  }
}
```

落库要求：

1. 创建 `ai_call_session`，`channel=web_call`，`status=CREATED`。
2. `livekit_room_name=ai_call_{call_id}`。
3. `sip_call_id`、`provider_call_id`、`livekit_sip_call_id` 为空。
4. 保存 `request_payload` 和三个配置快照。
5. 写入 `call_created` 事件。
6. 创建或准备 Web 用户参与者记录，`participant_type=web_user`。
7. 签发 Web 用户 token。
8. 调度 Agent 加入 Room，写入 `agent_dispatch_started` 事件。

### 8.3 查询通话详情

```text
GET {API_BASE}/ai-call/calls/{call_id}
```

返回：

```json
{
  "code": 200,
  "msg": "查询成功",
  "data": {
    "callId": "CALL123456789",
    "requestId": "d9cfe95a-785a-4f4c-9b48-fbb0b2e3d6d5",
    "channel": "web_call",
    "status": "IN_PROGRESS",
    "businessType": "web_phase00",
    "businessId": "demo-001",
    "roomName": "ai_call_CALL123456789",
    "createdAt": "2026-06-09T10:00:00",
    "answeredAt": "2026-06-09T10:00:05",
    "endedAt": null,
    "failCode": null,
    "failReason": null,
    "hangupReason": null
  }
}
```

### 8.4 按业务对象查询

```text
GET {API_BASE}/ai-call/calls?businessType=web_phase00&businessId=demo-001
```

返回同一业务对象关联的通话列表。按 `create_time desc` 排序。

### 8.5 结束通话

```text
POST {API_BASE}/ai-call/calls/{call_id}/end
```

请求体：

```json
{
  "endReason": "user_click",
  "force": false
}
```

`endReason`：

| 值 | 场景 |
|---|---|
| `user_click` | Web 用户点击结束 |
| `browser_disconnect` | 浏览器断开，后端判定结束 |
| `agent_finished` | Agent 达成通话目标后结束 |
| `human_finished` | 人工坐席结束接管 |
| `system_timeout` | 达到最大通话时长或静音超时 |
| `admin_stop` | 运营或管理员强制结束 |

处理要求：

1. 重复结束必须幂等。
2. 已是 `COMPLETED`、`FAILED`、`CANCELED`、`TIMEOUT` 时，直接返回当前状态。
3. 正常结束更新 `status=COMPLETED`、`ended_at`、`hangup_reason`。
4. 写入 `call_completed` 事件。
5. 停止或收敛 Agent。
6. 触发录音后处理任务。
7. 创建或更新 `ai_call_analysis.analysis_status=PENDING`，触发语义分析任务。

### 8.6 取消通话

```text
POST {API_BASE}/ai-call/calls/{call_id}/cancel
```

请求体：

```json
{
  "cancelReason": "user_cancel"
}
```

`cancelReason`：

| 值 | 场景 |
|---|---|
| `user_cancel` | 用户创建后立即取消 |
| `duplicate_request` | 前端或业务侧判断重复请求后取消 |
| `test_abort` | 测试过程中主动终止 |

取消只允许发生在未进入有效通话前。已经 `IN_PROGRESS` 的会话应调用 `end`。

### 8.7 消息列表

```text
GET {API_BASE}/ai-call/calls/{call_id}/messages
```

返回：

```json
{
  "total": 2,
  "rows": [
    {
      "messageId": "MSG1",
      "callId": "CALL123456789",
      "speakerType": "user",
      "messageType": "asr_final",
      "seq": "1",
      "contentText": "我要转人工",
      "interrupted": 0,
      "interruptReason": null,
      "createTime": "2026-06-09T10:00:10"
    },
    {
      "messageId": "MSG2",
      "callId": "CALL123456789",
      "speakerType": "agent",
      "messageType": "agent_text",
      "seq": "2",
      "contentText": "好的，我马上为您转接人工。",
      "interrupted": 0,
      "interruptReason": null,
      "createTime": "2026-06-09T10:00:11"
    }
  ],
  "code": 200,
  "msg": "查询成功"
}
```

本表只保存稳定文本：

1. 用户 ASR final。
2. Agent final text。
3. AI 回复是否被打断。

不保存 ASR partial，也不把每个 token 都写库。ASR partial、LLM token、TTS chunk 只用于实时链路和指标归因；如需排障，可在 `ai_call_event.event_payload` 中保存聚合指标、阶段耗时、chunk 数量和错误原因，不写入 `ai_call_message`。

### 8.8 事件列表

```text
GET {API_BASE}/ai-call/calls/{call_id}/events
```

事件按 `event_time asc` 返回。`event_payload` 是 JSON 字符串解析后的对象或原始字符串，前端可展示。

### 8.9 参与者列表

```text
GET {API_BASE}/ai-call/calls/{call_id}/participants
```

参与者类型：

| 值 | 场景 |
|---|---|
| `web_user` | 浏览器用户 |
| `agent` | AI Agent |
| `human` | 人工坐席 |

Phase 00 不出现 `sip_user`。

### 8.10 录音列表

```text
GET {API_BASE}/ai-call/calls/{call_id}/recordings
```

返回：

```json
{
  "total": 1,
  "rows": [
    {
      "recordingId": "REC123",
      "callId": "CALL123456789",
      "recordingType": "mixed",
      "recordingStatus": "COMPLETED",
      "ossId": "1900000000000000000",
      "durationSeconds": 86,
      "failReason": null
    }
  ],
  "code": 200,
  "msg": "查询成功"
}
```

### 8.11 录音播放地址

```text
GET {API_BASE}/ai-call/calls/{call_id}/recordings/{recording_id}/play-url
```

返回：

```json
{
  "code": 200,
  "msg": "查询成功",
  "data": {
    "recordingId": "REC123",
    "recordingType": "mixed",
    "url": "后端鉴权后返回的播放地址",
    "urlType": "sys_oss_url",
    "expiresIn": null,
    "contentType": "audio/wav"
  }
}
```

安全要求：

1. 先校验当前用户或调用方是否有权限访问该 `call_id`。
2. 再根据 `ai_call_recording.oss_id` 查询 `sys_oss.url`。
3. 不能让前端只凭 `oss_id` 绕过通话权限校验。
4. 当前基座返回 `sys_oss.url`，不等同于短期预签名 URL。
5. 如果生产 bucket 是 private，必须补短期预签名 URL 或后端代理流。

### 8.12 语义分析结果

```text
GET {API_BASE}/ai-call/calls/{call_id}/analysis
```

返回：

```json
{
  "code": 200,
  "msg": "查询成功",
  "data": {
    "callId": "CALL123456789",
    "analysisStatus": "COMPLETED",
    "analysisResult": {
      "summary": "用户要求转人工，已接入人工坐席。",
      "callResult": "transferred",
      "userIntent": "manual_service",
      "transferRequested": true,
      "transferSource": "user_intent",
      "tags": ["转人工"],
      "nextAction": "人工跟进",
      "evidenceMessageIds": ["MSG1", "MSG2"]
    },
    "generatedAt": "2026-06-09T10:02:00",
    "failReason": null
  }
}
```

### 8.13 触发转人工

Phase 00 支持两种转人工触发方式：

| 方式 | `transferSource` | 触发方 | 场景 |
|---|---|---|---|
| 用户意图触发 | `user_intent` | Agent 根据用户 ASR final 或 LLM 判断触发 | 用户说“转人工”“找客服”“人工服务”等 |
| Web 端主动接管 | `manual_console` | 页面按钮触发 | 测试人员或坐席看到对话内容后，主动点击人工接管 |

两种方式进入同一套后续流程：写入转人工事件、更新通话状态、签发人工入会 token、人工加入同一个 LiveKit Room、AI 静音或退出。

```text
POST {API_BASE}/ai-call/calls/{call_id}/human-transfer
```

请求体：

```json
{
  "transferSource": "manual_console",
  "reason": "Web 页面主动接管"
}
```

`transferSource`：

Phase 00 必须落地 `user_intent` 和 `manual_console`。`agent_decision`、`system_rule` 是预留来源，只有实现对应触发逻辑时才写入，不作为 Phase 00 最小通过标准。

| 值 | 场景 |
|---|---|
| `user_intent` | 用户明确说“转人工”“找客服”“人工服务”等 |
| `agent_decision` | AI 根据上下文判断需要转人工 |
| `system_rule` | 系统规则触发，例如连续识别失败、异常兜底 |
| `manual_console` | 运营台或坐席台人工点击接管 |

处理要求：

1. 写入 `human_transfer_requested` 事件。
2. `event_payload.transfer_source` 必须写入具体来源。
3. 更新 `ai_call_session.status=HUMAN_TRANSFER_REQUESTED`。
4. 不在该接口返回 LiveKit API secret。
5. 人工真正入会由 `human-token` 接口签发 token。
6. 如果来源是 `manual_console`，页面应立即进入待人工入会状态，并提示点击或自动调用 `human-token`。

如果用户在 AI 播放过程中说“转人工”，必须同时记录：

1. `barge_in`：表示 AI 播放被打断。
2. `human_transfer_requested`：表示转人工业务状态被触发。

### 8.14 Web 用户 token

```text
POST {API_BASE}/ai-call/calls/{call_id}/web-token
```

使用场景：

1. 创建通话接口没有直接返回 `webUserToken`。
2. 浏览器刷新后重新加入同一个 Room。
3. LiveKit token 过期后重新签发。

请求体：

```json
{
  "displayName": "Web 用户",
  "forceRefresh": false
}
```

返回：

```json
{
  "code": 200,
  "msg": "签发成功",
  "data": {
    "callId": "CALL123456789",
    "channel": "web_call",
    "roomName": "ai_call_CALL123456789",
    "livekitUrl": "ws://111.229.146.182:7880",
    "participantType": "web_user",
    "participantIdentity": "web_user_CALL123456789",
    "token": "短期 LiveKit JWT",
    "expiresIn": 600
  }
}
```

### 8.15 人工坐席 token

```text
POST {API_BASE}/ai-call/calls/{call_id}/human-token
```

请求体：

```json
{
  "displayName": "坐席名称"
}
```

`operator_id` 不从请求体传入。Phase 00 不做登录逻辑，后端使用固定开发态人工身份或按 `call_id` 生成开发态人工身份，例如 `human_dev_{call_id}`。

返回：

```json
{
  "code": 200,
  "msg": "签发成功",
  "data": {
    "callId": "CALL123456789",
    "channel": "web_call",
    "roomName": "ai_call_CALL123456789",
    "livekitUrl": "ws://111.229.146.182:7880",
    "participantType": "human",
    "participantIdentity": "human_{operatorId}_CALL123456789",
    "token": "短期 LiveKit JWT",
    "expiresIn": 600
  }
}
```

## 9. 状态机

Phase 00 使用以下状态：

| 状态 | 进入场景 |
|---|---|
| `CREATED` | 通话记录已创建，Room 和 token 已准备 |
| `IN_PROGRESS` | Web 用户或 Agent 已加入 Room，通话进入有效进行 |
| `HUMAN_TRANSFER_REQUESTED` | 已触发转人工，等待人工加入 |
| `HUMAN_TRANSFER_CONNECTED` | 人工坐席已加入 Room 并接管 |
| `COMPLETED` | 通话正常结束 |
| `FAILED` | 通话失败，例如 Agent 启动失败、LiveKit 操作失败 |
| `CANCELED` | 进入通话前被取消 |
| `TIMEOUT` | 最大通话时长、静音或等待人工超时 |

Phase 00 不使用：

```text
QUEUED
DIALING
RINGING
ANSWERED
```

这些状态留给真实 SIP 阶段。

状态流转：

```text
CREATED
  -> IN_PROGRESS
  -> COMPLETED

CREATED
  -> CANCELED

CREATED / IN_PROGRESS
  -> FAILED

IN_PROGRESS
  -> HUMAN_TRANSFER_REQUESTED
  -> HUMAN_TRANSFER_CONNECTED
  -> COMPLETED

IN_PROGRESS / HUMAN_TRANSFER_REQUESTED
  -> TIMEOUT
```

## 10. 事件清单

Phase 00 必须记录以下事件：

| 事件 | 触发时机 | 级别 |
|---|---|---|
| `call_created` | 创建通话记录成功 | `INFO` |
| `room_created` | LiveKit Room 准备完成 | `INFO` |
| `web_token_issued` | 签发 Web 用户 token | `INFO` |
| `web_user_joined` | 浏览器用户加入 Room | `INFO` |
| `web_user_left` | 浏览器用户离开 Room | `INFO` |
| `agent_dispatch_started` | 开始调度 Agent | `INFO` |
| `agent_joined` | Agent 加入 Room | `INFO` |
| `agent_left` | Agent 离开 Room | `INFO` |
| `web_audio_published` | 浏览器发布麦克风音频轨道 | `INFO` |
| `agent_audio_published` | Agent 发布音频轨道 | `INFO` |
| `client_remote_audio_subscribed` | 浏览器订阅到 Agent 远端音频 | `INFO` |
| `client_remote_audio_play_started` | 浏览器远端音频开始播放 | `INFO` |
| `client_remote_audio_play_failed` | 浏览器远端音频播放失败 | `WARN` |
| `mic_quality_sample` | 麦克风输入质量采样，需限频 | `INFO` |
| `user_speech_started` | VAD 判断用户开始说话 | `INFO` |
| `user_speech_ended` | VAD / endpointing 判断用户一句话结束 | `INFO` |
| `asr_stream_started` | 流式 ASR 会话启动 | `INFO` |
| `asr_interim_received` | 收到 ASR interim，需聚合或限频 | `INFO` |
| `asr_final` | 用户一句话最终识别完成 | `INFO` |
| `asr_empty` | ASR 未识别到有效文本 | `WARN` |
| `llm_stream_started` | LLM 流式生成开始 | `INFO` |
| `llm_first_token` | LLM 首 token 返回 | `INFO` |
| `llm_finished` | LLM 生成完成 | `INFO` |
| `agent_text_created` | Agent 回复文本生成 | `INFO` |
| `tts_stream_started` | TTS 流式合成开始 | `INFO` |
| `tts_first_audio` | TTS 首包音频返回 | `INFO` |
| `tts_started` | TTS 开始播放 | `INFO` |
| `tts_finished` | TTS 播放完成 | `INFO` |
| `barge_in` | 用户说话打断 AI 播放 | `INFO` |
| `turn_metrics` | 单轮端到端耗时归因 | `INFO` |
| `human_transfer_requested` | 触发转人工 | `INFO` |
| `human_joined` | 人工坐席加入 Room | `INFO` |
| `agent_muted` | 转人工后 AI 静音 | `INFO` |
| `recording_started` | 混音录音开始 | `INFO` |
| `recording_finished` | 混音录音完成 | `INFO` |
| `recording_failed` | 录音失败 | `ERROR` |
| `analysis_generated` | 语义分析完成 | `INFO` |
| `analysis_failed` | 语义分析失败 | `ERROR` |
| `call_completed` | 通话正常结束 | `INFO` |
| `call_failed` | 通话失败 | `ERROR` |
| `call_canceled` | 通话取消 | `INFO` |
| `call_timeout` | 通话超时 | `WARN` |

`event_payload` 只保存排障所需业务信息，不保存密钥、token、完整录音地址和模型 API key。

高频事件处理规则：

1. `mic_quality_sample`、`asr_interim_received` 不能按每个音频帧或每个 token 写库，应按时间窗口聚合或只保留关键样本。
2. 每轮对话必须至少有一个 `turn_metrics` 事件，记录 endpoint、ASR、LLM、TTS 和播放阶段耗时。
3. 如果浏览器播放失败，必须写 `client_remote_audio_play_failed`，避免把播放问题误判成 Agent 未回复。
4. 如果 ASR final 为空，必须写 `asr_empty`，并包含音频时长、输入能量摘要和 endpoint 原因。

## 11. Agent 实现要求

### 11.1 Agent 入会

创建通话后，后端必须调度 Agent 加入同一个 Room。

Agent dispatch metadata 必须包含：

```json
{
  "callId": "CALL123456789",
  "roomName": "ai_call_CALL123456789",
  "channel": "web_call",
  "agentConfigSnapshot": {},
  "modelConfigSnapshot": {},
  "scriptSnapshot": {}
}
```

Agent 日志必须打印：

```text
call_id
room_name
agent_job_id
agent_name
```

### 11.2 对话能力

Agent 必须完成：

1. 播放开场白。
2. 接收 Web 用户音频。
3. 通过 streaming ASR 生成用户 ASR final。
4. 通过 streaming LLM 生成回复文本。
5. 通过 streaming TTS 播放回复音频。
6. 记录用户 `asr_final` 消息。
7. 记录 Agent `agent_text` 消息。
8. 识别或规则触发转人工。
9. 支持用户说话打断 AI 播放。
10. 记录每轮耗时指标和 ASR 空结果原因。

实时链路禁止等待录音上传、语义分析、OSS 查询或页面请求。

主链路禁止按“整段录音完成 -> 批量 ASR -> 完整 LLM -> 完整 TTS -> 播放”的全非流式批处理方式实现。非流式只允许作为降级 adapter，并且必须在事件和配置快照中标明 `streamEnabled=false` 和降级原因。

### 11.3 打断记录

用户打断 AI 播放时：

1. 写入 `ai_call_event.event_type=barge_in`。
2. 找到正在播放或最近一条 Agent 消息。
3. 更新该消息 `interrupted=1`。
4. 尽量写入 `played_duration_ms`。
5. `interrupt_reason=user_speech`。
6. 立即停止当前 Agent 音频播放。
7. 取消尚未播放的 TTS chunk。
8. 取消或收敛当前 LLM 生成任务。

如果无法准确知道 AI 实际播放了多少字，不要求保存“AI 实际播放文本”。本阶段只要求能标识被打断。

### 11.4 单轮耗时指标

每一轮用户发言到 Agent 首段回复，必须生成 `turn_metrics` 事件。

建议 payload：

```json
{
  "turnId": "TURN123",
  "speechStartMs": 1200,
  "speechEndMs": 3180,
  "endpointDelayMs": 420,
  "asrFinalLatencyMs": 180,
  "llmTtftMs": 360,
  "ttsTtfbMs": 280,
  "firstAudioPlayedMs": 980,
  "e2eFirstAudioLatencyMs": 1400,
  "asrTextLength": 12,
  "agentTextLength": 24,
  "recognitionMode": "streaming",
  "ttsMode": "streaming"
}
```

指标口径：

1. `endpointDelayMs`：用户实际停止说话到系统判定一句话结束。
2. `asrFinalLatencyMs`：endpoint 后到 ASR final 可用。
3. `llmTtftMs`：LLM 请求开始到首 token。
4. `ttsTtfbMs`：TTS 请求开始到首包音频。
5. `firstAudioPlayedMs`：首包音频到浏览器或 LiveKit 播放层确认播放。
6. `e2eFirstAudioLatencyMs`：用户停止说话到听到 Agent 第一段声音。

## 12. 录音和 OSS

### 12.1 录音策略

Phase 00 必须做服务端混音录音。

不接受浏览器 `MediaRecorder` 作为最终验收录音，因为它无法可靠覆盖 Agent、人工坐席和服务端审计链路。

录音流程：

```text
Web 用户加入 Room
  -> 启动服务端混音录音
  -> ai_call_recording.recording_status=RECORDING
  -> 通话结束
  -> 停止 Egress 或等价录音任务
  -> 获取录音文件 bytes
  -> OssService.upload_service(...)
  -> sys_oss 写入文件索引
  -> ai_call_recording.oss_id=返回值
  -> ai_call_recording.recording_status=COMPLETED
```

录音文件命名：

```text
ai-call-{call_id}-mixed.wav
```

如果实际输出为 mp3：

```text
ai-call-{call_id}-mixed.mp3
```

### 12.2 失败处理

录音失败不能影响通话主流程结束。

失败时：

1. `ai_call_recording.recording_status=FAILED`。
2. `fail_reason` 写明失败原因。
3. 写入 `recording_failed` 事件。
4. 页面展示录音失败原因。

### 12.3 补偿任务

服务启动后应有补偿任务扫描：

1. 长时间 `PENDING` 的录音。
2. 长时间 `RECORDING` 但通话已结束的录音。
3. 上传 OSS 失败但本地或 Egress 产物仍可读取的录音。

补偿任务可以先基于基座调度器实现，不引入消息队列。

## 13. 语义分析

### 13.1 生成时机

语义分析只在通话结束后异步生成。

触发条件：

1. `end` 接口正常结束通话。
2. Agent 判断通话结束。
3. 超时任务结束通话。
4. 人工坐席结束接管。

### 13.2 输入

分析任务读取：

1. `ai_call_session` 主状态。
2. `ai_call_message` 稳定对话文本。
3. `ai_call_event` 关键事件。
4. `ai_call_recording` 录音状态。

不要求实时读取录音内容。

### 13.3 输出 JSON

Phase 00 固定输出 JSON 结构：

```json
{
  "summary": "一句话总结",
  "callResult": "completed",
  "userIntent": "unknown",
  "transferRequested": false,
  "transferSource": null,
  "tags": [],
  "nextAction": "none",
  "evidenceMessageIds": []
}
```

字段说明：

| 字段 | 说明 |
|---|---|
| `summary` | 通话摘要 |
| `callResult` | `completed`、`transferred`、`failed`、`no_effective_dialogue` |
| `userIntent` | 用户意图，先用字符串表达，不提前绑定业务枚举 |
| `transferRequested` | 是否触发转人工 |
| `transferSource` | 转人工来源，没有则为空 |
| `tags` | 标签数组 |
| `nextAction` | 下一步建议 |
| `evidenceMessageIds` | 支撑结论的消息 ID |

### 13.4 状态

`ai_call_analysis.analysis_status`：

| 状态 | 场景 |
|---|---|
| `PENDING` | 等待分析 |
| `PROCESSING` | 分析中 |
| `COMPLETED` | 分析完成 |
| `FAILED` | 分析失败 |

同一个 `call_id` 只保留一条当前分析结果。重试时更新原记录。

分析开始不单独记录 `analysis_started` 事件，由 `ai_call_analysis.analysis_status=PROCESSING` 表达。只有分析完成或失败时，才写入 `analysis_generated` 或 `analysis_failed` 事件。

## 14. 轻量转人工

Phase 00 的转人工是 WebRTC 坐席接管，不做完整坐席台。

本阶段支持两种触发方式：

1. 用户意图触发：通话中识别到用户表达转人工相关意思。
2. Web 端主动接管：页面观察到对话内容需要人工干预，点击人工接管按钮主动介入。

两种触发方式复用同一套接管流程：

```text
用户意图触发 或 Web 端主动接管
  -> 写入 human_transfer_requested
  -> status=HUMAN_TRANSFER_REQUESTED
  -> 页面显示待人工接管
  -> 调用 human-token
  -> 坐席加入同一 Room
  -> 写入 human_joined
  -> status=HUMAN_TRANSFER_CONNECTED
  -> AI 静音或退出
  -> 坐席看到接管前 ai_call_message
```

必须区分转人工来源：

| 来源 | 说明 | Phase 00 要求 |
|---|---|---|
| `user_intent` | 用户主动要求转人工 | 必须实现 |
| `manual_console` | 页面人工点击接管 | 必须实现 |
| `agent_decision` | AI 判断需要人工 | 预留；实现了对应逻辑才写入 |
| `system_rule` | 系统规则触发 | 预留；实现了对应逻辑才写入 |

本阶段不做技能组、排队、抢单、坐席忙闲状态和坐席工作台复杂能力。

## 15. Web 页面要求

Phase 00 必须交付前端页面。否则只能证明后端 API 存在，不能证明浏览器 WebRTC 用户、麦克风、LiveKit Room、Agent 播放、转人工、录音播放和分析展示形成闭环。

页面可以工程化简洁，定位是“Web 版智能外呼调试台”，不是完整运营后台。

### 15.1 前端形态

推荐先做单页前端：

```text
ai-call-web-console
```

可以采用以下任一方式：

1. 作为 `LingChenAiCallBase` 的静态页面，由 FastAPI 挂载访问。
2. 作为独立 Vite / Vue / React 小前端，通过 HTTP 调用后端 API。
3. 临时集成到现有前端工程，但不引入完整菜单、登录、权限和业务后台复杂度。

Phase 00 不要求做：

1. 登录页。
2. 菜单权限。
3. 租户切换。
4. 用户管理。
5. 完整运营后台。
6. 完整坐席工作台。

### 15.2 HTTP / HTTPS 策略

本地开发可以先使用 HTTP：

```text
http://127.0.0.1:19010
http://localhost:19010
```

本地 `localhost` / `127.0.0.1` 通常可以正常使用浏览器麦克风和 WebRTC 调试。

如果通过公网 IP、局域网 IP 或域名远程访问页面，推荐直接使用 HTTPS，并让 LiveKit 使用 WSS：

```text
https://{domain}
wss://{livekit-domain}
```

原因：

1. 浏览器麦克风权限依赖安全上下文，非本地 HTTP 访问可能被浏览器拦截。
2. HTTPS 页面连接非安全 WebSocket / WebRTC 资源容易被浏览器阻止。
3. 生产环境必须使用 HTTPS / WSS，本阶段不能把远程 HTTP 验证结果当作生产结论。

因此 Phase 00 的默认策略是：

| 场景 | 前端协议 | LiveKit 协议 |
|---|---|---|
| 本机开发 | HTTP | WS 可接受 |
| 测试服务器远程访问 | HTTPS 推荐 | WSS 推荐 |
| 生产 | HTTPS 必须 | WSS 必须 |

### 15.3 免登录和身份策略

Phase 00 不做登录逻辑。

具体要求：

1. 不生成登录页。
2. 不接入验证码。
3. 不调用 `/auth/login`。
4. 不做菜单权限。
5. 不做租户切换。
6. 不要求前端携带业务用户 token。

开发态配置：

```text
JWT_ENABLE=False
```

身份处理：

1. Web 用户身份由后端按 `call_id` 生成，例如 `web_user_{call_id}`。
2. Agent 身份由后端按 `call_id` 生成，例如 `agent_{call_id}`。
3. 人工坐席身份在 Phase 00 使用固定开发态身份或后端生成身份，例如 `human_dev_{call_id}`。
4. 前端不传 `operator_id`，避免伪造坐席身份。
5. 如果 OSS 上传需要 `AuthSchema`，后端构造受控系统身份，例如 `ai_call_system`。
6. LiveKit 入会 token 仍必须由后端签发，前端不能持有 LiveKit API secret。

说明：

1. Phase 00 的目标是验证 Web 业务闭环，不把登录体系作为前置依赖。
2. 内网、网关鉴权、API Key、IP 白名单、服务账号等访问控制方案不在 Phase 00 实现。
3. Phase 00 页面只用于开发和测试环境，不作为公网生产入口。
4. 访问控制的最终方案放到 Phase 04 业务系统接入和运营接口增强阶段处理。

### 15.4 页面能力

页面必须具备：

1. 创建 Web 会话。
2. 展示 `call_id`、`request_id`、`room_name`。
3. 展示 LiveKit 连接状态。
4. 麦克风权限检查。
5. 加入 Room。
6. 发布麦克风音频。
7. 展示麦克风输入音量和输入过低提示。
8. 播放 Agent 音频。
9. 展示远端音频订阅、播放成功或播放失败状态。
10. 展示通话状态。
11. 展示用户和 Agent 的加入状态。
12. 展示对话文本。
13. 展示事件日志和每轮耗时指标。
14. 触发转人工。
15. 人工坐席加入 Room。
16. 结束通话。
17. 查看录音列表并播放录音。
18. 查看语义分析结果。

页面不展示 Redis、数据库、LiveKit secret、OSS secret 或模型 API key。

## 16. 实施任务顺序

| 顺序 | 任务 | 完成标准 |
|---:|---|---|
| 1 | 创建 Phase 00 六张表 ORM 和迁移 SQL | 表、索引、唯一约束与 `04-data-model.md` 一致 |
| 2 | 实现 `POST /calls` 幂等创建 | 返回 `callId`、`roomName`、`webUserToken` |
| 3 | 实现 Phase 00 单页前端 | 页面能调用创建会话接口并展示 `callId`、`roomName` |
| 4 | 实现 LiveKit token 签发 | 浏览器可加入指定 Room |
| 5 | 实现 Web 页面入会和麦克风发布 | Room 中能看到 Web 用户 audio track |
| 6 | 实现 Agent dispatch | Agent 加入同一 Room |
| 7 | 实现输入质量和远端播放诊断 | 页面展示麦克风音量、远端音频订阅、播放成功或失败 |
| 8 | 实现流式级联 ASR / LLM / TTS 对话 | 有 streaming ASR final、LLM 首 token、TTS 首包音频和 Agent 播放 |
| 9 | 实现 VAD / endpointing / 打断 | 能判断用户开始、结束、打断并取消当前播放 |
| 10 | 实现每轮耗时指标 | `turn_metrics` 能归因 endpoint、ASR、LLM、TTS、播放耗时 |
| 11 | 实现事件和消息落库 | `events`、`messages` 接口能查到，页面能展示 |
| 12 | 实现结束通话 | 状态幂等进入结束态，页面能触发 |
| 13 | 实现混音录音 | `ai_call_recording` 有 `mixed` 记录 |
| 14 | 实现 OSS 上传和播放地址 | 录音能在页面播放 |
| 15 | 实现通话后语义分析 | `analysis` 接口能查到 JSON，页面能展示 |
| 16 | 实现轻量转人工 | 人工加入同 Room，能看到前文 |
| 17 | 补齐失败场景 | 页面有明确错误，服务端有事件和日志 |
| 18 | 完整验收 | 一个 `call_id` 可复盘全过程，并能解释每轮延迟 |

## 17. 实现完成后的测试用例矩阵

Phase 00 的验收必须尽量走真实生产链路，但这里的“生产链路”只指 Web 版智能外呼闭环：

```text
Web 页面
-> 后端 API
-> PostgreSQL / Redis
-> LiveKit Room / WebRTC
-> Agent Worker
-> ASR / LLM / TTS
-> LiveKit Egress 或服务端录音
-> OSS / sys_oss
-> 通话后语义分析
-> Web 页面复盘
```

Phase 00 不验证真实 SIP、PSTN、运营商线路、号码白名单、SIP RTP、CPS、并发外呼和真实电话振铃。这些必须放到 Phase 01 或更后面的生产压测阶段验证。

### 17.1 Codex / 实施者可以执行的验证

这些用例只要测试环境授权、连接信息可用，就可以由 Codex 或实施者直接执行，并沉淀命令输出、数据库查询结果、接口响应、日志片段或浏览器截图。

| 编号 | 类别 | 测试用例 | 验收点 |
|---|---|---|---|
| T00-001 | 配置 | `.env.dev` 可被服务读取 | 数据库、Redis、LiveKit、模型、OSS 关键配置能读取；日志不输出密码、secret、token、API key |
| T00-002 | 连接 | PostgreSQL 连接 | 能连接测试库，当前 schema 可用，迁移账号具备建表权限 |
| T00-003 | 连接 | Redis 连接 | 能读写 Phase 00 所需 key，连接失败时服务启动或健康检查有明确错误 |
| T00-004 | 连接 | LiveKit API key / secret 校验 | 后端可签发 token，浏览器或测试客户端可用 token 加入 Room |
| T00-005 | 连接 | 阿里百炼模型可用性 | ASR / LLM / TTS / 通话后分析模型至少各完成一次最小调用 |
| T00-006 | 数据库 | 创建 Phase 00 六张表 | 表结构、字段类型、索引、唯一约束与 `04-data-model.md` 一致，不创建物理外键 |
| T00-007 | 数据库 | `request_id` 幂等 | 相同请求重复提交返回同一个 `call_id`，不同参数复用同一 `request_id` 返回业务错误 |
| T00-008 | API | 创建 Web 通话 | `POST {API_BASE}/ai-call/calls` 返回 `callId`、`roomName`、`webUserToken` |
| T00-009 | API | 查询通话详情 | `GET {API_BASE}/ai-call/calls/{call_id}` 返回状态、业务标识、Room、快照信息 |
| T00-010 | API | 查询消息、事件、参与者 | messages、events、participants 接口能按 `call_id` 返回数据 |
| T00-011 | API | 结束通话幂等 | 首次结束进入结束态，重复结束不重复写异常数据 |
| T00-012 | WebRTC | 浏览器入会 | 页面显示 connected，LiveKit Room 出现 `web_user_{call_id}` |
| T00-013 | WebRTC | 麦克风音频发布 | 浏览器成功发布 audio track，Agent 或服务端能收到音频 |
| T00-014 | Agent | Agent 入会 | Room 出现 `agent_{call_id}` 或约定 Agent identity，事件记录 Agent 加入 |
| T00-015 | WebRTC | 输入质量诊断 | 页面或事件能展示麦克风能量、输入过低提示、远端音频播放状态 |
| T00-016 | Agent | 流式 ASR / LLM / TTS 主链路 | 用户说话后生成用户 final ASR、LLM 首 token、TTS 首包音频、Agent 音频播放 |
| T00-017 | Agent | 端点检测 | 用户停止说话后能在合理窗口内生成 final，过短/过长均有指标可查 |
| T00-018 | Agent | ASR 空结果 | 弱音量或无有效语音时记录 `asr_empty`，页面不误判为 Agent 无响应 |
| T00-019 | Agent | 单轮耗时指标 | 每轮都有 `turn_metrics`，能看到 endpoint、ASR、LLM、TTS、播放耗时 |
| T00-020 | 消息 | 对话文本落库 | `ai_call_message` 保存用户 final ASR 和 Agent final text |
| T00-021 | 打断 | 播放中用户说话 | 记录 `barge_in` 事件，对应 Agent 消息标记 `interrupted=1` |
| T00-022 | 转人工 | 用户意图触发转人工 | 用户说“转人工”后状态进入转人工请求，`transfer_source=user_intent` |
| T00-023 | 转人工 | Web 端主动接管 | 点击人工接管后状态进入转人工请求，`transfer_source=manual_console` |
| T00-024 | 转人工 | 人工坐席入会 | `human-token` 可用，人工加入同一 Room，事件记录 `human_joined` |
| T00-025 | 转人工 | 接管前文本可见 | 人工页面能按 `call_id` 查询并展示接管前对话文本 |
| T00-026 | 录音 | 混音录音生成 | 通话结束后产生 `ai_call_recording`，状态可追踪 |
| T00-027 | OSS | 录音上传 OSS | `ai_call_recording.oss_id` 有值，`sys_oss` 可查到文件索引 |
| T00-028 | 播放 | 录音播放地址 | 页面能获取播放地址，地址权限符合测试环境要求 |
| T00-029 | 分析 | 通话后语义分析 | `ai_call_analysis.analysis_status=COMPLETED`，结果是合法 JSON |
| T00-030 | 失败 | Agent Worker 未启动 | 会话失败或进入可重试状态，页面、日志、事件都有明确原因 |
| T00-031 | 失败 | LiveKit URL 不可达 | 页面提示连接失败，后端不误判为通话成功 |
| T00-032 | 失败 | OSS 上传失败 | 录音状态进入 `FAILED`，失败原因可查询，不影响通话主记录复盘 |
| T00-033 | 失败 | 语义分析失败 | `analysis_status=FAILED`，失败原因可查询，后续可补偿重试 |
| T00-034 | 安全 | 敏感信息检查 | 接口响应、日志、页面不出现数据库密码、Redis 密码、LiveKit secret、模型 API key、完整 token |

### 17.2 需要用户人工确认的验证

这些用例 Codex 可以协助发起、记录和排查，但最终体验判断或外部账号状态必须由用户确认。

| 编号 | 类别 | 测试用例 | 用户需要确认的内容 |
|---|---|---|---|
| U00-001 | 听感 | Agent 语音质量 | 声音是否清晰、自然、音量合适，是否满足商业试用观感 |
| U00-002 | 延迟 | 端到端对话延迟 | 从用户说完到听到 Agent 回复的体感是否可接受，并参考 P50 800ms - 1200ms、P95 1800ms - 2500ms 目标 |
| U00-003 | 设备 | 麦克风和扬声器 | 用户实际办公电脑、耳机、浏览器权限是否正常 |
| U00-004 | 浏览器 | Chrome / Edge / Safari 兼容性 | 目标使用浏览器能否正常入会、播放和录音 |
| U00-005 | 网络 | 远程访问环境 | 测试人员从实际网络访问时，HTTPS / WSS / 防火墙 / 端口是否畅通 |
| U00-006 | 模型账号 | 阿里百炼开通和额度 | API Key 是否有权限、额度、限流策略和计费确认 |
| U00-007 | OSS | 录音访问权限 | 录音 URL 在目标使用场景下能否播放，权限是否符合公司要求 |
| U00-008 | 话术 | 默认开场白和对话边界 | 默认 Agent 话术是否适合当前业务演示或试运行 |
| U00-009 | 语义分析 | JSON 业务含义 | 分析结果字段是否能支撑后续业务决策 |
| U00-010 | 人工接管 | 坐席工作方式 | 坐席看到前文、加入通话、接管时机是否符合真实操作习惯 |

### 17.3 Phase 00 不验证的内容

以下内容不能作为 Phase 00 的完成标准，也不能用 Web 调试结论替代真实线路验收。

| 类别 | 不在 Phase 00 验证的原因 | 后续阶段 |
|---|---|---|
| 真实 SIP 外呼 | Phase 00 不接运营商 SIP 入口 | Phase 01 |
| 手机振铃和接听 | WebRTC Room 不等于真实电话网络 | Phase 01 |
| FreeSWITCH 停止后的线路表现 | Phase 00 不依赖真实 SIP 线路 | Phase 01 |
| SIP From / Contact / Via / rport | 只有接入真实 SIP 时才有意义 | Phase 01 |
| 公网 IP + 端口白名单 | Web 链路不能证明运营商白名单 | Phase 01 |
| 运营商失败码 | WebRTC 失败码与 SIP 失败码不是一类问题 | Phase 01 |
| CPS 和并发外呼 | Phase 00 是功能闭环，不做压测结论 | Phase 07 / Phase 08 |
| 多节点高可用 | Phase 00 先验证单链路正确性 | Phase 07 / Phase 08 |
| 生产监控告警 | 需要真实部署拓扑和容量目标 | Phase 07 / Phase 08 |
| 自定义音色 | 音色体系单独设计，避免影响实时主链路 | Phase 03 |

### 17.4 验收记录要求

每次完整验收至少保留：

1. `call_id`、`request_id`、`room_name`。
2. 创建通话、入会 token、结束通话、录音、分析接口响应摘要。
3. `ai_call_session`、`ai_call_event`、`ai_call_message`、`ai_call_recording`、`ai_call_analysis` 查询结果。
4. LiveKit Room 参与者截图或日志。
5. Web 页面关键状态截图。
6. 录音播放地址或可播放证明。
7. 语义分析 JSON 样例。
8. 失败用例的错误信息、事件和日志片段。
9. 用户人工确认项的结论：通过、不通过、待优化。

验收记录不得包含数据库密码、Redis 密码、LiveKit secret、模型 API key 或完整 token。

## 18. 浏览器验证步骤

### 18.1 启动服务

```bash
cd /Users/tzk/Project/kit/LingChenAiCallBase
python main.py --env dev
```

如果本机没有安装依赖，先使用项目约定的虚拟环境或安装 `pyproject.toml` 依赖。

### 18.2 健康检查

```bash
curl http://127.0.0.1:19010/ai-call/health
```

或通过对外前缀：

```bash
curl http://127.0.0.1:19010/ai-call-api/v1/ai-call/health
```

### 18.3 创建 Web 会话

页面点击创建后调用：

```text
POST {API_BASE}/ai-call/calls
```

请求体由页面自动生成即可：

```json
{
  "requestId": "浏览器自动生成 UUID",
  "businessType": "web_phase00",
  "businessId": "页面自动生成或测试人员填写"
}
```

`channel` 不从前端传入，后端固定写入 `web_call`。

验收：

1. 返回 `callId`。
2. 返回 `roomName`。
3. 返回 `webUserToken`。
4. 数据库有 `ai_call_session`。
5. 事件有 `call_created`。

### 18.4 浏览器入会

验收：

1. 页面显示 connected。
2. LiveKit Room 有 `web_user_{call_id}` participant。
3. 浏览器发布 audio track。
4. 事件有 `web_user_joined`。

### 18.5 Agent 回复

说一句：

```text
你好，我在做 Web 会话测试。
```

验收：

1. Agent 加入同一 Room。
2. Agent 收到音频。
3. 产生 streaming ASR final。
4. 产生 `llm_first_token` 和 Agent 回复文本。
5. 产生 `tts_first_audio`。
6. 浏览器听到 TTS，且无 `client_remote_audio_play_failed`。
7. `ai_call_message` 有用户和 Agent 文本。
8. `turn_metrics` 能看到 endpoint、ASR、LLM、TTS 和播放耗时。

### 18.6 打断

在 Agent 播放时说话。

验收：

1. 事件有 `barge_in`。
2. 对应 Agent 消息 `interrupted=1`。
3. 当前 Agent 播放停止。
4. 未播放的 TTS chunk 被取消。
5. 页面显示该回复被打断。

### 18.7 转人工

方式一：用户意图触发。

用户说：

```text
我要转人工。
```

验收：

1. 事件有 `human_transfer_requested`。
2. `event_payload.transfer_source=user_intent`。
3. 状态为 `HUMAN_TRANSFER_REQUESTED`。
4. 坐席通过 `human-token` 加入同一 Room。
5. 事件有 `human_joined`。
6. 状态为 `HUMAN_TRANSFER_CONNECTED`。
7. 坐席页面能看到接管前对话文本。

方式二：Web 端主动接管。

在页面看到当前对话需要人工干预时，点击人工接管按钮。

验收：

1. 页面调用 `POST {API_BASE}/ai-call/calls/{call_id}/human-transfer`。
2. 请求体 `transferSource=manual_console`。
3. 事件有 `human_transfer_requested`。
4. `event_payload.transfer_source=manual_console`。
5. 状态为 `HUMAN_TRANSFER_REQUESTED`。
6. 坐席通过 `human-token` 加入同一 Room。
7. 事件有 `human_joined`。
8. 状态为 `HUMAN_TRANSFER_CONNECTED`。
9. AI 静音或退出。
10. 坐席页面能看到接管前对话文本。

### 18.8 结束通话

点击结束。

验收：

1. 状态变为 `COMPLETED`。
2. 重复点击结束不报错。
3. 录音任务进入 `RECORDING`、`COMPLETED` 或 `FAILED` 可追踪状态。
4. 分析任务进入 `PENDING`、`PROCESSING`、`COMPLETED` 或 `FAILED` 可追踪状态。

### 18.9 查看结果

按 `call_id` 查看：

1. 通话主状态。
2. 参与者列表。
3. 事件列表。
4. 对话文本。
5. 录音列表。
6. 录音播放地址。
7. 语义分析结果。
8. 转人工来源和人工加入时间。

## 19. 失败场景验收

至少验证：

| 场景 | 预期 |
|---|---|
| 浏览器拒绝麦克风权限 | 页面提示权限问题，不创建无意义音频状态 |
| LiveKit token 过期 | 页面可重新签发 token |
| LiveKit URL 不可达 | 页面提示连接失败，后端记录事件 |
| Agent Worker 未启动 | 会话进入失败或可重试状态，事件有失败原因 |
| Agent dispatch 超时 | `call_failed` 或明确错误事件 |
| 麦克风输入过低 | 页面提示输入过低，事件有输入能量摘要 |
| ASR 未识别到文本 | 记录 `asr_empty`，Agent 可提示重说，页面不能表现为无响应 |
| 浏览器远端音频播放失败 | 记录 `client_remote_audio_play_failed`，页面提示检查浏览器播放权限或输出设备 |
| 用户关闭页面 | 记录 `web_user_left`，按规则结束或等待超时 |
| 重复调用 `end` | 幂等返回当前结束状态 |
| 录音启动失败 | 通话可结束，录音状态 `FAILED` |
| OSS 上传失败 | 录音状态 `FAILED`，页面显示失败原因 |
| 语义分析失败 | `analysis_status=FAILED`，可重试 |
| 坐席 token 签发失败 | 页面提示，事件记录失败原因 |
| 坐席加入 Room 失败 | 状态保持可追踪，不影响原通话结束 |

每个失败场景必须有：

1. 页面错误提示。
2. 服务端日志。
3. `ai_call_event` 事件。
4. 可查询状态。

## 20. 日志字段

所有关键日志必须包含：

```text
call_id
request_id
business_type
business_id
channel
room_name
participant_identity
participant_type
agent_name
agent_job_id
recording_id
analysis_status
event_type
error_code
latency_ms
turn_id
endpoint_delay_ms
llm_ttft_ms
tts_ttfb_ms
first_audio_latency_ms
```

禁止在日志中输出：

1. 数据库密码。
2. Redis 密码。
3. LiveKit API secret。
4. LiveKit token。
5. 模型 API key。
6. 完整未脱敏手机号。

## 21. 通过标准

Phase 00 只有满足以下条件才算完成：

1. 不发起任何真实 SIP 呼叫。
2. 浏览器能创建 Web 会话。
3. 浏览器能加入 LiveKit Room。
4. 浏览器麦克风音频能进入 Room。
5. Agent 能加入同一个 Room。
6. Agent 主链路采用真实流式级联 ASR / LLM / TTS；非流式只能作为显式降级。
7. 用户正常音量连续 3 轮对话，每轮都有用户 ASR final、Agent 回复文本、Agent 音频播放和 `turn_metrics`。
8. 用户 ASR final 和 Agent 回复文本写入 `ai_call_message`。
9. 页面能展示麦克风输入质量、输入过低提示、远端音频订阅和播放失败事件。
10. ASR 空结果必须记录 `asr_empty`，并能解释音频时长、输入能量和 endpoint 情况。
11. 首段回复延迟能按 endpoint、ASR、LLM、TTS、播放阶段归因；如果未达目标，必须说明瓶颈。
12. 打断能记录 `barge_in` 和 `interrupted=1`，并能停止当前 Agent 播放。
13. 转人工必须能区分 `user_intent` 和 `manual_console`；`agent_decision`、`system_rule` 作为保留来源，若实现对应触发逻辑也必须正确落库。
14. 人工坐席能通过 WebRTC 加入 Room 并看到前文。
15. 状态、参与者、事件可查询。
16. 通话结束后生成服务端混音录音。
17. 录音上传 `sys_oss`，`ai_call_recording.oss_id` 有值。
18. 页面能播放录音。
19. 通话结束后生成 JSON 语义分析。
20. 页面能按 `call_id` 完整复盘。
21. 失败场景有明确状态、事件和日志。

## 22. 阶段完成输出

Phase 00 完成后必须沉淀：

1. 可运行的 Web 版业务入口。
2. 一条完整 Web 业务闭环通话记录。
3. 验收用 `call_id`、`request_id`、`room_name`。
4. Web 用户、Agent、人工坐席 participant identity。
5. Agent job ID。
6. `ai_call_session` 主记录截图或查询结果。
7. `ai_call_event` 关键事件截图或查询结果。
8. `ai_call_message` 对话文本截图或查询结果。
9. `ai_call_recording` 和 `sys_oss` 关联结果。
10. 可播放录音地址。
11. `ai_call_analysis` JSON 结果。
12. 已知问题清单。
13. 是否进入 Phase 01 的结论。
