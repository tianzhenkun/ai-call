# Phase B5：转人工后语义分析 Snapshot 契约

最后更新：2026-07-10

## 1. 文档定位

本文档定义转人工后语义分析第一版的最小 snapshot 契约。

它只解决一个问题：

> 通话后语义分析能在同一份 `transcript_snapshot_json` 里看见转人工状态、人工坐席身份，以及 AI、客户、人工坐席分别说了什么。

本阶段不是重新设计转人工状态机，也不是扩展完整质检结果。现有 `analysis_result` 仍保持五字段 JSON：`summary`、`feedback_type`、`key_points`、`time_hint`、`tags`。

“转人工后的语义分析”不是只分析人工接通后的片段，而是分析整通电话：

1. AI 与客户阶段。
2. 转人工请求、等待、接通或失败阶段。
3. 坐席与客户阶段。

但“纳入分析”和“可作为客户事实”必须分开。AI 和坐席文本可以帮助理解上下文，客户事实只能来自客户自己说过的话。

## 2. 第一性原理

转人工后语义分析不能先从 prompt 开始。

如果 snapshot 里没有人工阶段文本、没有转人工状态、没有坐席身份，那么模型只能分析 AI 阶段，或者在缺证据时编造人工沟通。因此第一优先级是补齐输入契约，再谈总结口径。

第一版必须满足三条边界：

1. 客户事实只能来自 `role=user` 且 `speaker_type=customer` 的真实文本。
2. AI 和人工坐席都属于服务方，使用 `role=assistant`，通过 `speaker_type=ai` / `speaker_type=human_agent` 区分。
3. 转人工请求、接通、失败、超时等过程事实放在 `handoffs`，不要伪装成对话 turns。

## 3. 最小契约

### 3.1 顶层字段

`transcript_snapshot_json` 第一版保持现有结构，并新增 `handoffs`：

```json
{
  "call_id": "call_xxx",
  "scene_code": "intro_geo",
  "turns": [],
  "handoffs": [],
  "metadata": {}
}
```

无转人工时：

```json
{
  "handoffs": []
}
```

### 3.2 `handoffs[]`

每条 handoff 保存一次转人工请求的业务状态：

```json
{
  "handoff_id": "handoff_123",
  "status": "requested",
  "request_source": "customer",
  "request_reason": "customer_request",
  "request_message": "用户明确要求转人工",
  "human_agent_identity": null,
  "requested_at": "2026-07-09T10:00:10Z",
  "accepted_at": null,
  "connected_at": null,
  "ended_at": null,
  "expires_at": "2026-07-09T10:02:10Z",
  "end_reason": null,
  "failure_stage": null,
  "failure_message": null
}
```

字段来源优先使用 `ai_call_handoff`。

字段含义：

| 字段 | 含义 |
| --- | --- |
| `handoff_id` | 本次转人工请求的唯一 ID，用于关联 handoff metadata 与人工阶段 turns。 |
| `status` | 转人工业务状态，例如已请求、已接通、已结束、失败、超时。 |
| `request_source` | 触发来源，例如客户主动要求、AI 判断触发、系统触发。 |
| `request_reason` | 触发原因，例如 `customer_request`。 |
| `request_message` | 转人工时的说明文本，可用于审计和展示。 |
| `human_agent_identity` | 接听坐席身份；未接通时可以为空。 |
| `requested_at` | 发起转人工请求的时间。 |
| `accepted_at` | 坐席接受请求的时间。 |
| `connected_at` | 客户与坐席实际接通的时间。 |
| `ended_at` | 人工阶段结束时间。 |
| `expires_at` | 转人工请求过期时间。 |
| `end_reason` | 人工阶段结束原因。 |
| `failure_stage` | 转人工失败时，失败发生的阶段。 |
| `failure_message` | 转人工失败时的说明。 |

### 3.3 `turns[]`

`turns` 只放真实说出来的话，不放 `handoff_requested`、`agent_joined` 这类系统事件。

第一版只把两个字段作为硬契约：

1. `speaker_type=human_agent`：标识人工坐席说的话。
2. `handoff_id`：当一条话能明确归属某次转人工时，挂到对应 handoff。

`speaker_identity` 可保留在 snapshot 里用于审计，但不作为语义强事实依据。`phase` 暂不作为硬契约；如需展示，可由 `handoff_id`、时间戳和 handoff 状态派生。

示例：

```json
{
  "seq": 8,
  "role": "assistant",
  "speaker_type": "human_agent",
  "speaker_identity": "agent-debug-001",
  "handoff_id": "handoff_123",
  "text": "您好，我来继续跟您沟通。",
  "source": "offline_asr",
  "segment_status": "final",
  "started_at": "2026-07-09T10:00:45Z",
  "ended_at": "2026-07-09T10:00:48Z"
}
```

## 4. 语义分析口径

### 4.1 人工阶段文本来源

人工坐席接通后，若分参与方录音开启，系统会记录 `track_role=human_agent` 的坐席轨道。

通话结束且录音完成后，离线 ASR 应处理 `customer` 和 `human_agent` 两类轨道，并写入 `ai_call_dialogue_segment`：

```json
{"source": "offline_asr", "speaker_type": "human_agent"}
```

`ai` 轨道仍不进入离线 ASR；AI 文本继续使用 realtime transcript。

这里的原因是：AI 话术本来就是系统生成的文本，再转成语音播出。它的原文已经存在于 realtime transcript / dialogue segment 中，不需要再把 AI 音频送入离线 ASR “听自己一遍”。这样可以避免额外成本、延迟和回声串音误识别。

### 4.2 分析范围与证据边界

整通电话都进入语义分析，但不同角色的文本用途不同：

| 阶段 | 文本来源 | 是否进入分析 | 是否可作为客户事实 |
| --- | --- | --- | --- |
| AI 与客户阶段 | 客户文本 | 是 | 是 |
| AI 与客户阶段 | AI 文本 | 是 | 否，只能作为服务方上下文 |
| 转人工请求/等待阶段 | `handoffs[]` metadata | 是 | 否，只能说明转人工过程事实 |
| 坐席与客户阶段 | 客户文本 | 是 | 是 |
| 坐席与客户阶段 | 坐席文本 | 是 | 否，只能作为服务方上下文或坐席动作 |

允许的总结示例：

```text
客户先询问 GEO 服务效果，转人工后继续询问价格、试用和收费周期。
```

不允许的总结示例：

```text
坐席介绍一年一万元，因此客户预算为一年一万元。
```

除非客户自己表达了预算、接受价格或购买承诺，否则不能从 AI 或坐席话术反推客户事实。

### 4.3 已请求但未接通

如果有 handoff metadata，但没有人工阶段 turns，summary 仍应总结 AI 阶段里客户和 AI 真实聊过的内容。

但 summary 不能写出不存在的人工沟通。

可以写：

```text
客户在 AI 沟通过程中询问产品试用，并表达希望转人工。系统已发起转人工请求，但未形成有效人工接通记录，因此未产生人工阶段沟通内容。
```

不能写：

```text
人工坐席与客户沟通后确认客户需要报价。
```

### 4.4 已接通且有人工 turns

人工阶段客户文本仍是客户事实来源：

```json
{"role": "user", "speaker_type": "customer", "handoff_id": "handoff_123"}
```

人工坐席文本只能作为服务方上下文或坐席动作：

```json
{"role": "assistant", "speaker_type": "human_agent", "handoff_id": "handoff_123"}
```

坐席说“我稍后发资料”可以总结为“坐席表示稍后发资料”，不能写成“客户承诺稍后发资料”。

### 4.5 语义证据与输出护栏

`semantic_evidence` 只能用于客户文本，即 `role=user` 且 `speaker_type=customer` 的真实话语。坐席文本不得生成 `semantic_evidence`，也不得进入客户事实、客户诉求、客户预算、客户身份、客户承诺等强结论。

既有护栏在转人工后继续有效：

1. `record_only`：可以保留在 transcript / evidence 审计中，但不能进入 `summary`、`key_points`、`time_hint` 这类强输出。
2. 低置信 ASR：可以保留原始文本，但必须降低采信等级；如果是坐席轨道串入客户音频、英文误识别、重叠说话等情况，不能当作正常坐席或客户事实。
3. 冲突来源：realtime transcript、offline ASR、不同轨道之间存在明显冲突时，不直接形成强事实。
4. 服务方文本：AI 和坐席文本不能被改写成“客户表示/客户承诺/客户需要”。

`analysis_result` 面向产品展示，不应泄露内部证据字段或诊断 token，包括但不限于 `semantic_evidence`、`supports_strong_fact`、`unsupported_*_fact`、`reason_codes`、`handoff_id`、`transcript_quality`、`record_only`。

`time_hint` 只能来自客户明确表达的时间信息，不能从 `requested_at`、`connected_at`、`started_at`、`ended_at` 等系统时间戳推导。

## 5. 不做范围

第一版不做：

1. 不扩展 `analysis_result` schema。
2. 不新增 `role=human`。
3. 不把 `phase` 作为必须落库字段。
4. 不把转人工系统事件塞进 `turns`。
5. 不要求人工阶段实时转写。
6. 不把坐席话术当客户诉求或客户承诺。

## 6. 验收用例

1. 无转人工：`handoffs=[]`，旧语义分析行为不变。
2. 已请求未接通：有 `handoffs[]`，无 `speaker_type=human_agent` turns，summary 可说明未接通，但不能编造人工沟通。
3. 已接通且有坐席文本：snapshot 出现 `speaker_type=human_agent`，该 turn 使用 `role=assistant`，并带 `handoff_id`。
4. 人工阶段客户文本：仍使用 `role=user`、`speaker_type=customer`，可进入客户侧语义证据。
5. 坐席文本：不得生成 `semantic_evidence`，不得被当作客户事实。
6. `record_only`、低置信 ASR、冲突来源等既有护栏继续有效。
7. AI 文本继续使用 realtime transcript，不对 `ai` 轨道做离线 ASR。
8. 输出结果不得包含内部证据字段、handoff 诊断字段或 transcript 质量 token。
9. `time_hint` 不得来自系统时间戳，只能来自客户明确说出的时间。
10. 已接通样本应能同时总结 AI 阶段客户诉求与人工阶段客户追问，例如试用、价格、收费周期；坐席话术只作为上下文。

## 7. 已验证样本

本地 `19012` 验证中，以下样本用于确认契约方向：

1. `call_333872434722619392`：AI 阶段与人工阶段均有客户有效表达，最终可总结“推荐概率关注、试用意向、价格咨询、收费周期确认”，semantic acceptance 与 timeline audit 均通过。
2. `call_333810168216141824`：人工阶段出现话题漂移和坐席轨道低置信串音，语义结果只采信客户真实话语，并带转写噪声风险标签。

这些样本只作为契约验收参照，不要求业务代码硬编码 call_id。
