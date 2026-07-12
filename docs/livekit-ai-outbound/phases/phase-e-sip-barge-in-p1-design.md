# SIP 外呼 P1 本地快速打断设计

最后更新：2026-06-30

适用范围：Phase E SIP 外呼 P1。

## 1. 文档定位

本文档定义 SIP 外呼场景下 P1 阶段的本地快速打断方案。

P1 只解决一个核心问题：

```text
本地 SIP 音频已经检测到用户插话
但当前系统仍等待 Qwen user_speech_started
导致 AI 停播慢
```

P1 的目标是把“停 AI 的第一权限”前移到本地：

```text
SIP 上行音频达成本地 candidate
-> 立即 pre-stop AI 播放
-> 短窗口确认是真人还是回声/噪声
-> Qwen 只做后续识别和语义处理
```

本文档不替代 Phase E SIP 接入设计，也不重新设计外呼任务、SIP trunk、录音闭环或转人工闭环。

## 2. 当前事实

当前项目已具备以下基础：

1. SIP 外呼入口代码地基已按 LiveKit SIP service 设计，SIP 用户身份为 `sip-{call_id}`；真实电话线路仍需单独验收。
2. AI Agent 在 LiveKit Room 内发布 `agent-{call_id}` 侧 AI 音频，并消费远端音频送入 Qwen Realtime。
3. 当前 `SipBargeInDetector` 已有 RMS + 持续时间的 SIP candidate 检测。
4. 当前 SIP candidate 只作为候选事件，真正停播仍依赖后续确认，容易被 Qwen `user_speech_started` 延迟拖慢。
5. Web 链路已有 generation gate 和 `interrupted` 文本展示基线，可复用“旧音频不能迟到播放”的思路。
6. 当前不要求实时展示 candidate/clean window 阶段字幕，用户文本只在 `user_transcript_done` 后展示。

## 3. 目标

P1 目标：

1. 用户插话时，AI 能先本地停播，不再等待 Qwen `user_speech_started`。
2. 候选检测不只依赖 RMS，增加 WebRTC VAD 作为人声证据。
3. pre-stop 后通过短 clean window 判断 confirmed / rejected。
4. confirmed 后再 cancel Qwen 当前 response，并进入用户输入轮。
5. rejected 后不污染上下文，不展示客户文本，通过短续接/重问恢复会话。
6. 不新增数据库字段，关键诊断信息先放事件 payload。
7. 先支持 Web 端状态验证和软电话 SIP 验证，真实电话作为最终验收。

P1 成功标准：

```text
candidate_to_stop_ms P95 < 300ms
pre_stop_to_confirm_ms P95 < 500ms
不再出现本地已检测但等待 Qwen 5-7 秒才停播
误停播后不会写入正式用户文本或触发业务动作
旧 AI 音频不会在 pre-stop 后继续迟到播放
```

## 4. 非目标

P1 不做以下内容：

1. 不做 duck 音量降低分支。
2. 不做从旧音频中间续播。
3. 不展示实时 provisional 字幕。
4. 不新增 `ai_call_dialogue_segment` 字段。
5. 不做话术级 `HIGH/NORMAL/LOW/OFF` barge-in 策略。
6. 不接 WebRTC APM AEC/NS。
7. 不接 RNNoise、DeepFilterNet 或源分离模型。
8. 不做 ASR 文本相似度判断。
9. 不依赖 LiveKit Cloud Krisp。
10. 不把 P2/P3 的媒体治理能力放进 P1 主链。

## 5. 总体链路

```text
SIP 上行音频
-> 输入隔离，只处理 sip-{call_id} 客户轨
-> RMS/SNR + impulse noise guard + WebRTC VAD + 持续时间
-> sip_interrupt_candidate
-> pre-stop authority 判断
-> sip_pre_stop，立即 stop_audio
-> generation gate，阻止旧 AI 音频迟到播放
-> clean window，默认 300ms
-> confirmed / rejected
```

confirmed 分支：

```text
clean window 内仍有稳定人声
-> sip_interrupt_confirmed
-> cancel_response / flush
-> 等 Qwen user_transcript_done
-> 展示 final 客户文本
-> 进入下一轮 AI 回复
```

rejected 分支：

```text
clean window 内声音消失或 VAD 不再稳定，且不满足强短句保护
-> sip_interrupt_rejected
-> 不展示客户文本
-> 不写上下文
-> 短静默后短续接/重问
```

## 6. 输入隔离

P1 必须先保证送入本地打断判断和 Qwen 的主要客户音频来自 `sip-{call_id}`。

设计要求：

1. SIP 会话只把目标 SIP participant 的音频作为客户上行音频。
2. `agent-{call_id}`、`human-agent-*`、`browser-*`、未知 participant 不应进入 SIP 客户输入主链。
3. 如果当前 LiveKit SDK 事件中无法可靠识别 participant，应先在 transport 层补 participant identity 校验。

这一步不是 AEC，但它是防止数字链路回灌的基础。

## 7. Candidate 检测

P1 candidate 不再只看 RMS。

候选条件：

```text
AI 正在播放
+ RMS 达标
+ SNR 达标
+ 不像拍手/敲桌/碰麦这类冲击噪声
+ WebRTC VAD 连续命中
+ 候选持续时间达标
```

pre-stop 前必须先做冲击噪声过滤。拍手、敲桌、碰麦通常表现为瞬时 peak 很高、持续很短、后续快速回到底噪、VAD 不连续。满足这类特征时，记录 `ignored_impulse_noise` 诊断，不触发 pre-stop。

P1 需要保护“停”“别说了”“不用”等强短句。保护方式不是匹配关键词，而是当本地 SIP 声学证据或 provider speech_started 已经确认这是客户人声后，允许后续短文本通过 transcript trust；不能因为文本只有一两个字且与 AI 音频重叠，就一律判成低可信噪声。

强短句保护不能等同于“所有高能量短声”。默认配置下，120ms 左右的短强声只作为观察证据，不能直接拿到 pre-stop 权限；需要继续到更稳定的本地人声窗口，或被 provider speech_started 佐证后，才进入强打断链路。若出现“过热爆发后能量快速掉到弱尾音”的形态，更接近咳嗽/碰麦/冲击尾音，应先暂缓 pre-stop；如果后续继续形成稳定人声，再升级为 pre-stop，如果很快静音则自然重置，避免先 pre-stop 再 rejected 的听感抖动。

削顶/过热短声不能立刻拿到 pre-stop 权限。若短窗口内 peak 贴近 0 dBFS，且 RMS 明显过高，即使 WebRTC VAD 连续命中，也更可能是喷麦、近场咳嗽或碰麦冲击；P1 应把这种 `clipped_hot_onset` 作为本地语音质量拒绝证据先暂缓 pre-stop。若后续持续形成稳定 VAD + SNR，且最新 peak/RMS 已回到非削顶人声范围，可释放该拒绝证据，避免把电话里常见的响亮短句永久锁死。

连续拍手、咳嗽串和碰麦摩擦不能只靠总时长判断。P1 在 candidate 前需要观察帧级能量包络：如果短窗口内 RMS 范围过大、相邻帧跳变频繁、能量方向多次反复反转，更接近冲击/节律噪声，应压制到静音重置；普通电话短句允许有音节起伏，不能因为 1-2 次 RMS 跳变就吞掉 candidate。平滑连续的人声短句才允许进入 `strong_short_speech_candidate`，持续稳定的人声再进入 `stable_speech_candidate`。pre-stop 之后的 clean window 也不能只看“还有 VAD”，如果能量包络呈现明显 rise-fall-tail，且尾部落到本轮最低能量附近，应判为本地语音质量证据不足，走 rejected。

候选分类：

1. `stable_speech_candidate`：稳定人声候选；默认 180ms 先记录 candidate。若本地证据是中等音量、SNR 充足、包络平稳且不处在刚结束客户语音的尾音保护窗内，继续稳定到 micro-confirm 时长后进入 pre-stop；否则继续 deferred。
2. `strong_short_speech_candidate`：高置信短人声，进入 pre-stop + protected hold；后续仍需要 provider speech_started、有效 transcript 或稳定本地人声证据确认，不能只靠“短且响”直接 confirmed。
3. `impulse_noise`：冲击噪声，不 pre-stop。

播放窗口边界：

1. AI 正在播放时，candidate 先进入本地候选；pre-stop authority 达标后才触发 pre-stop。
2. AI 刚播放完但已无音频可停时，不触发 pre-stop，只进入 echo-tail 保护判断，避免把短尾音当成正式用户输入。
3. AI 已安静超过 echo-tail 保护窗口后，按普通用户输入处理。

初始参数：

| 参数 | 初始值 | 说明 |
| --- | ---: | --- |
| `rms_threshold_dbfs` | `-36` | 绝对音量下限 |
| `snr_threshold_db` | `10` | 高过底噪的信噪比 |
| `vad_voiced_duration_ms` | `120` | WebRTC VAD 连续人声时长 |
| `candidate_min_duration_ms` | `180` | 候选总持续时长 |
| `pre_stop_min_duration_ms` | `240` | `stable_speech_candidate` / `strong_short_speech_candidate` 拿到本地停播权限前的 micro-confirm 时长；180ms 只产生候选，不直接 pre-stop |
| `short_speech_min_duration_ms` | `180` | 强短句最小保护时长；120ms 短强声只观察，不直接 pre-stop |
| `impulse_noise_max_duration_ms` | `120` | 冲击噪声候选最大时长 |
| `clean_window_ms` | `300` | pre-stop 后确认窗口 |
| `max_hold_ms` | `500` | 确认窗口最大保护上限 |
| `echo_tail_window_ms` | `500` | AI 刚播完后的尾音保护窗口 |
| `recovery_silence_ms` | `500-800` | rejected 后短续接前静默等待 |
| `recovery_max_per_turn` | `1` | 同一轮最多自动恢复一次 |

这些值只作为第一版基线，后续必须按 Web/软电话/真实电话样本调参。

## 8. Pre-stop 与 Generation Gate

candidate 达标不等于马上停播。P1 把“发现候选”和“允许停播”拆开：

```text
sip_interrupt_candidate
-> stable_speech_candidate 且 candidateDurationMs < pre_stop_min_duration_ms
-> 即使已有清晰短句证据，也先 sip_pre_stop_deferred
-> 后续继续稳定到 pre_stop_min_duration_ms 或形成 turn-taking 证据后 sip_pre_stop
```

默认 `candidate_min_duration_ms=180`、`pre_stop_min_duration_ms=240`。这样 180ms 左右的短声可以被记录为 candidate，但 single-short 证据本身不再等于停播授权：中等音量或清晰偏响、SNR 充足、未达到节律噪声包络阈值、非刚说完尾音窗口的本地人声，需要再经过约 60ms 本地 micro-confirm 才快速 pre-stop；短促强声、热起声回落、低能量边缘声和刚结束客户语音后的同源尾音仍继续 deferred。

为避免“客户刚说完 -> AI 下一句开头 -> SIP 上行尾音/回声又触发 pre-stop”，P1 在 provider `user_speech_stopped` 后保留一个短 tail guard。tail guard 内的本地-only `stable_speech_candidate` 先 deferred；如果 provider speech_started 后续确认，仍可按 provider 确认链路立即强停。

为避免“咳嗽/喷麦初段被识别为可疑 candidate，后续尾音又重新拿到 pre-stop 权限”，P1 对短热爆发的质量拒绝在同一有声段内保留到静音重置。短热爆发尾音只能继续 deferred，不能仅靠后续低幅稳定 VAD 升级为 pre-stop；如果热开头后持续形成稳定人声，且最新 peak/RMS 已恢复到非削顶范围，可在证据稳定后升级。

deferred candidate 也需要有效期。若 candidate 曾因质量证据不足、tail guard 或 rejected_noise guard 被 deferred，超过短有效期后不能再用旧 candidate 升级 pre-stop；必须重新形成新鲜、连续、稳定的本地人声证据，避免低能量噪声在数秒后“补票”打断下一句 AI。

拿到 pre-stop authority 后，P1 统一执行 pre-stop，不做 duck：

```text
sip_interrupt_candidate
-> stop_audio
-> sip_pre_stop
```

pre-stop 必须同时触发 generation gate：

1. 当前 AI generation 标记为 suppressed / invalidated。
2. 旧 generation 的 `model_audio_delta` 即使迟到，也不能继续发布到 LiveKit。
3. 旧 AI transcript 可以被标记为 `interrupted`，但不能变成新的完整回复。
4. pre-stop 后不从旧音频中间续播。

这样可以优先保证用户体感：AI 先让话。

## 9. Clean Window

clean window 是 pre-stop 后的短确认窗口。

```text
candidate 达标
-> AI 已 stop_audio
-> 从 stop_audio 后开始计 300ms
-> 观察 SIP 上行是否仍有稳定人声
```

判断规则：

1. clean window 内 VAD + SNR 仍稳定，判定 `confirmed`。
2. pre-stop 前已是 `strong_short_speech_candidate` 时，即使 clean window 内声音很快消失，也不立即 rejected；进入 protected hold，等待 provider speech_started、有效 final transcript 或稳定本地人声证据。超过 `max_hold_ms` 仍无可靠证据时 rejected。
3. clean window 内声音很快消失，且 pre-stop 前不是强短句，判定 `rejected_echo_or_tail`。
4. clean window 内只有短促、不稳定、非连续 VAD 声音，判定 `rejected_noise`。
5. 超过 `max_hold_ms` 仍不确定时，必须同时满足稳定 VAD、SNR 和累计人声时长达标才 confirmed；否则 rejected。

P1 不展示 clean window 阶段识别文本。

## 10. 确认处理

confirmed 表示系统确认“客户正在说话”，但不表示已经拿到最终文本。

处理动作：

1. 写入 `sip_interrupt_confirmed`。
2. 执行 `cancel_response`。
3. flush / 保持旧音频 gate。
4. 进入用户输入轮。
5. 等待 Qwen `user_transcript_done`。
6. 只展示 final 客户文本。
7. 基于 final transcript 触发下一轮 AI 回复和业务判断。

Qwen `user_speech_started` 在 P1 中只作为对照事件，不再决定第一时间停播。

## 11. 拒绝处理

rejected 表示系统认为本次 candidate 更像回声、尾音、短噪声或误触发。

处理动作：

1. 写入 `sip_interrupt_rejected`，payload 标明 reason。
2. 不展示客户文本。
3. 不写正式上下文。
4. 不触发转人工、业务判断或下一轮用户语义。
5. 不续播旧音频。
6. 等短静默后，发起短续接/重问。

短续接优先级：

1. 如果有当前业务节点问题，重问当前关键问题。
2. 如果没有当前节点问题，让 Qwen 用一句简短自然的话继续刚才未完成的问题或说明。

约束：

1. 同一轮最多自动续接 1 次。
2. 连续 rejected 超过 2 次，不再自动续接，进入静默等待或降低敏感度。
3. rejected_noise 后，如果同一个 AI response 尚未切换，后续本地-only `stable_speech_candidate` 先视为同源尾音并 deferred，不能再次 pre-stop；provider speech_started 仍可强停。
4. 短续接不应提“误触发”“系统判断”。

## 12. 字幕和正式文本

P1 不做实时 provisional 字幕。

展示规则：

```text
candidate / clean window 阶段：不展示客户文字
confirmed 后：仍等待 Qwen final transcript
user_transcript_done：展示客户 final 文本并进入上下文
rejected：不展示客户文本
```

现有 `partial/final/interrupted` 对话展示机制保持不变。P1 不新增 dialogue 表字段。

## 13. 事件设计

P1 最小事件：

| 事件 | 触发时机 |
| --- | --- |
| `sip_impulse_noise_ignored` | 高能量短促冲击声被 pre-stop 前过滤 |
| `sip_interrupt_candidate` | 本地 RMS/SNR + WebRTC VAD 达标 |
| `sip_pre_stop_deferred` | 候选已出现，但尚未达到本地停播授权时长 |
| `sip_pre_stop` | 已执行 stop_audio / generation gate |
| `sip_interrupt_confirmed` | clean window 确认为真人插话 |
| `sip_interrupt_rejected` | clean window 判定回声/尾音/噪声 |
| `sip_recovery_started` | rejected 后准备短续接/重问 |

事件 payload 建议字段：

```json
{
  "reason": "sip_uplink_speech_during_ai_audio",
  "decision": "confirmed",
  "candidateClass": "stable_speech_candidate",
  "rmsDbfs": -32.5,
  "noiseFloorDbfs": -45.0,
  "snrDb": 12.5,
  "vadVoicedMs": 140,
  "candidateDurationMs": 180,
  "requiredPreStopDurationMs": 240,
  "cleanWindowMs": 300,
  "candidateToStopMs": 120,
  "preStopToDecisionMs": 300,
  "responseId": "resp_xxx",
  "generation": 3
}
```

P1 不新增 DB 字段。事件 payload 足够支持第一阶段复盘。

## 14. 配置项

P1 需要配置项，但不需要复杂策略表：

```text
AI_CALL_SIP_BARGE_IN_ENABLED
AI_CALL_SIP_BARGE_IN_RMS_THRESHOLD_DBFS
AI_CALL_SIP_BARGE_IN_SNR_THRESHOLD_DB
AI_CALL_SIP_BARGE_IN_VAD_VOICED_DURATION_MS
AI_CALL_SIP_BARGE_IN_CANDIDATE_MIN_DURATION_MS
AI_CALL_SIP_BARGE_IN_PRE_STOP_MIN_DURATION_MS
AI_CALL_SIP_BARGE_IN_SHORT_SPEECH_MIN_DURATION_MS
AI_CALL_SIP_BARGE_IN_IMPULSE_NOISE_MAX_DURATION_MS
AI_CALL_SIP_BARGE_IN_CLEAN_WINDOW_MS
AI_CALL_SIP_BARGE_IN_MAX_HOLD_MS
AI_CALL_SIP_BARGE_IN_ECHO_TAIL_WINDOW_MS
AI_CALL_SIP_BARGE_IN_RECOVERY_SILENCE_MS
AI_CALL_SIP_BARGE_IN_RECOVERY_MAX_PER_TURN
```

已有配置可复用时优先复用，避免重复命名；新增配置应保持默认关闭或灰度可控。

## 15. 验证路径

### 15.1 Web 端验证

Web 端先验证状态机，不证明真实 SIP 回声质量。

验证内容：

1. 180ms 清晰调制短句只产生 candidate/deferred；继续稳定到 micro-confirm 时长后才可 pre-stop。
2. 削顶/过热短声只产生 candidate/deferred，不直接 pre-stop；后续恢复成稳定人声后可释放拒绝。
3. 普通电话短句允许有限 RMS 起伏，不能被节律噪声包络过早吞掉。
4. deferred candidate 过期后不能在数秒后升级 pre-stop。
5. 持续稳定人声达到 pre-stop 授权时长后能 pre-stop。
6. generation gate 能阻止旧 AI 音频继续播放。
7. confirmed 后能 cancel response。
8. rejected 后不会展示客户文本。
9. final transcript 仍按现有机制展示。

### 15.2 软电话验证

软电话验证 SIP participant 链路。

验证内容：

1. `sip-{call_id}` 音频能进入本地 detector。
2. SIP candidate 可以先停 AI，不等待 Qwen。
3. 分轨录音中客户轨和 AI 轨可复盘。
4. candidate / pre-stop / confirmed / rejected 事件时间线完整。

### 15.3 真实电话验收

真实手机线路是最终验收。

验证内容：

1. 外放/免提双讲下是否仍能快速停 AI。
2. AI 回声是否导致大量 rejected 或误 confirmed。
3. Qwen final transcript 是否可用。
4. `candidate_to_stop_ms` 是否稳定小于 300ms。
5. 误停后短续接是否自然。

## 16. 测试建议

单元测试：

1. RMS/SNR 不达标不产生 candidate。
2. VAD 未连续命中不产生 candidate。
3. 拍手/敲桌类冲击噪声不触发 pre-stop。
4. candidate 达标后触发 pre-stop。
5. clean window 持续人声进入 confirmed。
6. 强短句 candidate 不因 clean window 静音直接 rejected。
7. 非强短句的短促不稳定声音进入 rejected。
8. rejected 后同一轮最多恢复一次。

状态测试：

1. `candidate -> pre_stop -> confirmed -> cancel_response`。
2. `candidate -> pre_stop -> rejected -> recovery`。
3. pre-stop 后旧 generation audio_delta 被丢弃。
4. rejected 不生成 customer final 文本。

集成测试：

1. Web 验证页跑通一次 confirmed。
2. Web 验证页跑通一次 rejected。
3. 软电话 SIP 跑通一次 confirmed。
4. 软电话 SIP 跑通一次 rejected 或短噪声样本。

## 17. 验收标准

P1 验收以事件、录音和体感共同判断。

必须满足：

1. 本地 candidate 后 AI 不再等 Qwen `user_speech_started` 才停播。
2. `candidate_to_stop_ms` P95 小于 300ms。
3. `pre_stop_to_confirm_ms` P95 小于 500ms。
4. confirmed 后用户 final transcript 能正常展示。
5. rejected 后不污染正式对话上下文。
6. 旧 AI 音频不会在 pre-stop 后继续迟到播放。
7. Web 和软电话均能跑通最小验证。

真实电话必须补证：

1. 回声/双讲场景下误停播率可接受。
2. 真实线路下 Qwen final transcript 可用。
3. 分轨录音和事件能复盘 confirmed/rejected 原因。

## 18. 后续边界

P1 完成后，再决定是否进入 P2。

P2 暂只保留方向：

1. SIP 供应商 / 网关 AEC 验收。
2. 分轨录音样本复盘。
3. WebRTC APM NS/HPF/AEC 影子评估。
4. 音频质量指标。

P2 不进入 P1 实现。
