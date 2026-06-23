# Phase A：打断稳定化验收报告

最后更新：2026-06-22

## 1. 验收结论

当前 Web 通话链路的打断主链已通过本轮真实通话验收。

本报告只覆盖浏览器 Web Room + LiveKit + Qwen Realtime 的当前本地链路，不等同于 SIP 线路、批量外呼、生产并发和完整商用验收通过。

## 2. 验收范围

本轮只验证通用通话质量中的三类打断场景：

1. 真人在 AI 说话中插话。
2. 敲桌等非语音声音不误触发打断。
3. 连续多次真人插话后，通话状态仍能稳定恢复。

暂不纳入：

1. 开场白刚开始前后的首句盲区优化。
2. 用户说“挂了吧”等结束语义策略。
3. SIP 线路媒体入口差异。
4. DTMF 打断。
5. 转人工后的人工坐席真实媒体接管。

## 3. 验收标准

### 3.1 真人插话

真人插话通过需要满足：

1. `interrupt_candidate` 与 `interrupt_confirmed` 成对出现。
2. `candidateNotConfirmedCount = 0`。
3. `playout_queue_flushed` 至少覆盖每次确认打断。
4. `browserToConfirmedMs` 不超过当前阶段慢确认门禁。
5. `stale_audio_dropped` 只能是 `reason=interrupt_pending`，不能出现旧音频泄漏风险。

### 3.2 非语音误触发

敲桌等非语音声音通过需要满足：

1. 不产生 `interrupt_candidate`。
2. 不产生 `interrupt_confirmed`。
3. 不触发播放队列清理。
4. 不影响 AI 正常说话和后续对话。

### 3.3 连续插话

连续插话通过需要满足：

1. 多次 `interrupt_candidate` 都能被确认或被明确忽略。
2. 确认打断后能清播放队列并丢弃旧响应音频。
3. 后续 AI 回复能继续生成，不出现状态卡死。
4. 不出现重复结束、Agent 启动失败或会话异常失败。

## 4. 本轮真实通话样本

| 场景 | call_id | 结果 | 关键指标 |
| --- | --- | --- | --- |
| 真人插话 | `call_327340846771200000` | 通过 | `interruptCandidateCount=2`, `interruptConfirmedCount=2`, `candidateNotConfirmedCount=0`, `browserToConfirmedMs=426ms`, `playoutFlushCount=2`, `verdict=normal` |
| 敲桌误触发 | `call_327342841624125440` | 通过 | `interruptCandidateCount=0`, `interruptConfirmedCount=0`, `staleAudioDroppedCount=0`, `playoutFlushCount=0`, `verdict=normal` |
| 连续真人插话 | `call_327343739469422592` | 通过 | `interruptCandidateCount=6`, `interruptConfirmedCount=6`, `candidateNotConfirmedCount=0`, `browserToProviderMs=249ms`, `providerToConfirmedMs=297ms`, `browserToConfirmedMs=447ms`, `playoutFlushCount=6`, `verdict=normal` |

说明：第二通里出现的用户转写“好的。”已由测试人员确认是真人真实说话，不是敲桌噪声误转写。

## 5. 事件链确认

真人插话和连续插话均走通以下主链：

```text
browser_user_speech_started
-> interrupt_candidate
-> interrupt_pending
-> user_speech_started
-> response_generation_invalidated
-> interrupt_audio_stop_requested
-> playout_queue_flushed
-> interrupt_audio_stop_completed
-> interrupt_confirmed
```

`stale_audio_dropped` 在真人插话和连续插话中均为预期行为。它表示进入 `interrupt_pending` 后主动丢弃旧响应音频块，避免旧音频继续播出；本轮样本未发现非 `interrupt_pending` 原因导致的旧音频泄漏风险。

## 6. 已知边界

### 6.1 开场白首句盲区

此前真实样本中出现过开场白刚开始前后，浏览器侧还未进入稳定远端播放态，导致短句未触发 `browser_user_speech_started` 的情况。

本轮验收不关闭该边界。当前决定是开场白先保持现状，后续如果需要优化，再单独设计 opening barge-in window，而不是混入本次打断主链收口。

### 6.2 SIP 线路未覆盖

本轮样本来自 Web 浏览器通话，不覆盖 SIP 线路的回声、抖动、运营商侧缓冲、编解码和媒体网关行为。

后续接 SIP 时仍应复用当前事件口径，但需要新增 SIP 真实线路样本。

### 6.3 转人工附近观测口径

连续插话样本末尾出现过 `model_response_started` 数量多于 `model_response_done` 的现象。复盘后确认它发生在模型触发 `request_handoff` 工具调用附近，属于语音回复与工具调用并行产生的观测差异，不构成本轮打断失败。

该问题应放到 Phase B3.2 转人工自动触发真实通话验收中继续收口。

### 6.4 样本量边界

本轮是三通定向真实通话样本，足以作为当前开发阶段基线，但不能替代生产前压测、弱网测试、多人噪声环境测试和 SIP 真实线路验收。

## 7. 后续建议

1. 当前不要继续堆前端 VAD 规则，避免把已通过的主链重新复杂化。
2. 下一阶段优先做 Phase B3.2 转人工自动触发的真实通话手工验收。
3. 如果后续出现“明显没打断”“明显慢”“旧音频漏出”，先按 `call_id` 复盘 `interrupt_candidate`、`interrupt_confirmed`、`stale_audio_dropped` 和对话段，不直接猜测原因。
4. SIP 接入前，把当前 Web 样本作为基线；SIP 接入后重新跑真人插话、敲桌误触发、连续插话三类样本。
