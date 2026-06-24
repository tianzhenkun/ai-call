# Phase A：打断与通话文本展示收口验收记录

最后更新：2026-06-24

## 1. 验收结论

当前 Web 通话链路的“打断状态落库 + 通话文本展示”已完成阶段收口。

本轮结论只覆盖浏览器 Web/LAN 验证链路、当前本地 SQLite 运行库和 Qwen Realtime 通话样本，不等同于 SIP 线路、生产并发、完整商用质检或业务语义理解全部通过。

本轮已收口：

1. AI 被用户打断后，通话文本能保留该 AI 段，并标记为 `interrupted`。
2. 前端通话文本区域能把 `speakerType=ai` 且 `segmentStatus=interrupted` 的气泡展示为 `AI（已打断）`。
3. 迟到的 `response.done`、`response_generation_invalidated` 或 response item 变化，不应再把同一句 AI 展示成重复 final 文本。
4. 打断后的旧音频块继续到达时，能通过 response/generation gate 丢弃，避免旧音频继续泄漏播放。

## 2. 验收范围

本轮验收聚焦两类问题：

1. 打断事件发生后，AI 文本状态是否准确反映为“已打断”。
2. 页面通话文本是否稳定展示，不重复、不丢失被打断 AI 段。

暂不纳入：

1. AI 在 GEO 场景中偶发“替客户说话”的角色边界问题。
2. ASR 对业务词、短句、时间表达的识别准确率问题。
3. 只展示“用户实际听到的 AI 片段”的精确截断能力。
4. SIP 线路下的回声、运营商缓冲和媒体网关行为。
5. 完整商用质检、压测和弱网验收。

## 3. 当前运行态确认

本轮验收使用当前本地运行态：

| 项 | 值 |
| --- | --- |
| 运行入口 | `192.168.0.106:19011` LAN 验证页 |
| 静态页 | `/static/ai-call/customer.html` |
| 本地库 | `/private/tmp/ai_call_ed81_local.db` |
| 进程状态 | `19011` 有 Python 进程监听；`19000` 未监听 |
| 场景样本 | `intro_geo` |
| 模型 | `qwen3.5-omni-plus-realtime` |
| ASR | `qwen3-asr-flash-realtime` |

说明：Codex 当前工具环境对 `127.0.0.1:19011` 的 `curl` 可能失败，不能单独作为服务停止的证据；本轮以后判断服务是否运行，优先以 `lsof`、进程工作目录、日志和 DB 写入为准。

## 4. 真实通话样本

### 4.1 最新样本

| call_id | 时间 | 结束原因 | 结果 |
| --- | --- | --- | --- |
| `call_328112729058590720` | 2026-06-24 10:02:50 - 10:04:34 | `web_user_end` | 打断状态和文本展示链路通过 |

关键事件统计：

| 指标 | 值 |
| --- | --- |
| `interrupt_confirmed` | 7 |
| `response_generation_invalidated` | 7 |
| `browser_audio_hold_confirmed` | 7 |
| `browser_pre_stop_confirmed` | 2 |
| `stale_audio_dropped` | 357 |

对话段落库统计：

| speaker_type | segment_status | 数量 |
| --- | --- | --- |
| `ai` | `interrupted` | 7 |
| `ai` | `final` | 2 |
| `customer` | `final` | 11 |

判定：

1. 最新样本中，多次用户插话均进入确认打断链路。
2. 被打断 AI 段已落库为 `interrupted`。
3. 用户侧已确认页面能显示 `AI（已打断）`。
4. `stale_audio_dropped` 大量出现是预期行为，表示已取消/失效 response 的旧音频块被丢弃。

### 4.2 相关样本

| call_id | 关注点 | 结论 |
| --- | --- | --- |
| `call_328105370468974592` | 同一句 AI 文本重复展示 | 已通过 response/source 关联和迟到事件处理收口 |
| `call_328109399729463296` | 短句 partial 文本后续被 ASR final 合并 | 属于 partial -> final 替换，不是短句被过滤 |
| `call_328112729058590720` | 多次打断、展示 `AI（已打断）` | 本轮主验收样本 |

## 5. 自动化验证

已执行：

```bash
node --check static/ai-call/customer.js
uv run ruff check app/services/ai_call/dialogue_service.py tests/test_ai_call_phase_a_core.py tests/test_ai_call_phase_b1_records.py
uv run pytest tests/test_ai_call_phase_b1_records.py tests/test_ai_call_phase_a_core.py -q
```

验证结果：

```text
node --check: 通过
ruff: All checks passed!
pytest: 230 passed, 8 warnings
```

说明：`ruff` 只用于 Python 文件；JavaScript 语法检查使用 `node --check`。

## 6. 实现要点

### 6.1 通话文本展示

前端通话文本展示遵循：

```text
speakerType=ai + segmentStatus=interrupted -> AI（已打断）
speakerType=ai + 其他状态 -> AI
speakerType=customer -> 用户
speakerType=human_agent -> 人工
```

这解决的是“被打断 AI 气泡没有可见标识”的问题。

### 6.2 被打断 AI 段保留

通话文本查询不再因为 AI 段是 realtime interrupted fragment 就直接隐藏。被打断 AI 段会保留在文本里，并通过 `segmentStatus=interrupted` 表达状态。

这解决的是“刚开始展示了被打断 AI，刷新或查询后又不展示”的问题。

### 6.3 迟到事件与重复文本

对 AI response 做 response id 与 source item id 关联：

1. `response_generation_invalidated` 到达时，标记当前 response/source 为 interrupted。
2. 如果 `model_response_done` 先到、invalidation 后到，后续会把已 final 的 AI 段补标为 interrupted。
3. 如果 provider 后续用新的 item id 返回同一个 response 的 done，不再生成重复 final 文本。

这解决的是“同一句 AI 文本展示两条”和“没有出现 `AI（已打断）`”的问题。

## 7. 已知边界

### 7.1 被打断文本不是精确播放截断

当前保留的是 provider 返回的 AI transcript，并用 `interrupted` 表示“这段被打断”。它不保证文本长度刚好等于用户实际听到的音频长度。

如果后续要做到“只展示用户实际听到的半句话”，需要引入播放进度与文本截断映射，不能靠当前 transcript 状态字段直接推断。

### 7.2 短句第一声仍可能有体感延迟

短句如“行”“嗯”“是的”可能先经历 browser candidate / audio hold / provider speech 确认，再进入最终打断。最新样本里主链路能确认，但短句首段不等同于每次都 0 感知延迟。

本轮不继续调整 VAD 阈值，避免为了少量短句体验把噪声误触发风险重新拉高。

### 7.3 模型角色边界未纳入本轮

最新 `intro_geo` 样本中出现过 AI 说“我们确实很关注”“我们现在最大的困惑是”这类像替客户表态的内容。

该问题当前判断更接近 prompt/上下文治理问题，不属于本轮“打断与文本展示”验收范围，后续应单独收口。

### 7.4 商用前仍需补证

当前阶段通过的是本地 Web/LAN 验证链路。商用前至少还需要补齐：

1. SIP 线路真实打断样本。
2. 弱网和外放/耳机场景对比。
3. 多通连续样本统计。
4. 被打断文本在质检、摘要、CRM 记录里的下游使用边界。
5. 角色边界和业务语义理解的独立验收。

## 8. 阶段判断

“AI 被打断后的文本状态与页面展示”可以作为当前开发阶段基线。

后续如果继续推进，不建议再围绕 `AI（已打断）` 展示做小补丁；应转向两个更高价值方向：

1. 转人工闭环和通话结束闭环继续验收。
2. 角色边界、上下文治理和业务语义理解单独设计与验收。
