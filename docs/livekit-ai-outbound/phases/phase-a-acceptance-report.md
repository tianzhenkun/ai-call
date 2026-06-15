# Phase A：Web 端到端核心引擎验收报告

最后更新：2026-06-15

## 1. 结论

Phase A 实现已收尾，可以进入 Phase B 正式技术设计。

但这不是“已证明商用可用”的结论。Phase A 仍有两项补证项：一次修复后的 5 分钟真实多轮通话复测，以及该复测中的浏览器侧首包 `browser_first_audio_ms` p50/p90 数据。补证项不阻止 Phase B 设计，但不建议在补证前直接进入 Phase B 实现。

## 2. 验收范围

本报告只验收 Web 入口下的端到端实时通话核心链路：

```text
Browser
  -> LiveKit Room
  -> Realtime Call Agent
  -> Qwen Omni Realtime
  -> Realtime Call Agent
  -> LiveKit Room
  -> Browser
```

不覆盖真实 SIP、批量外呼、正式数据库、录音、完整转人工、并发容量和生产运维。

## 3. 验收环境

| 项 | 值 |
|---|---|
| 后端模式 | 本地 FastAPI，AI Call standalone 模式 |
| 入口 | 浏览器 Phase A 验证页 |
| 媒体层 | 开发环境 LiveKit Room |
| 模型 | `qwen3.5-omni-plus-realtime` |
| 默认音色 | `Tina` |
| 开场白 | 默认启用，内容来自服务端配置 |
| VAD | `server_vad`，`threshold=0.5`，`silence_duration_ms=800` |
| 浏览器音频约束 | `echoCancellation=true`、`noiseSuppression=true`、`autoGainControl=true` |
| 事件存储 | 内存运行态存储 |

文档不记录 API Key、LiveKit Secret、完整 Token 或生产配置。

## 4. 自动化测试

| 命令 | 结果 |
|---|---|
| `node --check static/ai-call/phase-a.js` | 通过 |
| `ruff check app/api/v1/ai_call app/services/ai_call tests/test_ai_call_phase_a_core.py` | 通过 |
| `pytest tests/test_ai_call_phase_a_core.py` | 通过，53 passed |

自动化测试覆盖 Session API、统一响应、事件查询、浏览器事件上报、指标计算、打断事件、资源释放和 Web 验证页关键脚本检查。

## 5. 手工验证记录

| call_id | 类型 | 结果 |
|---|---|---|
| `call_324827628607397888` | 真实浏览器多轮通话 | `completed`；无 `model_error`、无 `session_failed`；5 次模型响应；4 次用户说话；1 次真实打断；用户听感反馈无明显问题 |
| `call_324831477094207488` | 修复后浏览器入口烟测 | 创建会话成功，状态进入 `ready`；事件轮询支持 `afterEventId`；结束会话成功，状态进入 `completed`；未见前端控制台错误 |

真实多轮通话的模型侧首包统计：

| 指标 | 值 |
|---|---|
| 样本 | 516ms、562ms、571ms、573ms、612ms |
| p50 | 571ms |
| p90 | 约 596ms |
| max | 612ms |

上述模型侧首包已满足 Phase A 的 1 秒目标线。但浏览器侧首包在修复前采集口径不稳定，因此不能用该轮数据证明 `browser_first_audio_ms` 已达标。

## 6. 完成定义对照

| 完成定义 | 当前判断 | 说明 |
|---|---|---|
| 浏览器完成至少 5 分钟多轮端到端 S2S 对话 | 部分满足 | 已完成真实多轮通话，但已记录样本不足 5 分钟 |
| `user_speech_stopped -> browser_first_ai_audio` p50 <= 1000ms，p90 <= 1500ms | 部分满足 | 模型侧首包达标；浏览器侧首包修复后需要重新采样 |
| 用户真实打断后 AI 100-300ms 停播，旧音频队列不继续播放 | 部分满足 | 已验证一次真实打断和队列清理逻辑；仍需长通话复测补样本 |
| 背景噪声、短促附和、AI 回声不会频繁误打断 | 待补证 | 代码已有基础防护，真实噪声样本不足 |
| 会话结束、浏览器断开、模型报错时 Room、Agent、模型连接能释放 | 基本满足 | 自动化测试覆盖释放逻辑；修复后烟测确认 Room 删除路径可走通 |
| 每通会话可通过 `call_id` 查询状态、事件和指标 | 满足 | 状态、事件、指标接口可用，事件支持增量查询 |
| 自动化测试和手工验收记录完成，并更新总纲当前状态 | 满足 | 本报告生成后，总纲同步更新 |

## 7. 剩余补证项

进入 Phase B 实现前，应补齐以下证据：

1. 使用修复后的浏览器首包采集逻辑，完成一通至少 5 分钟真实多轮通话。
2. 记录 `browser_first_audio_ms` p50、p90、max，并和模型侧首包一起对照。
3. 在同一轮长通话中覆盖至少一次用户打断、一次短促附和、一次短暂停顿和一次结束会话。
4. 记录是否出现半句中断、重复叠音、长尾音、误打断或旧音频继续播放。

补证完成后，更新本报告，不需要重写 Phase A 设计文档。

## 8. 阶段判断

Phase A 的代码和核心体验已达到进入 Phase B 正式技术设计的门槛。

Phase B 正式设计可以开始，但 Phase B 实现应等上述补证项完成，或者在设计文档中明确把这些缺口列为已接受风险。
