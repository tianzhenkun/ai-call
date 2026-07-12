# P1 SIP barge-in sample intake

本文件用于收集后续 P1 打断样本线索。记录者不需要剪音频，只需要提供
`call_id`、大概时间和期望结果；后续再由离线 replay 工具导出客户轨/AI 轨并固化 fixture。

## 记录原则

- 只记录真实问题或明确边界场景，不为了凑数量重复拨测同一句话。
- `should_stop` 表示客户真实插话，应快速停播。
- `should_not_stop` 表示背景中文、电视声、旁人说话、AI 回声、风噪等，不应停播。
- `should_defer` 表示可以先形成 candidate，但 authority 应等待更强证据。
- 没有分轨音频或时间对齐前，不把结论写成 runtime 阈值修改依据。
- 不在文档里粘贴密钥、客户隐私文本、完整录音 URL；只写必要定位信息。

## 最小模板

| field | example | required | note |
| --- | --- | --- | --- |
| `call_id` | `call_333517350307270656` | yes | AI Call 记录 ID |
| `approx_time` | `08:00:00` 或 `56.4s-68.4s` | yes | 可写绝对时间或通话内偏移 |
| `expected` | `should_stop` / `should_not_stop` / `should_defer` | yes | 期望行为 |
| `scene` | `客户说别说了挂了吧` | yes | 一句话描述，不要粘贴敏感长文本 |
| `why` | `客户明确要求停止` / `电视中文声不是客户` | yes | 为什么该停或不该停 |
| `observed` | `停慢了约3秒` / `误停` / `未停` | no | 实际体验 |
| `speaker_context` | `客户本人` / `旁人` / `电视` / `多人` / `AI外放回声` | no | 帮助分类 source authority |
| `noise_context` | `车内` / `办公室` / `会议室` / `安静` | no | 帮助分类噪声 |

## 建议优先级

优先收集这些中文负样本，因为它们决定能不能安全优化断续短句：

1. `should_not_stop`: 中文电视/视频/外放声音进入客户轨。
2. `should_not_stop`: 旁边有人说中文，但不是客户和 AI 对话。
3. `should_not_stop`: 多人环境、会议室背景聊天。
4. `should_defer`: AI 正在播报时，客户轨里有 AI 外放回声。
5. `should_stop`: 客户明确说“停一下”“别说了”“挂了吧”等短命令。

## 进入 replay 的条件

样本进入 benchmark 或 authority fixture 前至少满足：

- 能定位到客户轨音频窗口。
- 能判断该窗口是否有 AI 轨/AI playout 回声。
- 正样本能标出稳定声学起点；如果只有短促早期 burst，要在报告里说明。
- 负样本能说明 source authority：为什么这段人声不是客户授权插话。

## 当前无新样本时的门槛

在没有新增真实中文负样本前：

- 不降低全局 RMS/SNR 阈值。
- 不把 FSMN 或 WebRTC+FSMN agreement 直接接入停播主链。
- 可以继续加入 public corpus / synthetic 样本，但只能作为 offline/shadow evidence。
- 任何 detector/authority 改动都必须通过
  `docs/livekit-ai-outbound/p1-vad-benchmark.local.example.json` 的 `benchmarkGates`，
  主 sample matrix 的 `fixture-only` gate，以及
  `real_zh_call_333517_bieshuole_gualeba` 独立 authority fixture。
