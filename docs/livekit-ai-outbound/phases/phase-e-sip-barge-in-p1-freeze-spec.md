# SIP Barge-In P1 Freeze Spec

状态：P1 freeze candidate；2026-07-13 已按 fixture / replay 证据更新验收口径。

适用范围：19011 本地 SIP barge-in P1。

不适用范围：19012 语义分析线、P2/P3、平台级媒体治理、ASR 语义相似度、实时 provisional 字幕、dialogue 表结构调整。

## 1. 为什么先冻结

过去几周 P1 打断优化已经出现反复摆动：

1. 放宽本地证据时，开场环境声、短促冲击声、AI 尾音容易误停。
2. 收紧本地证据时，真实客户插话又容易漏停或慢停。
3. 继续按单通真实电话体验调阈值，会在 false stop 和 missed/slow stop 之间来回切换。

因此，下一步不能继续做“某一种声音的例外规则”或“某一个阈值的临时修正”。必须先把 P1 的决策边界冻结下来，再按可回放样本矩阵验证实现。

## 2. 当前事实

最近通话已经证明两件事可以同时成立：

1. P1 不是完全听不到客户说话。
2. P1 也不是只差一个更激进的 VAD 阈值。

最新已分析通话 `call_333533194026348544`：

1. SIP 链路已通，通话正常完成。
2. `sip_pre_stop` 只有 1 次，且这次和 provider `user_speech_started` 基本对齐，说明授权后停播链路本身可以很快。
3. `sip_interrupt_rejected` 为 0，说明误停已经被明显压住。
4. 仍存在真实客户语音慢停或漏停：早期 candidate 因 echo guard / deferred 被压住，证据没有在同一 turn 内正确延续，后面才触发 pre-stop。

结论：当前主矛盾已经从“本地检测太敏感”转成“authority 对 deferred/echo-guarded 证据的生命周期处理不稳定”。这不是单纯 RMS/SNR 阈值问题，也不是单纯换一个 VAD 模型的问题。

2026-07-13 补充事实：

1. 最新两通 `call_334882377875877888`、`call_334885037324660736` 已沉淀为红点样本池，不再作为继续盲调阈值的理由。
2. 风扇/噪声误停已经按 authority 噪声门控收口，并由 5 个 latest-call negative controls 锁住。
3. 中文短句在 single 180ms decision-point authority fixture 中仍有 4 个红点；这类红点只说明“单帧快照证据太薄”，不代表应该放宽全局阈值。
4. 同一批中文短句转成客户轨 audio authority replay 后，4 个样本均能在 60ms 内 pre-stop，说明真实连续音频证据可以通过现有 authority 路径。
5. `shadowObservations` 已进入离线 evaluator，可用于把后续真实 FSMN / WebRTC shadow 证据回放进 authority；但当前最新两通没有真实 shadow 事件，不能宣称 FSMN 已验证 live 主链。

## 3. 冻结问题定义

P1 只解决一个问题：

```text
客户在 AI 播放期间真实插话时，本地 SIP 先停 AI，不再等 Qwen user_speech_started。
```

同时必须满足：

```text
候选成立但证据不足时，只能 candidate/deferred，不能直接 pre-stop。
```

也就是说：

1. `candidate` 表示“听到了一个可能相关的声音窗口”。
2. `deferred` 表示“声音像客户插话，但证据还不够安全，先观察”。
3. `pre-stop` 表示“已经授权先停 AI”。
4. `sip_pre_stop` 是 pre-stop 决策落到事件日志里的事件名。

P1 的核心不是让 detector 更会“听见声音”，而是让 authority 更稳定地判断“什么时候可以为这个声音承担先停 AI 的责任”。

## 4. 停止继续做的事

以下做法从本规格生效后停止：

1. 不再按单通电话主观体验直接调 RMS、SNR、duration 阈值。
2. 不再为风扇、咳嗽、喘气、拍手、敲桌子、键盘声单独堆场景例外。
3. 不再让 180ms 左右 single-short 证据直接授权 pre-stop。
4. 不再让 shadow evidence、local short evidence 绕过 echo/tail guard。
5. 不再把连续有声等同于连续人声。
6. FSMN 不进入 P1 实时主链；保留为 offline benchmark / shadow 对照。
7. recovery/response continuation 暂不混入 authority 改造；先单独收敛“是否授权 pre-stop”的根因。
8. 不再让用户用一通通真实拨测作为主要回归方式。

## 5. 必须统一的模型边界

P1 主链按下面边界收敛：

1. Detector 只产出 observation / candidate，不直接停播。
2. `_decide_sip_pre_stop_authority(...)` 是唯一 pre-stop 授权入口。
3. 所有 `sip_pre_stop` 都必须能追溯到 authority 的单一决策结果。
4. 短促声学证据只能先进入 candidate/deferred，除非后续形成稳定人声或被 provider 佐证。
5. echo/tail guard 是硬门控，不能被 single burst、shadow burst、local short evidence 绕过。
6. turn cluster 只能由新鲜、连续、同一 response 下的 speech-like 证据组成。
7. 已过期 deferred candidate、历史峰值、连续冲击声、周期噪声不能补票升级成 pre-stop。
8. provider `speech_started` 可以升级 deferred 或确认 pre-stop，但 P1 不能重新退化成“必须等 provider 才停”。
9. pre-stop 后必须进入 clean window，并在 confirmed / rejected 中收口。

## 6. 证据生命周期

authority 必须显式管理一段声音从出现到消失的生命周期：

```text
observation
-> candidate
-> deferred 或 pre-stop
-> confirmed / rejected / expired
```

关键规则：

1. `candidate` 不能无限保留，过期后必须重新形成新鲜证据。
2. `deferred` 不是丢弃。对真实客户插话，它应该允许同一 turn 内后续稳定证据升级。
3. `deferred` 也不是蓄力。对风扇、咳嗽串、拍手串、键盘声、桌面冲击，它不能只靠持续时间或 burst 数升级。
4. echo guard 压住的真实客户候选，需要在同一 response 下保留可解释的 pending evidence，而不是直接消失。
5. 每次升级、过期、拒绝都必须写明 reason，方便 replay 和矩阵报告定位。

## 7. 第一批硬样本

实现前，样本矩阵必须至少覆盖这些失败和正向窗口：

1. `call_333059784225075200`：大量 180ms 左右短促误停，验证 single-short 不得直通 pre-stop。
2. `call_333167639641690112`：开场环境声误停和真实插话漏停同时存在，验证 opening guard 与 must-interrupt 不互相吞噬。
3. `call_333513524133138432`：近期客户语音漏停样本，验证收紧后不能只剩不误停。
4. `call_333517350307270656`：近期 `customer_end` 附近样本，验证尾音和真实续说边界。
5. `call_333533194026348544`：最新 echo-guard/deferred 后慢停样本，验证 evidence lifecycle。
6. 已知健康的“你好”“挂了吧”窗口，验证真实短句仍能快速 pre-stop。

样本类别必须包含：

1. `must_interrupt`
2. `must_not_interrupt`
3. `must_defer`
4. `must_confirm_or_reject`

## 8. 验收门槛

实现必须同时满足误停和漏停两侧约束：

1. `must_not_interrupt` 窗口内不得出现非预期 `sip_pre_stop`。
2. `must_defer` 窗口内只能 candidate/deferred/expired，不得直接 `sip_pre_stop`。
3. `must_interrupt` 窗口内，真实客户插话必须在保守上限 600ms 内 pre-stop。
4. local + shadow 双证据、或 local + provider 佐证的真实插话，目标 350ms 内 pre-stop。
5. `call_333533194026348544` 这类慢停窗口不得再出现数秒级延迟。
6. 噪声处理的正常结果应该是 defer/ignore/expired，不应该依赖先 `sip_pre_stop` 再 rejected。
7. pre-stop 后必须在 clean window 内 confirmed 或 rejected。
8. rejected 不展示客户文本、不写上下文、不触发业务动作。
9. `stale_audio_dropped` 不能重新变成大量堆积风险。

这些门槛没有通过前，不重启 19011 做真实通话验证。

### 8.1 2026-07-13 freeze gates

当前 P1 freeze candidate 必须用下面三层证据一起判断，不允许只拿单通拨测体验下结论：

1. 默认主矩阵：`docs/livekit-ai-outbound/p1-sample-matrix.local.example.json`
   - 验收：fixture-only `23/23` 通过。
   - 用途：每次代码变更都应该优先跑，证明既有 P1 样本没有回退。
2. 最新两通 authority fixture pairs：`docs/livekit-ai-outbound/reports/phase-e-p1-authority-fixture-pairs-calls_334882_334885-2026-07-13.json`
   - 当前结果：`10` 个样本中 `6` 个通过、`4` 个失败。
   - 验收解释：5 个风扇/噪声 negative controls 必须全绿；4 个中文 single-snapshot positive red 保留为诊断红点，不作为放宽阈值的阻塞项。
3. 本地音频 authority probe：`docs/livekit-ai-outbound/reports/phase-e-p1-audio-authority-probes-call_334885-2026-07-13.local.example.json`
   - 当前结果：4 个中文短句 `4/4` 通过，`candidateToPreStopMs=60`。
   - 用途：证明中文短句在真实连续客户轨音频证据下能快停。
   - 限制：依赖 `/tmp` 本地再生成音频，不进入默认主矩阵，不提交录音文件。

配套工程验证：

1. `tests/test_ai_call_interrupt_offline_analysis.py` focused pytest 必须通过。
2. `tests/test_ai_call_phase_a_core.py -k 'call_end or no_barge or low_trust or short_overlap'` 必须通过，用来覆盖明确客户结束意图、no-barge 尾音和低可信转写边界。
3. `tools/ai_call_p1_eval.py` 必须支持 `authorityFixtures.*.shadowObservations` 回放，但 shadow 证据只能来自真实 shadow 事件或明确构造的 fixture，不能口头补证。
4. `py_compile` 和 `git diff --check` 必须通过。

固定验收入口：

```bash
.venv/bin/python tools/ai_call_p1_freeze_acceptance.py
```

该命令会一次性跑：

1. 默认主矩阵 fixture-only gate。
2. 本地 audio authority probe gate。
3. 最新两通 authority fixture pairs gate，并只允许 4 个已登记的 single-snapshot 诊断红点。
4. offline focused pytest。
5. Phase A call-end focused pytest。
6. `ruff check --no-fix`。
7. `py_compile`。
8. `git diff --check`。

freeze candidate 的当前结论：

1. 风扇/噪声 false stop 可以按当前 authority 噪声门控冻结。
2. 中文短句快停可以按 audio authority replay 行为冻结。
3. single-snapshot 中文红点保留为诊断项，不再驱动全局阈值放宽。
4. FSMN / WebRTC 只作为 shadow/offline evidence 进入样本矩阵，暂不替代 live authority。

## 9. 验证顺序

实现阶段必须按这个顺序推进：

1. 先写失败回归和样本矩阵。
2. 跑 focused pytest。
3. 跑 replay / shadow / offline interrupt 工具。
4. 跑 `tools/ai_call_p1_eval.py --sample-matrix ...`。
5. 跑 ruff。
6. 跑 `py_compile`。
7. 跑 diff check。
8. 阅读 `docs/livekit-ai-outbound/p1-local-test-baseline.md`。
9. 确认 19012 没有被停、改、共用 LiveKit 状态。
10. 重启 19011。
11. 最后只做少量真实通话验收。

2026-07-13 起，少量真实通话只用于发现新红点和做最终 smoke，不作为主要调参手段。新问题必须先转成 fixture、audio probe 或 shadow observation，再决定是否改代码。

## 10. 下一步允许做什么

评审通过后，只允许进入一条实现线：

```text
P1 authority evidence lifecycle
```

实现范围：

1. 补齐样本矩阵和失败回归。
2. 收敛 `_decide_sip_pre_stop_authority(...)` 的授权条件。
3. 删除或降级短路直通 pre-stop 的 burst 分支。
4. 让 deferred/echo-guarded 证据在同一 turn 内有明确升级、过期、拒绝路径。
5. 输出稳定、可解释的 authority decision reason。

不在这一轮处理：

1. recovery 长话术重复。
2. response continuation。
3. 语义分析。
4. FSMN 实时主链化；本阶段只保留 offline benchmark / shadow 对照价值。
5. P2/P3 媒体治理。

2026-07-13 freeze candidate 后，下一步只允许做三类事：

1. 把真实 FSMN / WebRTC shadow 事件接入离线样本矩阵，和当前 authority 做横向比较。
2. 为本地 audio authority probe 制定隐私合规的持久化策略；未确认前，只保留 `.local.example.json` 和再生成命令。
3. 把上述 gates 整理成固定验收命令，减少人工漏跑。

## 11. 非目标

以下事情不属于本次 P1 freeze：

1. 不把 FSMN / WebRTC 直接接进 live pre-stop 主链。
2. 不新增按风扇、咳嗽、键盘声等命名的 live 特判。
3. 不提交录音文件、OSS URL、object name、密钥、手机号或 hash。
4. 不修改 19012 语义分析线。
5. 不以“用户再打一通”为主要验收方式。

## 12. 评审结论

本规格通过后，P1 可以进入 freeze candidate：

1. 风扇/噪声 false stop 按 latest-call negative controls 验收。
2. 中文短句快停按本地 audio authority replay 验收。
3. 后续不是继续凭感觉调参，而是按“新红点 -> fixture/audio/shadow -> matrix -> 少量真实 smoke”的顺序推进。
