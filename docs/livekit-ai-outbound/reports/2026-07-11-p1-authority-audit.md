# P1 Authority Audit - 2026-07-11

适用范围：19011 本地 SIP barge-in P1。

不适用范围：19012 语义分析线、P2/P3、AEC/平台降噪迁移、ASR 语义相似度、recovery/response continuation。

## 结论

当前问题不是“还差一个风扇/咳嗽/停字补丁”，而是 P1 authority 已经出现结构性发散：

1. `sip_pre_stop` 的最终执行点是单一的：`_pre_stop_sip_barge_in_candidate(...)`。
2. `sip_pre_stop` 的授权入口形式上也回到了 `_decide_sip_pre_stop_authority(...)`。
3. 但 `_decide_sip_pre_stop_authority(...)` 内部仍然是多条局部证据分支直接返回 `pre_stop`。
4. 这导致“统一入口”没有变成“统一模型”，后续每次按单通电话补一个 evidence 子类，都会继续退化成规则堆。
5. 完整 sample matrix 当前失败，不能继续用真实拨测作为主要回归路径。

下一步只能做 `P1 authority evidence lifecycle` 收口；不能继续做单点阈值或声音类别补丁。

## 当前能触发 sip_pre_stop 的入口

| 入口 | 代码位置 | 性质 | 结论 |
| --- | --- | --- | --- |
| 普通 SIP candidate | `_handle_sip_barge_in_candidate(...) -> _maybe_pre_stop_sip_barge_in_candidate(...) -> _decide_sip_pre_stop_authority(...)` | 核心路径 | 保留，但必须只走统一模型 |
| deferred candidate 后续升级 | `_maybe_upgrade_deferred_sip_pre_stop(...) -> _maybe_pre_stop_sip_barge_in_candidate(...) -> _decide_sip_pre_stop_authority(...)` | 核心路径 | 保留，用于同一 turn 的证据延续 |
| shadow 回流升级 | `_maybe_upgrade_deferred_sip_pre_stop_from_shadow(...) -> _decide_sip_pre_stop_authority(...)` | 核心路径 | 保留，但 shadow 不能绕过 echo/tail guard |
| shadow-assisted candidate | `_maybe_create_shadow_assisted_sip_candidate(... allow_pre_stop=False)` | 候选路径 | 只能建 candidate/deferred，不应直接停播 |
| 最终执行 | `_pre_stop_sip_barge_in_candidate(...)` | 执行点 | 保持单一执行点 |

## authority 内部 pre_stop 分支审计

| evidence | 当前行为 | 风险判断 | 收口方向 |
| --- | --- | --- | --- |
| `echo_guarded_turn_evidence` | echo-like 场景下满足 turn 条件直接 `pre_stop` | 可保留，但必须受 echo guard 硬门控和新鲜同 response 约束 | 放入 evidence matrix 的 `turn_lifecycle` 类 |
| `echo_guarded_local_speech` | echo-like 场景下 local micro-confirm 可直接 `pre_stop`，opening 下 defer | 高风险，容易把风扇/AI 尾音/局部短声升级 | 降级为 provisional evidence，必须再有 turn/provider/shadow 双证据 |
| `deferred_speech_episode` / `deferred_multi_candidate_turn` | 多段 deferred 后直接 `pre_stop` | 方向正确，但当前条件被多轮局部修补拉宽 | 重写为明确 episode 状态机：fresh、same response、non-periodic、non-impulse、not echo-tail |
| continuous shadow + local | shadow 连续上下文和 local 证据同时满足后 `pre_stop` | 可保留 | 必须在 echo/tail guard 之后，且 local 当前段不能是 short-only |
| shadow local duration bypass | shadow + local duration 满足后 `pre_stop` | 名字和语义都有 bypass 味道 | 改成普通 evidence row，不允许叫 bypass，不允许越过 guard |
| has shadow local speech | shadow + local speech 满足后 `pre_stop` | 可保留，但要避免 shadow 单独放权 | 要求 local 当前段达到 stable 或 episode 条件 |
| `turn_cluster` | turn cluster 满足后 `pre_stop` | 核心路径，但过往 false stop 集中过这里 | 必须显式区分 speech-like turn 和 periodic/noise turn |
| `stable_local_speech` | local fast/stable 满足后直接 `pre_stop` | 高风险，开场风扇误停与这里强相关 | opening/echo/tail/high-noise 下默认 defer；非 opening 也应受 evidence matrix 约束 |
| `single_high_confidence_burst` / `clear_short_modulated_burst` | 当前主要作为 payload，但仍影响 local/echo/local-shadow 分支 | 高风险历史遗留 | 不再作为独立授权条件，只能作为 observation annotation |

## sample matrix 当前状态

本次只读验证命令：

```bash
python tools/ai_call_p1_eval.py --sample-matrix docs/livekit-ai-outbound/p1-sample-matrix.local.example.json --fixture-only
python tools/ai_call_p1_eval.py --sample-matrix docs/livekit-ai-outbound/p1-sample-matrix.local.example.json
```

结果：

1. fixture-only：5 个样本，5 通过。
2. 完整 sample matrix：32 个样本，15 通过，17 失败。
3. 覆盖门禁失败：目标 60 个样本，当前 32 个。
4. source type 覆盖失败：`live_call` 目标 20，当前 sample 未显式标注 live_call；`synthetic` 目标 20，当前 4；`corpus_noise_mix` 目标 20，当前 1。
5. 类别覆盖失败：`opening_fan_noise`、`midcall_fan_noise`、`continuous_noise`、`impulse_noise`、`echo_guarded_near_end_speech`、`call_end` 等都不足。

关键失败类型：

1. 历史 180ms 短促误停仍在矩阵里失败：`call_333059784225075200`。
2. opening noise 误停仍在矩阵里失败：`call_333167639641690112`。
3. 真实客户插话慢停/漏停仍在矩阵里失败：多个 `must_interrupt` / `must_pre_stop_after_candidate` 样本。
4. 风扇场景仍有 unexpected pre-stop：`call_333790178132156416`、`call_333810711039770624`。

这说明当前不能把 19011 真实拨测当主验证；必须先让矩阵门禁过线。

补充：`tools/ai_call_p1_eval.py --sample-matrix ...` 现在会在每个样本输出里标明 `evaluationSource`：

1. `audio_fixture`：本地可复放音频 fixture。
2. `fixture_report`：本地构造的事件 fixture。
3. `api_history`：从 19011 API 读取的历史通话事件。

因此完整矩阵中的历史失败只能说明“旧通话事件中存在这些失败形态”，不能直接证明“当前未重启代码仍失败”。当前实现验收应优先看 `fixture-only` 和后续 authority replay fixture；历史 `api_history` 样本用于提醒必须导出成可复放 fixture，而不是继续要求人工拨测。

## 需要改成的模型

把 `_decide_sip_pre_stop_authority(...)` 从“if 分支裁判”改成“证据矩阵裁判”：

```text
observation
-> guards
-> evidence rows
-> lifecycle state
-> decision
```

### 1. guards 先行

这些 guard 必须先于所有 evidence：

1. no playback target -> defer
2. post speech tail guard -> defer
3. opening guard -> defer unless provider/shadow+stable turn confirms
4. echo guard -> defer unless explicit echo-safe turn evidence
5. non-recoverable quality rejection -> defer/ignore
6. stale/deferred response mismatch -> expired

任何 local short、shadow、episode 都不能绕过这些 guard。

### 2. evidence 只表达事实，不直接停播

 evidence row 只输出：

1. evidence kind
2. confidence tier
3. freshness
4. same response/generation
5. duration/voiced/wall/gap
6. acoustic shape: SNR、RMS range、direction changes、large jumps
7. risk tags: opening、echo-like、tail、elevated noise、impulse-like、periodic-like

### 3. decision 只在最后统一判断

允许 `pre_stop` 的条件收敛为：

1. stable local speech 达到 `pre_stop_min_duration_ms`，且无 opening/echo/tail/high-noise risk。
2. turn cluster 是新鲜、同 response、speech-like 的连续证据。
3. deferred episode 在同一 turn 内形成 speech-like progression，不是单纯 burst 数堆积。
4. local + shadow 双证据，但不能绕过 echo/tail/opening guard。
5. provider `speech_started` 佐证 deferred 或 confirmed pre-stop。

不允许 `pre_stop` 的条件：

1. 单个 180ms short evidence。
2. 仅靠 `single_high_confidence_burst`。
3. 仅靠 `clear_short_modulated_burst`。
4. 仅靠连续有声或 burst 数。
5. opening 风扇、AI 尾音、客户刚说完尾音窗口内的 local-only evidence。

## 实施顺序

1. 先补红测：覆盖当前矩阵失败里的代表性失败，而不是新增某一通电话的特例。
2. 引入 authority evidence row / guard result / final decision 的小模型。
3. 把 `_decide_sip_pre_stop_authority(...)` 改成 guard-first + evidence-matrix + final-decision。
4. 降级 `single_high_confidence_burst` / `clear_short_modulated_burst` 为 annotation。
5. 收紧 `echo_guarded_local_speech` 和 `stable_local_speech`：不能在 opening/echo/high-noise 下 local-only 直接授权。
6. 让 deferred/echo-guarded episode 明确过期、升级、拒绝，不用历史峰值补票。
7. 跑 focused pytest。
8. 跑完整 sample matrix，必须从当前 17 failed 明显下降；失败不归零前不重启 19011 让用户拨测。
9. 再跑 ruff、py_compile、diff check。
10. 最后按 baseline 重启 19011，并只做少量真实通话验收。

## 当前禁止事项

1. 不新增风扇/咳嗽/拍手/敲桌子的单独例外。
2. 不继续按单通电话调单个阈值。
3. 不把 FSMN 接入 P1 主链。
4. 不处理 recovery/response continuation。
5. 不碰 19012。
6. 不在完整矩阵明显过线前要求继续真实拨测。
