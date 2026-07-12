# 2026-07-12 P1 sample matrix failure triage

本报告只拆解 `docs/livekit-ai-outbound/p1-sample-matrix.local.example.json`
里仍为 `api_history` 的失败样本。它们记录的是历史通话结果，不等同于当前
authority 代码 replay 结果。

## 当前结论

- fixture replay 已经可以作为当前代码回归入口；`api_history` 仍用于保留旧失败证据。
- 不再把剩余 `api_history` 红点逐条转换为 fixture；重复失败只保留代表 fixture。
- 只有独立失败机制、准备修改 authority 的案例，或能提供完整音频和时间对齐证据的案例，才继续转换。
- 不能继续按单次拨测体验调阈值；先把代表性失败变成可复放样本。

## 红点归类收口

完整矩阵当前有 17 个 `api_history` 红点，按是否还需要转换归类如下：

| classification | count | handling |
| --- | ---: | --- |
| `covered_duplicate` | 11 | 6 个历史误停和 5 个历史慢停已有代表 authority fixture；保留历史红点，不重复转换 |
| `insufficient_evidence` | 4 | 缺可靠语音起点、分轨窗口或一对一 pre-stop 对齐；不根据事件记录硬造 fixture |
| `authority_fixture_resolved` | 1 | `call_333517350307270656_bieshuole_gualeba` 已由真实音频 replay 提炼成独立 fixture，当前 authority replay 已通过，不进入主 fixture gate |
| `independent_mechanism` | 1 | `repeated_bieshuole` 是连续多段 echo-guarded speech 的独立机制；只有准备修改该路径时才先补红 fixture |

`insufficient_evidence` 包括：

- `call_333167639641690112_naxieren_must_interrupt`
- `call_333513524133138432_nihao_haode_must_interrupt`
- `call_333513524133138432_bieshuole_must_interrupt`
- `call_333517350307270656_tingyixia_must_interrupt`

`call_333517350307270656_bieshuole_gualeba_must_interrupt` 不再归为
`insufficient_evidence`：它已经有客户/AI 对齐音频，并被提炼为独立 fixture。
当前 authority replay 已通过；下一阶段不要根据事件记录硬造其它仍缺音频证据的
fixture。

## 失败分组

| group | count | meaning | next action |
| --- | ---: | --- | --- |
| `unexpected_pre_stop` | 6 | 历史上误停，随后被 clean window reject | 优先转 fixture，验证当前 authority 是否已 defer |
| `pre_stop_too_slow` | 8 | 历史上最终停了，但晚于期望窗口 | 需要声学窗口/候选起点对齐，优先转 episode fixture |
| `missing_pre_stop` | 3 | 历史上没有 local pre-stop | 必须结合音频或 transcript 对齐，不能只靠事件补票 |

## Unexpected Pre-Stop

| sample | category | fixture route |
| --- | --- | --- |
| `call_333059784225075200_single_short_burst_1_must_defer` | `single_short_noise` | converted to `fixture_single_short_noise_high_snr` |
| `call_333059784225075200_single_short_burst_2_must_defer` | `single_short_noise` | converted to `fixture_single_short_noise_low_rms` |
| `call_333167639641690112_opening_noise_1_must_defer` | `opening_noise` | converted to `fixture_opening_noise_high_snr` |
| `call_333167639641690112_opening_noise_2_must_defer` | `opening_noise` | converted to `fixture_opening_noise_high_peak` |
| `call_333790178132156416_opening_fan_must_not_interrupt` | `opening_fan_noise` | converted to `fixture_opening_fan_echo_guarded_local_micro_confirm` |
| `call_333810711039770624_midcall_fan_must_not_interrupt` | `midcall_fan_noise` | converted to `fixture_midcall_fan_echo_guarded_turn_elevated_noise` |

## Slow Or Missing Interrupt

| sample | category | fixture route |
| --- | --- | --- |
| `call_333167639641690112_zenmezuo_must_interrupt` | `near_end_speech` | converted to `fixture_slow_zenmezuo_strong_short` |
| `call_333513524133138432_nihao_haode_must_interrupt` | `near_end_speech` | audio fixture preferred; two utterances share one historical pre-stop |
| `call_333513524133138432_bieshuole_must_interrupt` | `near_end_speech` | audio fixture preferred; same call/window coupling as above |
| `call_333533194026348544_xing_haode_liaojiele_must_interrupt` | `echo_guarded_near_end_speech` | converted to `fixture_echo_guarded_near_end_second_frame` |
| `call_333551735128162304_repeated_bieshuole_must_interrupt` | `repeated_deferred_echo_guarded_speech` | audio fixture or authority decision required; preloaded echo-guarded state still defers under current authority |
| `call_333790178132156416_ting_candidate_must_pre_stop_after_candidate` | `short_command_candidate` | converted to `fixture_ting_then_strong_short_followup` |
| `call_333810711039770624_ting_candidate_must_pre_stop_after_candidate` | `short_command_candidate` | converted to `fixture_ting_deferred_episode_micro_confirmed` |
| `call_333810711039770624_haode_keyi_candidate_must_pre_stop_after_candidate` | `short_ack_candidate` | converted final transition to `fixture_haode_keyi_deferred_episode_final`; early echo defers covered by `aiPlaybackFrames` test |
| `call_333167639641690112_naxieren_must_interrupt` | `near_end_speech` | audio fixture required; event-only evidence does not locate actual speech start |
| `call_333517350307270656_tingyixia_must_interrupt` | `near_end_speech` | audio fixture required; event-only path remained deferred |
| `call_333517350307270656_bieshuole_gualeba_must_interrupt` | `near_end_speech` | converted to independent fixture `phase-e-p1-authority-red-fixtures-call_333517350307270656-2026-07-12.json`; current authority now pre-stops after AI recedes |

## This Pass

Converted all six `unexpected_pre_stop` failures into authority fixtures:

- `fixture_single_short_noise_high_snr`
- `fixture_single_short_noise_low_rms`
- `fixture_opening_noise_high_snr`
- `fixture_opening_noise_high_peak`
- `fixture_opening_fan_echo_guarded_local_micro_confirm`
- `fixture_midcall_fan_echo_guarded_turn_elevated_noise`

Current replay behavior for that case:

- single short noise fixtures: `candidate_without_pre_stop`
- opening noise fixtures: `candidate_without_pre_stop`
- opening fan fixture: `candidate_without_pre_stop`
- mid-call fan fixture: `candidate_without_pre_stop`

This means the historical failures remain in `api_history`, but current authority replay
now has regression fixtures proving these representative noise cases defer.

Converted five `pre_stop_too_slow` paths into authority fixtures:

- `fixture_slow_zenmezuo_strong_short`: strong short local speech pre-stops at 180ms.
- `fixture_echo_guarded_near_end_second_frame`: echo-guarded near-end speech pre-stops at 360ms.
- `fixture_ting_deferred_episode_micro_confirmed`: first candidate defers, deferred episode pre-stops at 360ms.
- `fixture_ting_then_strong_short_followup`: first candidate defers, strong short follow-up pre-stops at 300ms.
- `fixture_haode_keyi_deferred_episode_final`: preloaded three-burst deferred episode pre-stops at the final candidate.

These five fixtures verify current authority behavior with event-derived metrics and
compressed episode offsets. They do not prove the original call audio speech-start
alignment; rows that depend on true utterance boundaries still need audio fixtures.

Added authority-fixture support for preloaded `turn.deferredEpisode` state and a focused
regression. This is only valid when the preloaded values represent state before the
current observation. `haode_keyi` now has a final-transition fixture using the live-like
three-burst deferred episode state; the earlier two echo-guarded defers are covered by the
separate `aiPlaybackFrames` observation-episode test.

Added explicit audio-to-authority replay for `audioFixtures.*.authorityReplay=true`.
This path feeds detector observations from wav/PCM frames through
`RealtimeCallAgentRunner._handle_sip_barge_in_candidate`, so it exercises the same
candidate/defer/pre-stop authority path instead of detector-only synthetic pre-stop events.

Detector-only audio replay for `/private/tmp/ai_call_p1_audio_fixtures` was `1/5`:

- pass: `single_short_noise_must_defer`
- fail: opening noise and mid-call fan noise still produce detector pre-stop
- fail: near-end speech is too slow relative to speech start
- fail: one short-command sample misses the expected candidate window

With `authorityReplay=true` on the same five wav fixtures, and explicit
`openingStarted/opening` context on the opening clip, the current runner authority result
is `3/5`:

- pass: `single_short_noise_must_defer` defers with `awaiting_pre_stop_authority`.
- pass: `opening_noise_must_defer` no longer pre-stops once opening context is present.
- pass: `midcall_fan_noise_must_not_interrupt` no longer produces pre-stop on this clip.
- fail: `near_end_speech_must_interrupt` still pre-stops too late relative to speech start.
- fail: `short_command_candidate_must_pre_stop_after_candidate` misses the matrix candidate
  timestamp and needs candidate-time realignment from audio replay windows.

Use detector-only replay to compare VAD behavior and `audio_authority_fixture` replay to
decide whether a failure is still in the current authority path.

The `near_end_speech_must_interrupt` failure above was caused by the old fixture marking
the clip boundary as `speechStartTime`. The real wav has short early bursts, while stable
speech begins around `780ms`. After re-aligning `speechStartTime` and `vadWindows` to that
stable acoustic onset, the same audio authority replay passes with
`speechStartToPreStopMs=380ms` under the `500ms` gate. The export tool now requires a
stable acoustic onset before writing `speechStartTime` / `candidateTime`, so this case
should be re-exported instead of treated as a runtime slow-stop regression.

The `short_command_candidate_must_pre_stop_after_candidate` audio clip does not contain a
stable acoustic onset under the export detector; it is a short early burst followed by
quiet audio. The export tool now rejects future `must_interrupt` /
`must_pre_stop_after_candidate` fixtures when no stable acoustic speech can be located,
instead of writing `candidateTime` at the clip boundary. This `ting` case needs a wider
audio slice or transcript-backed evidence before it can be used as a positive fast-stop
fixture.

`call_333810711039770624_haode_keyi_candidate_must_pre_stop_after_candidate` was exported
as a wider customer slice (`84000-91000ms`) with an aligned AI track. The exporter now has
`--include-ai-track`, writes `aiWavPath`, and the audio authority replay feeds per-frame AI
RMS into the runner echo guard. With those exported tracks and `authorityReplay=true`, the
fixture pre-stops about `220ms` after the first stable acoustic candidate, so the current
authority can authorize this utterance when the customer/AI frames are aligned from the
recorded tracks.

That does not fully invalidate the historical live failure. The original events show early
defers at `2026-07-10T03:26:04.530974Z` and `2026-07-10T03:26:06.151207Z` because the
runtime echo guard saw AI playout RMS above the uplink at those exact trigger frames, then
only pre-stopped at `2026-07-10T03:26:08.092177Z` after the deferred episode accumulated
three bursts. The remaining gap is therefore replay fidelity: a recorded-track fixture is
useful, but a final fix also needs event-level playback timing.

Authority fixtures now support `aiPlaybackFrames`, a per-offset AI playout RMS timeline.
This lets observation-episode fixtures preserve `_last_ai_audio_rms_dbfs` /
`_last_ai_audio_published_at` over time and reproduce the two early
`sip_ai_playback_echo_deferred` decisions without hand-writing `recentAiAudio` on each
observation. The final transition is now covered by
`fixture_haode_keyi_deferred_episode_final`, which carries the accumulated
`turn.deferredEpisode` state into the final candidate and pre-stops without changing
runtime thresholds.

`call_333517350307270656_bieshuole_gualeba_must_interrupt` now has an independent
authority fixture distilled from real customer/AI audio replay:
`phase-e-p1-authority-red-fixtures-call_333517350307270656-2026-07-12.json`.
The first stable candidate appears at `5220ms` and is deferred by
`awaiting_ai_playback_echo_guard` because uplink RMS is about `9.47dB` below AI playout.
The later stable burst reaches only `220ms`, while the two-burst episode totals about
`400ms`, RMS range `5.63dB`, and max SNR `16.21dB`. Current authority now uses
`ai_receded_compact_two_burst_turn` evidence and pre-stops `1660ms` after the first
candidate, while preserving the first echo-guard defer. This fixture is intentionally
outside the main fixture gate and should be kept as a focused regression when changing
echo guard / deferred-episode authority.

`fixture-only` coverage is now 22/22. The remaining red rows in the full matrix are
still historical `api_history` rows or cases that need audio fixtures or an explicit
authority decision before changing runtime logic.
