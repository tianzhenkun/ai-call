# P1 Latest Two Calls Red Sample Pool

## Context

This report captures the two latest 19011 SIP P1 calls as an offline red sample
pool. It is intentionally not a green regression gate and does not justify
runtime patching from one call.

Source calls:

- `call_334882377875877888`
- `call_334885037324660736`

Fixture matrix:

- `docs/livekit-ai-outbound/reports/phase-e-p1-red-sample-pool-calls_334882_334885-2026-07-13.json`

The fixture file stores only compact P1 evaluator inputs:

- SIP interrupt candidate / pre-stop / defer / reject / confirm events
- provider `user_speech_started` events
- call-end intent and scheduled end events
- session ending/completed events
- compact dialogue segments

It omits orchestration events, phone hashes, recording object names, provider
configuration payloads, and unrelated call lifecycle noise.

## Fixture-Only Replay

Command:

```bash
python tools/ai_call_p1_eval.py \
  --sample-matrix docs/livekit-ai-outbound/reports/phase-e-p1-red-sample-pool-calls_334882_334885-2026-07-13.json \
  --fixture-only
```

Observed result:

```text
p1_sample_matrix samples=12 passed=3 failed=9 missingReports=0
coverage status=pass failures=0
```

The non-zero exit code is expected for this red sample pool. Coverage passes,
which means the matrix structure and category counts are complete; sample
failures are the triage evidence.

## Confirmed Patterns

### Repeated fan/noise false pre-stop

Five `must_defer` samples currently fail with `unexpected_pre_stop`:

- `call_334882_fan_echo_guarded_false_pre_stop_must_defer`
- `call_334882_deferred_episode_false_pre_stop_must_defer`
- `call_334882_elevated_noise_compact_false_pre_stop_must_defer`
- `call_334882_sparse_multi_candidate_false_pre_stop_must_defer`
- `call_334885_clear_short_modulated_false_pre_stop_must_defer`

These represent the product problem the user reported: fan/noise-like bursts can
still acquire enough local authority to stop AI playback, then get rejected as
noise around 500 ms later.

### Chinese short utterance slow pre-stop

Four `must_pre_stop_after_candidate` samples currently fail with
`pre_stop_too_slow`:

- `好的。`
- `特价率。`
- `好的，知道了。`
- `我要挂了吧挂了吧。`

The candidate appears, but authority waits too long or requires later evidence.
This is distinct from the fan/noise false-stop problem and should not be fixed
by simply relaxing all thresholds.

### Call-end scheduling is not the main blocker

Two call-end intent samples pass:

- `我要挂了吧。`
- `挂了吧。`

The intent-to-schedule latency is already within the current 1000 ms gate. The
remaining issue is earlier customer speech interruption latency, not the final
call-end scheduling path.

## Decision

Do not modify runtime authority from these calls directly.

The next engineering step is to promote repeated patterns into smaller
authority/audio fixture pairs:

1. Fan/noise false pre-stop negative controls must stay `must_defer`.
2. Chinese short utterance positive controls must become fast
   `must_pre_stop_after_candidate`.
3. Each change must improve the red pattern without regressing existing green
   fixture-only matrix coverage.

Only after those fixtures are stable should FSMN/WebRTC/shadow evidence be
compared against the same sample pool.

## Authority Fixture Pairs

Decision-point authority fixtures were added in:

- `docs/livekit-ai-outbound/reports/phase-e-p1-authority-fixture-pairs-calls_334882_334885-2026-07-13.json`

These fixtures intentionally do not store recording URLs, object names, phone
hashes, or audio files. They distill each red point into the authority inputs
needed to decide whether to defer or pre-stop.

Command:

```bash
.venv/bin/python tools/ai_call_p1_eval.py \
  --sample-matrix docs/livekit-ai-outbound/reports/phase-e-p1-authority-fixture-pairs-calls_334882_334885-2026-07-13.json \
  --fixture-only
```

Initial observed result before the authority noise-risk fix:

```text
p1_sample_matrix samples=10 passed=2 failed=8 missingReports=0
coverage status=pass failures=0
```

Initial red authority decisions:

- Fan/noise negative controls: 4 of 5 still fail with `unexpected_pre_stop`.
- Chinese short-utterance positives: 4 of 4 still fail with `missing_pre_stop`.

Initial green controls:

- `authority_call_334882_elevated_compact_noise_must_defer`
- `authority_clean_stable_customer_speech_positive_control`

This gives a tighter next-change gate than live calling:

1. A noise fix must turn the four red `authority_fan_noise_negative` samples
   green without turning the clean stable positive control red.
2. A Chinese short-utterance fix must turn the four
   `authority_cn_short_positive` samples green without regressing the five
   fan/noise negative controls.
3. Any runtime change must still pass the existing fixture-only matrix before
   another live call is useful.

## Authority Noise-Risk Fix Verification

After narrowing deferred/echo-guarded noise risk in
`app/services/ai_call/agent_runner.py`, the latest authority pair matrix now
replays as:

```text
p1_sample_matrix samples=10 passed=6 failed=4 missingReports=0
coverage status=pass failures=0
```

The five fan/noise negative controls now pass:

- `authority_call_334882_echo_guarded_noise_must_defer`
- `authority_call_334882_deferred_episode_noise_must_defer`
- `authority_call_334882_elevated_compact_noise_must_defer`
- `authority_call_334882_sparse_noise_must_defer`
- `authority_call_334885_clear_short_noise_must_defer`

The clean positive control still passes:

- `authority_clean_stable_customer_speech_positive_control`

The four Chinese short-utterance positives intentionally remain red with
`missing_pre_stop`:

- `authority_call_334885_haode_short_ack_must_pre_stop_fast`
- `authority_call_334885_tejialv_short_content_must_pre_stop_fast`
- `authority_call_334885_haode_zhidaole_must_pre_stop_fast`
- `authority_call_334885_call_end_phrase_must_pre_stop_fast`

Do not force these four green by relaxing acoustic thresholds globally; the
next fix needs additional cross-sample authority evidence, preferably
FSMN/WebRTC shadow or audio-derived speech probability compared through the
same fixture gate.

## Chinese Short-Utterance Audio Probe

The four remaining authority-pair red points above are decision-point fixtures:
they preserve a single 180 ms authority snapshot. They are useful as a warning
against over-relaxing thresholds, but they are too thin to prove the real audio
path still misses these utterances.

Temporary customer/AI track slices were exported to `/tmp` from
`call_334885037324660736` and replayed through audio authority fixtures. No
recording URLs, OSS object names, phone hashes, or audio files were committed.

Local-only matrix:

- `docs/livekit-ai-outbound/reports/phase-e-p1-audio-authority-probes-call_334885-2026-07-13.local.example.json`

Replay command:

```bash
.venv/bin/python tools/ai_call_p1_eval.py \
  --sample-matrix docs/livekit-ai-outbound/reports/phase-e-p1-audio-authority-probes-call_334885-2026-07-13.local.example.json \
  --fixture-only
```

Observed audio authority replay:

```text
好的。: candidateToPreStopMs=60, passed
特价率。: candidateToPreStopMs=60, passed
好的，知道了。: candidateToPreStopMs=60, passed
我要挂了吧挂了吧。: candidateToPreStopMs=60, passed
coverage status=pass failures=0
```

Interpretation:

1. The current runtime authority can pre-stop these Chinese short utterances
   when replayed from real customer-track audio with enough consecutive frames.
2. The remaining 4 authority-pair failures should not drive a global threshold
   change by themselves.
3. The next durable step is to promote real audio authority fixtures, or
   shadow-backed authority fixtures once real FSMN/WebRTC shadow evidence is
   available, instead of treating the 180 ms single-snapshot fixtures as the
   final source of truth.

The evaluator now also supports `authorityFixtures.*.shadowObservations` so a
future fixture can explicitly replay FSMN/WebRTC shadow evidence through the
same authority gate.

This local matrix is intentionally separate from
`docs/livekit-ai-outbound/p1-sample-matrix.local.example.json`: the latter must
remain runnable without private `/tmp` audio exports, while this probe matrix is
for machines that have regenerated the local customer/AI wav slices.
