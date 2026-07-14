# 2026-07-14 P1 live timeline replay closure

## Scope

This pass is the final bounded P1 root-cause check. It does not change runtime
authority thresholds, does not enable FSMN in the live pre-stop path, and does
not modify the 19012 semantic-analysis environment.

Target call: `call_335185420579983360`.

The current 19011 listener loaded the working-tree runner before this call. The
call completed with `5` confirmed pre-stops, `0` false pre-stops, and `0`
pending pre-stops, but the P1 evaluator still reported `5` customer speech
windows without a pre-stop inside `500ms`.

## Missing replay state

The previous full-call provider comparison marked every customer-track frame as
interruptible. Live runtime behavior is different:

1. `_is_sip_barge_in_interruptible(...)` is true while AI is speaking, or while
   a model response/recent AI audio still makes the session interruptible.
2. Non-interruptible frames reset detector activity and can update the learned
   noise floor.
3. Therefore an always-interruptible replay can materially overstate how early
   the detector forms a candidate.

`tools/ai_call_p1_vad_provider_compare.py --live-timeline` now reconstructs
interruptible windows from `model_response_started` / `model_response_done`
events, applies the runtime's `600ms` AI tail, and reports the original live
candidate/pre-stop offsets alongside replay output.

## Command

```bash
uv run --with funasr --with modelscope --with soundfile --with torch --with torchaudio \
  python tools/ai_call_p1_vad_provider_compare.py \
  --base-url http://127.0.0.1:19011 \
  --call-id call_335185420579983360 \
  --timeout-seconds 180 \
  --live-timeline \
  --json
```

No recording URL, object name, audio file, phone value, or secret is committed.

## Result

The replay reconstructed `9` interruptible windows covering `142222ms` of the
customer track.

| path | candidates | detector pre-stop evidence | first candidate |
| --- | ---: | ---: | ---: |
| live events | 19 | 5 actual `sip_pre_stop` events | 6299ms |
| WebRTC, always interruptible | 53 | 26 | 6040ms |
| WebRTC, live timeline | 28 | 16 | 6040ms |
| FSMN, live timeline | 0 | 0 | none |

The highest-value missed utterance starts near customer-track `28720ms`:

| evidence | candidate offset | lag from offline speech start |
| --- | ---: | ---: |
| WebRTC, always interruptible | 29500ms | 780ms |
| WebRTC, live timeline | 32080ms | 3360ms |
| live `sip_interrupt_candidate` | 32171ms | 3451ms |

The live-timeline replay is within `91ms` of the real candidate for this window.
This closes the main ambiguity: the multi-second delay is caused before
authority, by detector state under the real interruptible/noise-floor lifecycle.
It is not evidence that another authority exception is needed.

FSMN does not provide a safe shortcut. Under the same live timeline it forms no
candidate because replacing the frame VAD does not remove the shared local
RMS/SNR/noise-floor gates. The earlier always-interruptible FSMN comparison was
useful as a detector benchmark, but it did not reproduce runtime state.

The existing 45-sample labeled benchmark was also rerun without
`--live-timeline` to check the unchanged default path. All three provider paths
still detected `13/13` speech samples. The WebRTC/FSMN agreement path produced
`51` false-positive windows against the registered maximum of `50`, so that
benchmark gate failed by one window. The gate is not relaxed: this small drift
is additional evidence that agreement should remain offline/shadow-only.

## Limits

The timeline is reconstructed from persisted response events rather than a
per-frame runtime `interruptible` event. Egress recording timestamps and live
RTP processing can also differ by a few hundred milliseconds. The replay is
therefore a root-cause tool, not a claim of bit-exact runtime reproduction.

Some later windows still differ between replay and live. That means the latest
call does not support one universal runtime change that would fix every missed
utterance without reopening false-stop risk.

## Closure decision

P1 remains `freeze-ready`, not `product-done`.

1. Do not add more noise categories or threshold branches to authority.
2. Do not enable FSMN or WebRTC/FSMN agreement in the live pre-stop path.
3. Keep the current conservative behavior: false-stop control takes priority,
   and known noisy/echo-heavy cases may wait for provider speech detection.
4. Treat higher-quality noisy-environment interruption as a later media/AEC or
   source-separation problem, outside P1.
5. Reopen P1 runtime work only when a new change passes the existing freeze
   gates and demonstrates a repeated latency win on timeline-aware fixtures
   without increasing false pre-stops.

This is the stop condition for the current P1 tuning cycle. No further live
threshold tuning is justified by the available evidence.
