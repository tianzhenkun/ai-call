# Phase E SIP barge-in P1 delivery closure

## Decision

The P1 delivery baseline is the exact runtime snapshot at:

```text
8ffa0b9bd903d68df5e5b4662c1966aacbfc69e5
```

This snapshot is `freeze-ready`, not `product-done`. Delivery closure freezes
the verified behavior and its known limits. It does not claim that every real
Chinese interruption is detected within the target latency.

The delivery branch must not change runtime files relative to `8ffa0b9`.

## Included

Keep the complete 13-commit P1 sequence from `a7a1dbc` through `8ffa0b9` as one
verified snapshot. Do not reconstruct the delivery by cherry-picking only the
latest authority commits: the runtime, configuration, replay fixtures, and
freeze gate were verified together.

The delivery includes:

- the SIP local candidate, pre-stop, generation gate, confirm/reject, and
  rejected recovery path;
- scene-level `barge_in_enabled` enforcement on both local and provider paths;
- explicit `customer_end` fast termination;
- the committed fixture matrices and timeline-aware replay tools;
- `tools/ai_call_p1_freeze_acceptance.py` as the release acceptance entrypoint;
- the final real-call report for `call_335256081413709824`.

## Retained outside the runtime delivery

The following assets remain useful for diagnosis and future research, but stay
on the isolated `codex/p1-speech-evidence-provider` branch and are not merged
into this delivery:

- normalized speech evidence provider and observation events;
- WebRTC, FSMN, Silero, agreement, and streaming admission comparisons;
- delayed evidence episode audits;
- source attribution and AI-reference echo experiments;
- 19013 Shadow deployment examples and archive tools;
- associated offline fixtures, reports, and focused tests.

These assets may be used to explain a future defect or to evaluate a new media
provider. They have no stop authority in the P1 delivery.

## Explicitly excluded

Do not merge or reapply these changes to the P1 runtime:

1. The uncommitted `ed81` high-noise authority additions in
   `app/services/ai_call/agent_runner.py` and their call-specific tests. The
   clean acceptance runtime did not load them, and the final report explicitly
   freezes before them.
2. The experimental `voiced_low_rms_hold_ms` detector behavior. It improved
   isolated recall but increased negative activity and added no complete
   authority benefit.
3. The live speech-evidence adapter, FSMN/Silero promotion, delayed episode
   authority, source-attribution veto, or AI-reference echo veto. Their
   promotion gates failed or lacked real positive evidence.
4. Local IP edits, `.env.dev`, `*.local.yaml`, SQLite databases, recordings,
   provider credentials, and `/tmp` fixture audio.
5. Unrelated Phase B SQL and browser automation artifacts present in the dirty
   `ed81` worktree.

## Acceptance

Run from a clean checkout of this delivery branch:

```bash
.venv/bin/python tools/ai_call_p1_freeze_acceptance.py
git diff --exit-code 8ffa0b9 -- app/services/ai_call
git diff --check 8ffa0b9..HEAD
```

Expected freeze-gate results:

- main fixture matrix: `23/23`;
- local audio authority matrix: `4/4`;
- latest authority pairs: `6/10`, with the four documented diagnostic reds;
- focused tests, Ruff, Python compilation, and diff check: pass;
- final result: `P1 freeze acceptance: PASS`.

The second command is the release invariant: closure documentation may differ,
but runtime code must remain identical to `8ffa0b9`.

## Closure verification: 2026-07-15

The complete freeze gate was rerun from the clean delivery worktree with the
existing project virtual environment. It produced:

- main fixture matrix: `23/23`;
- local audio authority matrix: `4/4`;
- latest authority pairs: `6/10`, with exactly the four expected reds;
- offline analysis tests: `56 passed`;
- call-end focused tests: `17 passed`;
- Ruff, Python compilation, and diff check: pass;
- final result: `P1 freeze acceptance: PASS`.

At verification time, ports `19011`, `19012`, and `19013` each returned HTTP
`200` from `/ai-call/health`. No service was restarted. The `19011` process was
already running from the clean `8ffa0b9` worktree, `19012` remained in the
isolated semantic worktree, and the unpromoted `19013` Shadow process remained
isolated from both.

## Release and rollback

Before deployment, record the current target-environment commit as the rollback
commit. Then deploy this branch with environment-specific values supplied by
the deployment system, not committed files.

Verify the target environment in this order:

1. API health, database target, LiveKit/SIP isolation, and effective prompt
   profile configuration;
2. one controlled smoke call covering normal completion and explicit
   `customer_end`;
3. event persistence for candidate, pre-stop, confirm/reject, and call end;
4. no regression in a scene with `barge_in_enabled=false`.

If a blocking regression appears, first set `barge_in_enabled=false` for the
affected scene to remove P1 authority, then roll back to the recorded
pre-deployment commit. Do not tune detector or authority thresholds in the
target environment.

## Reopening gate

P1 implementation stays closed until a new proposal has replayable real audio
and satisfies all of the following before touching live authority:

- opening and short Chinese speech meet the agreed pre-stop latency target;
- the committed noise and echo negatives remain non-interrupting;
- repeated real calls add zero false pre-stops;
- every stop decision names both its detector evidence and authority rule;
- an echo-specific proposal includes at least one labeled real echo positive
  with a synchronized causal AI reference track.

Without that evidence, the correct action is to keep `8ffa0b9` frozen.
