from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

ROOT = Path(__file__).resolve().parents[1]

MAIN_MATRIX = Path("docs/livekit-ai-outbound/p1-sample-matrix.local.example.json")
AUDIO_AUTHORITY_MATRIX = Path(
    "docs/livekit-ai-outbound/reports/"
    "phase-e-p1-audio-authority-probes-call_334885-2026-07-13.local.example.json"
)
LATEST_AUTHORITY_PAIRS = Path(
    "docs/livekit-ai-outbound/reports/"
    "phase-e-p1-authority-fixture-pairs-calls_334882_334885-2026-07-13.json"
)

EXPECTED_LATEST_PAIR_DIAGNOSTIC_FAILURE_IDS = {
    "authority_call_334885_haode_short_ack_must_pre_stop_fast",
    "authority_call_334885_tejialv_short_content_must_pre_stop_fast",
    "authority_call_334885_haode_zhidaole_must_pre_stop_fast",
    "authority_call_334885_call_end_phrase_must_pre_stop_fast",
}

PY_COMPILE_FILES = [
    "tools/ai_call_p1_eval.py",
    "tools/ai_call_p1_freeze_acceptance.py",
    "app/services/ai_call/agent_runner.py",
]

RUFF_FILES = [
    "tools/ai_call_p1_eval.py",
    "tools/ai_call_p1_freeze_acceptance.py",
    "tests/test_ai_call_interrupt_offline_analysis.py",
    "app/services/ai_call/agent_runner.py",
]

DIFF_CHECK_FILES = [
    "app/services/ai_call/agent_runner.py",
    "tests/test_ai_call_interrupt_offline_analysis.py",
    "tools/ai_call_p1_eval.py",
    "tools/ai_call_p1_freeze_acceptance.py",
    "docs/livekit-ai-outbound/phases/phase-e-sip-barge-in-p1-freeze-spec.md",
    "docs/livekit-ai-outbound/reports/2026-07-13-p1-latest-two-calls-red-sample-pool.md",
    str(AUDIO_AUTHORITY_MATRIX),
    str(LATEST_AUTHORITY_PAIRS),
]


@dataclass(frozen=True)
class CommandResult:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the SIP barge-in P1 freeze acceptance gates.",
    )
    parser.add_argument(
        "--skip-local-audio",
        action="store_true",
        help="Skip the /tmp-backed audio authority probe gate.",
    )
    parser.add_argument(
        "--skip-ruff",
        action="store_true",
        help="Skip ruff check --no-fix.",
    )
    parser.add_argument(
        "--skip-pytest",
        action="store_true",
        help="Skip focused pytest.",
    )
    return parser


def run(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    args = build_parser().parse_args(argv)
    failed = False

    _print_step("main sample matrix", stdout)
    failed |= not _gate_matrix_all_green(MAIN_MATRIX, stdout=stdout, stderr=stderr)

    if not args.skip_local_audio:
        _print_step("local audio authority probe", stdout)
        failed |= not _gate_audio_authority_matrix(stdout=stdout, stderr=stderr)

    _print_step("latest authority fixture pairs", stdout)
    failed |= not _gate_latest_authority_pairs(stdout=stdout, stderr=stderr)

    if not args.skip_pytest:
        _print_step("focused pytest", stdout)
        failed |= not _gate_command(
            [sys.executable, "-m", "pytest", "tests/test_ai_call_interrupt_offline_analysis.py", "-q"],
            stdout=stdout,
            stderr=stderr,
        )

    if not args.skip_ruff:
        _print_step("ruff check", stdout)
        failed |= not _gate_command(
            [sys.executable, "-m", "ruff", "check", "--no-fix", *RUFF_FILES],
            stdout=stdout,
            stderr=stderr,
        )

    _print_step("py_compile", stdout)
    failed |= not _gate_command(
        [sys.executable, "-m", "py_compile", *PY_COMPILE_FILES],
        stdout=stdout,
        stderr=stderr,
    )

    _print_step("git diff --check", stdout)
    failed |= not _gate_command(
        ["git", "diff", "--check", "--", *DIFF_CHECK_FILES],
        stdout=stdout,
        stderr=stderr,
    )

    if failed:
        print("P1 freeze acceptance: FAIL", file=stdout)
        return 1
    print("P1 freeze acceptance: PASS", file=stdout)
    return 0


def _gate_matrix_all_green(
    matrix_path: Path,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> bool:
    result, report = _run_p1_eval_json(matrix_path)
    _print_command_result(result, stdout=stdout, stderr=stderr)
    if result.returncode != 0:
        print(f"FAIL {matrix_path}: expected exit 0, got {result.returncode}", file=stderr)
        return False
    return _assert_no_failed_samples(report, label=str(matrix_path), stderr=stderr)


def _gate_audio_authority_matrix(
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> bool:
    if not _assert_audio_fixture_paths_exist(AUDIO_AUTHORITY_MATRIX, stderr=stderr):
        return False
    return _gate_matrix_all_green(AUDIO_AUTHORITY_MATRIX, stdout=stdout, stderr=stderr)


def _gate_latest_authority_pairs(
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> bool:
    result, report = _run_p1_eval_json(LATEST_AUTHORITY_PAIRS)
    _print_command_result(result, stdout=stdout, stderr=stderr)
    if result.returncode not in {0, 2}:
        print(
            f"FAIL {LATEST_AUTHORITY_PAIRS}: expected exit 0 or 2, got {result.returncode}",
            file=stderr,
        )
        return False

    samples = _samples(report)
    failed_ids = {str(sample.get("id")) for sample in samples if not sample.get("passed")}
    if failed_ids != EXPECTED_LATEST_PAIR_DIAGNOSTIC_FAILURE_IDS:
        print(
            "FAIL latest authority pairs: unexpected diagnostic failure set "
            f"expected={sorted(EXPECTED_LATEST_PAIR_DIAGNOSTIC_FAILURE_IDS)} "
            f"actual={sorted(failed_ids)}",
            file=stderr,
        )
        return False

    fan_noise_failures = [
        str(sample.get("id"))
        for sample in samples
        if sample.get("category") == "authority_fan_noise_negative" and not sample.get("passed")
    ]
    if fan_noise_failures:
        print(
            "FAIL latest authority pairs: fan/noise negative controls failed "
            f"{fan_noise_failures}",
            file=stderr,
        )
        return False

    summary = report.get("summary")
    if not isinstance(summary, dict):
        print("FAIL latest authority pairs: missing summary", file=stderr)
        return False
    if summary.get("samples") != 10 or summary.get("passed") != 6 or summary.get("failed") != 4:
        print(
            "FAIL latest authority pairs: expected samples=10 passed=6 failed=4, got "
            f"samples={summary.get('samples')} passed={summary.get('passed')} "
            f"failed={summary.get('failed')}",
            file=stderr,
        )
        return False

    print(
        "latest authority pairs accepted with 4 expected single-snapshot diagnostic reds",
        file=stdout,
    )
    return True


def _gate_command(
    command: list[str],
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> bool:
    result = _run_command(command)
    _print_command_result(result, stdout=stdout, stderr=stderr)
    if result.returncode == 0:
        return True
    print(f"FAIL command returned {result.returncode}: {_format_command(command)}", file=stderr)
    return False


def _run_p1_eval_json(matrix_path: Path) -> tuple[CommandResult, dict[str, Any]]:
    command = [
        sys.executable,
        "tools/ai_call_p1_eval.py",
        "--sample-matrix",
        str(matrix_path),
        "--fixture-only",
        "--json",
    ]
    result = _run_command(command)
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"p1 eval did not return JSON for {matrix_path}: {exc}") from exc
    if not isinstance(report, dict):
        raise RuntimeError(f"p1 eval JSON report must be an object for {matrix_path}")
    return result, report


def _run_command(command: list[str]) -> CommandResult:
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    return CommandResult(
        args=command,
        returncode=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


def _assert_no_failed_samples(report: dict[str, Any], *, label: str, stderr: TextIO) -> bool:
    failed_samples = [sample for sample in _samples(report) if not sample.get("passed")]
    if failed_samples:
        print(
            f"FAIL {label}: failed samples={[sample.get('id') for sample in failed_samples]}",
            file=stderr,
        )
        return False
    summary = report.get("summary")
    if isinstance(summary, dict) and summary.get("failed") not in {0, None}:
        print(f"FAIL {label}: summary failed={summary.get('failed')}", file=stderr)
        return False
    return True


def _assert_audio_fixture_paths_exist(matrix_path: Path, *, stderr: TextIO) -> bool:
    try:
        matrix = json.loads((ROOT / matrix_path).read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"FAIL {matrix_path}: cannot read matrix: {exc}", file=stderr)
        return False

    missing_paths: list[str] = []
    for fixture in matrix.get("audioFixtures") or []:
        if not isinstance(fixture, dict):
            continue
        for key in ("wavPath", "aiWavPath"):
            value = fixture.get(key)
            if not isinstance(value, str) or not value:
                continue
            path = Path(value)
            if not path.is_absolute():
                path = (ROOT / matrix_path).parent / path
            if not path.exists():
                missing_paths.append(str(path))

    if not missing_paths:
        return True

    print(f"FAIL {matrix_path}: local audio fixture files are missing", file=stderr)
    for path in missing_paths:
        print(f"missing {path}", file=stderr)
    print(
        "Regenerate them with the localAudioFixtureGeneration commands in the matrix.",
        file=stderr,
    )
    return False


def _samples(report: dict[str, Any]) -> list[dict[str, Any]]:
    samples = report.get("samples")
    if not isinstance(samples, list):
        return []
    return [sample for sample in samples if isinstance(sample, dict)]


def _print_step(name: str, stdout: TextIO) -> None:
    print(f"\n== {name} ==", file=stdout)


def _print_command_result(
    result: CommandResult,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> None:
    print(f"$ {_format_command(result.args)}", file=stdout)
    if result.stdout.strip():
        print(_summarize_stdout(result.stdout), file=stdout)
    if result.stderr.strip():
        print(result.stderr.rstrip(), file=stderr)


def _summarize_stdout(output: str) -> str:
    try:
        report = json.loads(output)
    except json.JSONDecodeError:
        return output.rstrip()
    if not isinstance(report, dict):
        return output.rstrip()
    summary = report.get("summary")
    if not isinstance(summary, dict):
        return output.rstrip()
    return (
        "summary "
        f"samples={summary.get('samples')} "
        f"passed={summary.get('passed')} "
        f"failed={summary.get('failed')} "
        f"missingReports={summary.get('missingReports')}"
    )


def _format_command(command: Sequence[str]) -> str:
    return " ".join(command)


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


if __name__ == "__main__":
    main()
