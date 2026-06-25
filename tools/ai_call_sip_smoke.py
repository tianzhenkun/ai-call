from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from typing import Any, TextIO

PostJson = Callable[[str, dict[str, Any], float], dict[str, Any]]


def mask_phone_number(phone_number: str) -> str:
    digits = "".join(ch for ch in str(phone_number or "") if ch.isdigit())
    if len(digits) <= 7:
        return "***"
    return f"{digits[:3]}****{digits[-4:]}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guarded smoke script for POST /ai-call/sip-sessions.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:19011",
        help="AI Call API base URL, without trailing /ai-call/sip-sessions.",
    )
    parser.add_argument(
        "--callee-phone-number",
        required=True,
        help="Real callee phone number. Output is always masked.",
    )
    parser.add_argument("--scene-code", required=True, help="Business scene code.")
    parser.add_argument("--business-id", default=None, help="Optional upstream business ID.")
    parser.add_argument("--voice", default=None, help="Optional Qwen Realtime voice.")
    parser.add_argument(
        "--business-params-json",
        default="{}",
        help="Optional JSON object for businessParams.",
    )
    parser.add_argument(
        "--ringing-timeout-seconds",
        type=int,
        default=None,
        help="Optional SIP ringing timeout seconds.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP request timeout seconds.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print the masked request only. Does not call the API.",
    )
    parser.add_argument(
        "--confirm-real-call",
        action="store_true",
        help="Required for real SIP outbound calls.",
    )
    return parser


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    business_params = _parse_business_params(args.business_params_json)
    payload: dict[str, Any] = {
        "calleePhoneNumber": args.callee_phone_number,
        "sceneCode": args.scene_code,
        "businessParams": business_params,
    }
    if args.voice:
        payload["voice"] = args.voice
    if args.business_id:
        payload["businessId"] = args.business_id
    if args.ringing_timeout_seconds is not None:
        payload["ringingTimeoutSeconds"] = args.ringing_timeout_seconds
    return payload


def run(
    argv: Sequence[str] | None = None,
    *,
    post_json: PostJson | None = None,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = build_payload(args)
    masked_phone = mask_phone_number(args.callee_phone_number)
    endpoint = f"{args.base_url.rstrip('/')}/ai-call/sip-sessions"

    if not args.dry_run and not args.confirm_real_call:
        parser.error(f"真实 SIP 外呼已被阻断：被叫 {masked_phone} 必须显式传入 --confirm-real-call")

    safe_payload = _redact_payload(payload)
    if args.dry_run:
        print(f"DRY RUN endpoint={endpoint}", file=stdout)
        print(json.dumps(safe_payload, ensure_ascii=False, sort_keys=True), file=stdout)
        return 0

    post_json = post_json or _post_json
    print(f"REAL CALL endpoint={endpoint} callee={masked_phone}", file=stdout)
    try:
        response = post_json(endpoint, payload, args.timeout_seconds)
    except Exception as exc:
        print(_redact_text(str(exc)), file=stderr)
        return 1
    if response.get("code") != 200:
        print(_redact_text(json.dumps(response, ensure_ascii=False)), file=stderr)
        return 1

    data = response.get("data") or {}
    print(
        "created "
        f"callId={data.get('callId')} "
        f"roomName={data.get('roomName')} "
        f"participantIdentity={data.get('participantIdentity')} "
        f"status={data.get('status')} "
        f"sipCallId={data.get('sipCallId')} "
        f"sipTrunkId={data.get('sipTrunkId')} "
        f"sipCallStatus={data.get('sipCallStatus')} "
        f"callee={masked_phone}",
        file=stdout,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> None:
    raise SystemExit(run(argv))


def _parse_business_params(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError("business params must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("business params must be a JSON object")
    return parsed


def _post_json(url: str, payload: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(_redact_text(raw_body)) from exc
    return json.loads(raw_body)


def _redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(payload)
    if "calleePhoneNumber" in redacted:
        redacted["calleePhoneNumber"] = mask_phone_number(str(redacted["calleePhoneNumber"]))
    return redacted


def _redact_text(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        return mask_phone_number(match.group(0))

    return re.sub(r"\+?\d[\d\s-]{5,}\d", replace, value)


if __name__ == "__main__":
    main()
