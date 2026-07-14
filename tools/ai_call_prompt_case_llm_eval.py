from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Sequence
from html import escape
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config.setting import settings


DEFAULT_CASES_PATH = "docs/livekit-ai-outbound/testdata/intro_geo_prompt_cases.jsonl"
DEFAULT_BASE_URL = "http://127.0.0.1:19012"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run prompt JSONL cases through an OpenAI-compatible chat model.",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--scene-code", default="intro_geo")
    parser.add_argument("--cases-path", default=DEFAULT_CASES_PATH)
    parser.add_argument("--customer-name", default="张总")
    parser.add_argument(
        "--instructions-path",
        help="Use a local final instructions file instead of fetching prompt preview.",
    )
    parser.add_argument("--opening-message", default="", help="Opening message for local candidate mode.")
    parser.add_argument(
        "--barge-in-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Barge-in flag for local candidate mode.",
    )
    parser.add_argument("--output", help="Optional markdown report path.")
    parser.add_argument(
        "--html-output",
        action="append",
        default=[],
        help="Optional visual HTML report path. Can be repeated.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Run only the first N cases.")
    parser.add_argument(
        "--case-id",
        action="append",
        default=[],
        help="Run only a specific case id. Can be repeated.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--json", action="store_true", help="Print raw JSON report.")
    return parser


async def run_async(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _llm_config()
    cases = _load_cases(Path(args.cases_path))
    if args.case_id:
        wanted = set(args.case_id)
        cases = [case for case in cases if str(case.get("id")) in wanted]
    if args.limit > 0:
        cases = cases[: args.limit]
    preview_data = _resolve_prompt_preview(args)
    instructions = str(preview_data.get("instructions") or "")
    opening_message = str(preview_data.get("openingMessage") or "")
    rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=args.timeout_seconds) as client:
        for case in cases:
            rows.append(
                await _run_case(
                    client=client,
                    config=config,
                    instructions=instructions,
                    opening_message=opening_message,
                    case=case,
                )
            )
    passed = sum(1 for row in rows if row["verdict"] == "PASS")
    failed = len(rows) - passed
    report = {
        "sceneCode": args.scene_code,
        "casesPath": args.cases_path,
        "mode": "llm_simulation",
        "model": config["model"],
        "promptHash": preview_data.get("promptHash"),
        "openingMessage": opening_message,
        "bargeInEnabled": preview_data.get("bargeInEnabled"),
        "summary": {
            "total": len(rows),
            "passed": passed,
            "failed": failed,
            "verdict": "PASS" if failed == 0 else "REVIEW",
        },
        "rows": rows,
    }
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(_markdown_report(report), encoding="utf-8")
    for html_output in args.html_output:
        Path(html_output).parent.mkdir(parents=True, exist_ok=True)
        Path(html_output).write_text(_html_report(report), encoding="utf-8")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(_text_summary(report))
    return 0 if failed == 0 else 1


def _resolve_prompt_preview(args: argparse.Namespace) -> dict[str, Any]:
    if args.instructions_path:
        return _preview_from_instructions_path(
            Path(args.instructions_path),
            opening_message=args.opening_message,
            barge_in_enabled=bool(args.barge_in_enabled),
        )
    return _prompt_preview(
        base_url=args.base_url,
        scene_code=args.scene_code,
        customer_name=args.customer_name,
    )


def _preview_from_instructions_path(
    path: Path,
    *,
    opening_message: str,
    barge_in_enabled: bool,
) -> dict[str, Any]:
    instructions = path.read_text(encoding="utf-8")
    return {
        "instructions": instructions,
        "openingMessage": opening_message,
        "bargeInEnabled": barge_in_enabled,
        "promptHash": "sha256:" + hashlib.sha256(instructions.encode("utf-8")).hexdigest(),
    }


def run(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run_async(argv))


def _llm_config() -> dict[str, str]:
    base_url = (settings.LLM_BASE_URL or settings.DASHSCOPE_BASE_URL).rstrip("/")
    api_key = settings.EFFECTIVE_LLM_API_KEY
    model = settings.LLM_MODEL or settings.POST_ANALYSIS_MODEL or settings.AI_CALL_SEMANTIC_ANALYSIS_MODEL or "qwen-plus"
    if not base_url or not api_key or not model:
        raise RuntimeError("LLM 配置不完整；请确认 ENVIRONMENT=dev 且 env/.env.dev 中有 DASHSCOPE_API_KEY 或 LLM_API_KEY。")
    return {"base_url": base_url, "api_key": api_key, "model": model}


def _load_cases(path: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        case = json.loads(line)
        if not isinstance(case, dict):
            raise ValueError(f"{path}:{line_no} is not a JSON object")
        cases.append(case)
    return cases


def _prompt_preview(*, base_url: str, scene_code: str, customer_name: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/ai-call/prompt-profiles/preview",
        data=json.dumps(
            {"sceneCode": scene_code, "businessParams": {"customerName": customer_name}},
            ensure_ascii=False,
        ).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body) from exc
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("prompt preview missing data")
    return data


async def _run_case(
    *,
    client: httpx.AsyncClient,
    config: dict[str, str],
    instructions: str,
    opening_message: str,
    case: dict[str, Any],
) -> dict[str, Any]:
    conversation: list[dict[str, str]] = [
        {"role": "system", "content": instructions},
        {"role": "assistant", "content": opening_message},
    ]
    responses: list[dict[str, str]] = []
    for turn in case.get("turns") or []:
        conversation.append({"role": "user", "content": str(turn)})
        answer = await _chat(
            client=client,
            config=config,
            messages=conversation,
            temperature=0.2,
            max_tokens=260,
        )
        conversation.append({"role": "assistant", "content": answer})
        responses.append({"customer": str(turn), "assistant": answer})
    judge = await _judge_case(
        client=client,
        config=config,
        case=case,
        responses=responses,
    )
    return {
        "id": case.get("id"),
        "category": case.get("category"),
        "customerState": case.get("customerState"),
        "verdict": "PASS" if bool(judge.get("passed")) else "REVIEW",
        "reason": str(judge.get("reason") or ""),
        "issues": judge.get("issues") if isinstance(judge.get("issues"), list) else [],
        "responses": responses,
    }


async def _judge_case(
    *,
    client: httpx.AsyncClient,
    config: dict[str, str],
    case: dict[str, Any],
    responses: list[dict[str, str]],
) -> dict[str, Any]:
    prompt = {
        "case": {
            "id": case.get("id"),
            "category": case.get("category"),
            "customerState": case.get("customerState"),
            "turns": case.get("turns"),
            "expected": case.get("expected"),
            "forbidden": case.get("forbidden"),
        },
        "responses": responses,
    }
    content = await _chat(
        client=client,
        config=config,
        messages=[
            {
                "role": "system",
                "content": (
                    "你是 AI 外呼提示词测试裁判。只根据 case.expected、case.forbidden 和 responses 判断。"
                    "如果回答满足 expected 且没有触犯 forbidden，则 passed=true。"
                    "只返回 JSON：passed(boolean), reason(string), issues(string[])。"
                ),
            },
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ],
        temperature=0,
        max_tokens=300,
        response_format={"type": "json_object"},
    )
    try:
        data = json.loads(_strip_code_fence(content))
    except json.JSONDecodeError:
        return {"passed": False, "reason": "judge_json_parse_failed", "issues": [content[:200]]}
    return data if isinstance(data, dict) else {"passed": False, "reason": "judge_not_object", "issues": []}


async def _chat(
    *,
    client: httpx.AsyncClient,
    config: dict[str, str],
    messages: list[dict[str, str]],
    temperature: float,
    max_tokens: int,
    response_format: dict[str, str] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "model": config["model"],
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_format is not None:
        payload["response_format"] = response_format
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = await client.post(
                f"{config['base_url']}/chat/completions",
                headers={
                    "Authorization": f"Bearer {config['api_key']}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            break
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_error = exc
            if attempt == 2:
                raise
            await asyncio.sleep(float(attempt + 1))
    else:
        raise RuntimeError("LLM request failed") from last_error
    response.raise_for_status()
    data = response.json()
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("LLM response missing choices")
    message = choices[0].get("message")
    if not isinstance(message, dict):
        raise RuntimeError("LLM response missing message")
    content = message.get("content")
    if not isinstance(content, str):
        raise RuntimeError("LLM response content is not text")
    return content.strip()


def _strip_code_fence(text: str) -> str:
    value = text.strip()
    if value.startswith("```"):
        lines = value.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        value = "\n".join(lines).strip()
    return value


def _zh_verdict(value: Any) -> str:
    return {
        "PASS": "通过",
        "FAIL": "失败",
        "REVIEW": "需复核",
    }.get(str(value), str(value))


def _zh_bool(value: Any) -> str:
    if value is True:
        return "是"
    if value is False:
        return "否"
    return str(value)


def _zh_mode(value: Any) -> str:
    return {
        "llm_simulation": "文本模型模拟",
    }.get(str(value), str(value))


def _text_summary(report: dict[str, Any]) -> str:
    summary = report["summary"]
    return (
        f"场景={report['sceneCode']} 模式={_zh_mode(report['mode'])} 模型={report['model']} "
        f"结论={_zh_verdict(summary['verdict'])} 通过数={summary['passed']}/{summary['total']} "
        f"是否允许打断={_zh_bool(report.get('bargeInEnabled'))} "
        f"提示词哈希={report.get('promptHash')}"
    )


def _markdown_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        f"# {report['sceneCode']} 提示词文本模型模拟报告",
        "",
        "> 本报告使用已配置的文本模型模拟客户对话，并由模型裁判按 expected/forbidden 规则判定。"
        "它不等同于真实 Qwen Realtime 电话链路验收。",
        "",
        f"- 场景：`{report['sceneCode']}`",
        f"- 模式：`{_zh_mode(report['mode'])}`",
        f"- 模型：`{report['model']}`",
        f"- 结论：`{_zh_verdict(summary['verdict'])}`",
        f"- 通过数：`{summary['passed']}/{summary['total']}`",
        f"- 是否允许打断：`{_zh_bool(report.get('bargeInEnabled'))}`",
        f"- 提示词哈希：`{report.get('promptHash')}`",
        f"- 开场白：{report.get('openingMessage')}",
        "",
        "| 用例 | 类别 | 结果 | 客户问题/追问 | 判定原因 | 首轮 AI 回复 |",
        "|---|---|---|---|---|---|",
    ]
    for row in report["rows"]:
        responses = row.get("responses") or []
        customer_turns = [
            str(response.get("customer") or "").replace("\n", "<br>")
            for response in responses
            if response.get("customer")
        ]
        customer_questions = "<br>".join(customer_turns) if customer_turns else "-"
        first_answer = responses[0].get("assistant", "") if responses else ""
        first_answer = first_answer.replace("\n", "<br>")
        reason = str(row.get("reason") or "").replace("\n", " ")
        lines.append(
            f"| `{row['id']}` | `{row['category']}` | {_zh_verdict(row['verdict'])} | "
            f"{customer_questions} | {reason} | {first_answer} |"
        )
    lines.append("")
    return "\n".join(lines)


def _html_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    verdict = _zh_verdict(summary["verdict"])
    verdict_class = "pass" if verdict == "通过" else "review"
    rows_html = "\n".join(_html_case_card(row) for row in report["rows"])
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_html_text(report['sceneCode'])} 提示词文本模型模拟报告</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f7fb;
      --surface: #ffffff;
      --surface-soft: #f2f5f9;
      --text: #182230;
      --muted: #667085;
      --border: #d9e0ea;
      --accent: #2563eb;
      --pass: #087443;
      --pass-bg: #e8f6ef;
      --review: #b54708;
      --review-bg: #fff4e5;
      --shadow: 0 10px 28px rgba(16, 24, 40, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.55;
    }}
    main {{
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 48px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 20px;
      align-items: start;
      padding: 26px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }}
    .eyebrow {{
      margin: 0 0 6px;
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(24px, 3vw, 34px);
      line-height: 1.18;
      letter-spacing: 0;
    }}
    .hero-note {{
      max-width: 780px;
      margin: 12px 0 0;
      color: var(--muted);
    }}
    .verdict {{
      min-width: 106px;
      text-align: center;
      padding: 10px 16px;
      border-radius: 999px;
      font-weight: 800;
    }}
    .verdict.pass, .badge.pass {{ color: var(--pass); background: var(--pass-bg); }}
    .verdict.review, .badge.review {{ color: var(--review); background: var(--review-bg); }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 16px 0;
    }}
    .metric {{
      padding: 16px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 13px;
    }}
    .metric-value {{
      margin-top: 6px;
      font-size: 20px;
      font-weight: 800;
      overflow-wrap: anywhere;
    }}
    .toolbar {{
      position: sticky;
      top: 0;
      z-index: 2;
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      align-items: center;
      margin: 18px 0;
      padding: 12px;
      background: rgba(246, 247, 251, 0.94);
      backdrop-filter: blur(10px);
      border: 1px solid var(--border);
      border-radius: 8px;
    }}
    .search {{
      width: 100%;
      min-height: 42px;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 15px;
      background: var(--surface);
    }}
    .filters {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .filter-button {{
      min-height: 38px;
      padding: 8px 12px;
      border: 1px solid var(--border);
      border-radius: 6px;
      background: var(--surface);
      color: var(--text);
      cursor: pointer;
      font-weight: 700;
    }}
    .filter-button.is-active {{
      border-color: var(--accent);
      color: var(--accent);
      background: #eff6ff;
    }}
    .result-count {{
      margin: 0 0 10px;
      color: var(--muted);
      font-size: 14px;
    }}
    .case-list {{
      display: grid;
      gap: 14px;
    }}
    .case-card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: 0 8px 20px rgba(16, 24, 40, 0.05);
      overflow: hidden;
    }}
    .case-head {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 12px;
      align-items: start;
      padding: 16px 18px;
      border-bottom: 1px solid var(--border);
      background: var(--surface-soft);
    }}
    .case-title {{
      margin: 0;
      font-size: 18px;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }}
    .case-meta {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      padding: 4px 9px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 800;
    }}
    .badge.neutral {{
      color: #344054;
      background: #e9edf3;
    }}
    .case-body {{
      display: grid;
      grid-template-columns: minmax(220px, 0.9fr) minmax(260px, 1.1fr);
      gap: 18px;
      padding: 18px;
    }}
    .block {{
      min-width: 0;
    }}
    .block-title {{
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
    }}
    .question-list {{
      margin: 0;
      padding-left: 20px;
    }}
    .question-list li + li {{
      margin-top: 8px;
    }}
    .answer, .reason {{
      margin: 0;
      padding-left: 12px;
      border-left: 3px solid var(--border);
      overflow-wrap: anywhere;
    }}
    details {{
      grid-column: 1 / -1;
      border-top: 1px solid var(--border);
      padding-top: 12px;
    }}
    summary {{
      cursor: pointer;
      color: var(--accent);
      font-weight: 800;
    }}
    .turn {{
      margin-top: 12px;
      padding-left: 12px;
      border-left: 3px solid #dbeafe;
    }}
    .turn-label {{
      margin: 0 0 4px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
    }}
    .turn p {{
      margin: 0;
      overflow-wrap: anywhere;
    }}
    .empty {{
      display: none;
      padding: 26px;
      text-align: center;
      color: var(--muted);
      background: var(--surface);
      border: 1px dashed var(--border);
      border-radius: 8px;
    }}
    @media (max-width: 860px) {{
      main {{ width: min(100vw - 20px, 1180px); padding-top: 14px; }}
      .hero, .toolbar, .case-head, .case-body {{ grid-template-columns: 1fr; }}
      .summary-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .filters {{ justify-content: flex-start; }}
    }}
    @media (max-width: 520px) {{
      .summary-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div>
        <p class="eyebrow">{_html_text(_zh_mode(report['mode']))}</p>
        <h1>{_html_text(report['sceneCode'])} 提示词文本模型模拟报告</h1>
        <p class="hero-note">使用已配置的文本模型模拟客户对话，并由模型裁判按 expected/forbidden 规则判定；这不等同于真实 Qwen Realtime 电话链路验收。</p>
      </div>
      <div class="verdict {verdict_class}">{_html_text(verdict)}</div>
    </section>

    <section class="summary-grid" aria-label="报告摘要">
      <div class="metric"><div class="metric-label">通过数</div><div class="metric-value">{summary['passed']} / {summary['total']}</div></div>
      <div class="metric"><div class="metric-label">模型</div><div class="metric-value">{_html_text(report['model'])}</div></div>
      <div class="metric"><div class="metric-label">是否允许打断</div><div class="metric-value">{_html_text(_zh_bool(report.get('bargeInEnabled')))}</div></div>
      <div class="metric"><div class="metric-label">提示词哈希</div><div class="metric-value">{_html_text(report.get('promptHash'))}</div></div>
      <div class="metric"><div class="metric-label">开场白</div><div class="metric-value">{_html_text(report.get('openingMessage'))}</div></div>
    </section>

    <section class="toolbar" aria-label="筛选工具">
      <input class="search" id="search" type="search" placeholder="搜索用例、类别、客户问题或回复">
      <div class="filters">
        <button class="filter-button is-active" type="button" data-filter="全部">全部</button>
        <button class="filter-button" type="button" data-filter="通过">通过</button>
        <button class="filter-button" type="button" data-filter="需复核">需复核</button>
      </div>
    </section>

    <p class="result-count" id="resultCount"></p>
    <section class="case-list" id="caseList">
      {rows_html}
    </section>
    <div class="empty" id="emptyState">没有匹配的用例。</div>
  </main>

  <script>
    const searchInput = document.querySelector('#search');
    const buttons = [...document.querySelectorAll('.filter-button')];
    const cards = [...document.querySelectorAll('.case-card')];
    const resultCount = document.querySelector('#resultCount');
    const emptyState = document.querySelector('#emptyState');
    let activeFilter = '全部';

    function applyFilters() {{
      const keyword = searchInput.value.trim().toLowerCase();
      let visible = 0;
      for (const card of cards) {{
        const matchesStatus = activeFilter === '全部' || card.dataset.status === activeFilter;
        const matchesKeyword = !keyword || card.dataset.search.includes(keyword);
        const show = matchesStatus && matchesKeyword;
        card.hidden = !show;
        if (show) visible += 1;
      }}
      resultCount.textContent = `当前显示 ${{visible}} / ${{cards.length}} 条用例`;
      emptyState.style.display = visible === 0 ? 'block' : 'none';
    }}

    searchInput.addEventListener('input', applyFilters);
    for (const button of buttons) {{
      button.addEventListener('click', () => {{
        activeFilter = button.dataset.filter;
        for (const item of buttons) item.classList.toggle('is-active', item === button);
        applyFilters();
      }});
    }}
    applyFilters();
  </script>
</body>
</html>
"""


def _html_case_card(row: dict[str, Any]) -> str:
    responses = row.get("responses") or []
    verdict = _zh_verdict(row.get("verdict"))
    status_class = "pass" if verdict == "通过" else "review"
    questions = [
        _html_text(response.get("customer"))
        for response in responses
        if response.get("customer")
    ]
    question_items = "".join(f"<li>{question}</li>" for question in questions) or "<li>-</li>"
    first_answer = _html_text(responses[0].get("assistant", "")) if responses else "-"
    reason = _html_text(row.get("reason") or "")
    transcript = "\n".join(_html_turns(responses))
    search_text = " ".join([
        str(row.get("id") or ""),
        str(row.get("category") or ""),
        verdict,
        str(row.get("reason") or ""),
        " ".join(str(response.get("customer") or "") for response in responses),
        " ".join(str(response.get("assistant") or "") for response in responses),
    ]).lower()
    return f"""
      <article class="case-card" data-status="{_html_attr(verdict)}" data-category="{_html_attr(row.get('category'))}" data-search="{_html_attr(search_text)}">
        <header class="case-head">
          <div>
            <h2 class="case-title">{_html_text(row.get('id'))}</h2>
            <div class="case-meta">
              <span class="badge neutral">{_html_text(row.get('category'))}</span>
              <span class="badge {status_class}">{_html_text(verdict)}</span>
            </div>
          </div>
        </header>
        <div class="case-body">
          <section class="block">
            <h3 class="block-title">客户问题/追问</h3>
            <ol class="question-list">{question_items}</ol>
          </section>
          <section class="block">
            <h3 class="block-title">首轮 AI 回复</h3>
            <p class="answer">{first_answer}</p>
          </section>
          <section class="block">
            <h3 class="block-title">判定原因</h3>
            <p class="reason">{reason}</p>
          </section>
          <details>
            <summary>查看完整模拟对话</summary>
            {transcript}
          </details>
        </div>
      </article>
    """


def _html_turns(responses: list[dict[str, str]]) -> list[str]:
    turns: list[str] = []
    for index, response in enumerate(responses, start=1):
        turns.append(
            f"""<div class="turn">
              <p class="turn-label">客户第 {index} 轮</p>
              <p>{_html_text(response.get('customer'))}</p>
            </div>
            <div class="turn">
              <p class="turn-label">AI 第 {index} 轮</p>
              <p>{_html_text(response.get('assistant'))}</p>
            </div>"""
        )
    return turns


def _html_text(value: Any) -> str:
    return escape(str(value if value is not None else ""), quote=False).replace("\n", "<br>")


def _html_attr(value: Any) -> str:
    return escape(str(value if value is not None else ""), quote=True)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
