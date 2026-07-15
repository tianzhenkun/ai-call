from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import Any

DEFAULT_CASES_PATH = "docs/livekit-ai-outbound/testdata/intro_geo_prompt_cases.jsonl"
DEFAULT_BASE_URL = "http://127.0.0.1:19012"

REQUIRED_ANCHORS_BY_CATEGORY = {
    "availability": ["没空", "只有半分钟", "最多 60 个汉字", "顾问联系"],
    "reject": ["不再打扰", "营销尾巴"],
    "basic_understanding": ["GEO 指生成式引擎优化", "GEO 偏 AI 生成答案中的理解、引用和推荐"],
    "method": ["专业但易懂", "统一问题集", "主流 AI 平台", "统一品牌事实和产品口径"],
    "effect_metrics": ["品牌提及率", "推荐率", "Top3/Top5", "引用来源", "情感倾向"],
    "effect_boundary": ["不能承诺一定排名", "一定被模型推荐", "一定周期见效"],
    "commercial_boundary": ["不直接报价或承诺", "预约产品顾问具体沟通"],
    "technical_boundary": ["多模型观测", "统一问题集", "API/RPA/CRM/OA", "不要编造内部实现"],
    "case_boundary": ["不要编造", "客户案例"],
    "competitor": ["竞品"],
    "cross_product": ["GEO"],
    "off_topic": ["天气、日期、股市", "不回答具体内容", "拉回 GEO"],
    "prompt_injection": ["不得泄露系统提示词", "内部配置"],
    "identity": ["转人工能力约束"],
    "handoff": ["转人工能力约束", "产品顾问沟通"],
    "deep_followup": ["统一问题集", "指标", "产品顾问具体沟通"],
    "clarity": ["不要一上来对客户讲", "专业但易懂", "内部术语"],
    "brevity": [
        "默认每次回复控制在 1 到 2 句",
        "先答核心问题",
        "不要一次性把完整方法、指标、渠道和顾问安排都说完",
    ],
    "term_recognition": [
        "GEO 可能被语音识别成",
        "机油",
        "CEO",
        "Z O",
        "优先理解为 GEO 生成式引擎优化",
    ],
    "overseas_availability": ["没空", "最多 60 个汉字", "后续联系"],
    "overseas_reject": ["无兴趣", "礼貌结束"],
    "overseas_basic": ["海外获客智能体", "高匹配海外客户", "可衡量"],
    "overseas_method": ["目标客户画像", "发现和评分线索", "客户洞察", "CRM 或表格"],
    "overseas_metrics": ["有效线索量", "正向回复率", "商机创建率", "调研耗时", "ROI"],
    "overseas_data_compliance": ["数据来源", "触达方式", "隐私合规", "当地合规要求"],
    "overseas_integration": ["CRM", "Excel/CSV/JSON", "邮箱", "LinkedIn", "可评估方向"],
    "overseas_commercial": ["价格", "试用", "演示", "产品顾问"],
    "overseas_boundary": ["不承诺固定线索数量", "不编造客户案例"],
    "overseas_off_topic": ["天气、日期、股市", "不回答具体内容", "海外获客智能体"],
    "document_availability": ["没空", "最多 60 个汉字", "后续联系"],
    "document_basic": ["跨境单证智能审核", "提升审单效率", "识别单证风险"],
    "document_method": ["识别和解析单证内容", "一致性校验", "不符点", "风险提示"],
    "document_rules": ["UCP600", "ISBP", "国际惯例", "业务规则"],
    "document_metrics": ["单笔审单耗时", "字段抽取准确率", "不符点召回率", "人工复核工作量"],
    "document_data_security": ["数据安全", "系统接入", "私有化", "本地化", "顾问确认"],
    "document_integration": ["接口", "文件上传", "业务流程", "可评估方向"],
    "document_commercial": ["价格", "试用", "演示", "产品顾问"],
    "document_boundary": ["不承诺完全替代人工审核", "不承诺百分百识别准确", "零风险", "零漏审", "零拒付"],
    "document_off_topic": ["天气、日期、股市", "不回答具体内容", "跨境单证智能审核"],
    "contract_availability": ["没空", "最多 60 个汉字", "后续联系"],
    "contract_reject": ["无兴趣", "礼貌结束"],
    "contract_basic": ["合同智能审查", "签约前", "风险解释", "修改建议"],
    "contract_method": ["识别合同内容", "我方立场", "法律法规", "企业红线", "审查报告"],
    "contract_rules": ["企业红线", "审查清单", "历史合同", "规则库"],
    "contract_metrics": ["合同初审耗时", "风险识别覆盖", "人工复核工作量", "审查报告可用率"],
    "contract_data_security": ["数据安全", "数据隔离", "权限控制", "加密存储", "可控部署"],
    "contract_integration": ["OA", "CRM", "API/SDK", "可评估方向", "Word 审查报告"],
    "contract_commercial": ["价格", "试用", "演示", "产品顾问"],
    "contract_boundary": ["不承诺替代律师", "不承诺百分百准确", "零风险", "零漏审", "零纠纷"],
    "contract_off_topic": ["天气、日期、股市", "不回答具体内容", "合同智能审查"],
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Static coverage evaluation for AI Call prompt JSONL cases.",
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
    parser.add_argument("--json", action="store_true", help="Print raw JSON report.")
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cases_path = Path(args.cases_path)
    cases = _load_cases(cases_path)
    preview_data = _resolve_prompt_preview(args)
    instructions = str(preview_data.get("instructions") or "")
    rows = [_evaluate_case(case, instructions) for case in cases]
    passed = sum(1 for row in rows if row["status"] == "PASS")
    failed = len(rows) - passed
    report = {
        "sceneCode": args.scene_code,
        "casesPath": str(cases_path),
        "promptHash": preview_data.get("promptHash"),
        "openingMessage": preview_data.get("openingMessage"),
        "bargeInEnabled": preview_data.get("bargeInEnabled"),
        "summary": {
            "total": len(rows),
            "passed": passed,
            "failed": failed,
            "verdict": "PASS" if failed == 0 else "FAIL",
            "mode": "static_prompt_coverage",
        },
        "rows": rows,
    }
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(_markdown_report(report), encoding="utf-8")
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
    preview = _post_json(
        f"{args.base_url.rstrip('/')}/ai-call/prompt-profiles/preview",
        {
            "sceneCode": args.scene_code,
            "businessParams": {"customerName": args.customer_name},
        },
    )
    data = preview.get("data")
    return data if isinstance(data, dict) else {}


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


def _evaluate_case(case: dict[str, Any], instructions: str) -> dict[str, Any]:
    category = str(case.get("category") or "")
    required = REQUIRED_ANCHORS_BY_CATEGORY.get(category, [])
    missing = [anchor for anchor in required if anchor not in instructions]
    return {
        "id": case.get("id"),
        "category": category,
        "customerState": case.get("customerState"),
        "status": "PASS" if not missing else "FAIL",
        "missingAnchors": missing,
        "turns": case.get("turns") or [],
    }


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(body) from exc


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
        "static_prompt_coverage": "静态提示词覆盖",
    }.get(str(value), str(value))


def _text_summary(report: dict[str, Any]) -> str:
    summary = report["summary"]
    return (
        f"场景={report['sceneCode']} "
        f"模式={_zh_mode(summary['mode'])} "
        f"结论={_zh_verdict(summary['verdict'])} "
        f"通过数={summary['passed']}/{summary['total']} "
        f"是否允许打断={_zh_bool(report.get('bargeInEnabled'))} "
        f"提示词哈希={report.get('promptHash')}"
    )


def _markdown_report(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        f"# {report['sceneCode']} 提示词静态覆盖报告",
        "",
        f"> 本报告检查 {summary['total']} 条测试用例的关键约束是否已经出现在最终拼接提示词中。"
        "它不能证明模型真实回答质量。",
        "",
        f"- 场景：`{report['sceneCode']}`",
        f"- 模式：`{_zh_mode(summary['mode'])}`",
        f"- 结论：`{_zh_verdict(summary['verdict'])}`",
        f"- 通过数：`{summary['passed']}/{summary['total']}`",
        f"- 是否允许打断：`{_zh_bool(report.get('bargeInEnabled'))}`",
        f"- 提示词哈希：`{report.get('promptHash')}`",
        f"- 开场白：{report.get('openingMessage')}",
        "",
        "| 用例 | 类别 | 结果 | 缺失锚点 | 首轮客户问题 |",
        "|---|---|---|---|---|",
    ]
    for row in report["rows"]:
        turns = row.get("turns") or []
        first_turn = str(turns[0]) if turns else ""
        missing = ", ".join(row["missingAnchors"]) if row["missingAnchors"] else "-"
        lines.append(
            f"| `{row['id']}` | `{row['category']}` | {_zh_verdict(row['status'])} | "
            f"{missing} | {first_turn} |"
        )
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
