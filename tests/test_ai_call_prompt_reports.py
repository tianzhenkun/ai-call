from __future__ import annotations

from tools.ai_call_prompt_case_llm_eval import (
    _html_report as llm_html_report,
)
from tools.ai_call_prompt_case_llm_eval import (
    _markdown_report as llm_markdown_report,
)
from tools.ai_call_prompt_case_llm_eval import (
    _preview_from_instructions_path as llm_preview_from_instructions_path,
)
from tools.ai_call_prompt_case_static_eval import (
    REQUIRED_ANCHORS_BY_CATEGORY,
)
from tools.ai_call_prompt_case_static_eval import (
    _markdown_report as static_markdown_report,
)
from tools.ai_call_prompt_case_static_eval import (
    _preview_from_instructions_path as static_preview_from_instructions_path,
)


def test_prompt_report_tools_can_use_candidate_instructions_file(tmp_path) -> None:
    instructions_path = tmp_path / "candidate-prompt.md"
    instructions_path.write_text("候选提示词：海外获客智能体", encoding="utf-8")

    static_preview = static_preview_from_instructions_path(
        instructions_path,
        opening_message="您好张总，请问现在方便吗？",
        barge_in_enabled=False,
    )
    llm_preview = llm_preview_from_instructions_path(
        instructions_path,
        opening_message="您好张总，请问现在方便吗？",
        barge_in_enabled=False,
    )

    assert static_preview == llm_preview
    assert static_preview["instructions"] == "候选提示词：海外获客智能体"
    assert static_preview["openingMessage"] == "您好张总，请问现在方便吗？"
    assert static_preview["bargeInEnabled"] is False
    assert static_preview["promptHash"].startswith("sha256:")


def test_static_prompt_coverage_report_uses_chinese_copy() -> None:
    report = {
        "sceneCode": "intro_geo",
        "promptHash": "sha256:test",
        "openingMessage": "您好张总，请问现在方便吗？",
        "bargeInEnabled": False,
        "summary": {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "verdict": "PASS",
            "mode": "static_prompt_coverage",
        },
        "rows": [
            {
                "id": "case_001",
                "category": "availability",
                "status": "PASS",
                "missingAnchors": [],
                "turns": ["我现在没空。"],
            }
        ],
    }

    markdown = static_markdown_report(report)

    assert "# intro_geo 提示词静态覆盖报告" in markdown
    assert "本报告检查" in markdown
    assert "- 场景：" in markdown
    assert "- 是否允许打断：`否`" in markdown
    assert "| 用例 | 类别 | 结果 | 缺失锚点 | 首轮客户问题 |" in markdown
    assert "| `case_001` | `availability` | 通过 | - | 我现在没空。 |" in markdown
    assert "Prompt Static Coverage Report" not in markdown
    assert "Barge-in enabled" not in markdown


def test_llm_simulation_report_uses_chinese_copy() -> None:
    report = {
        "sceneCode": "intro_geo",
        "mode": "llm_simulation",
        "model": "qwen-plus",
        "promptHash": "sha256:test",
        "openingMessage": "您好张总，请问现在方便吗？",
        "bargeInEnabled": False,
        "summary": {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "verdict": "PASS",
        },
        "rows": [
            {
                "id": "case_001",
                "category": "availability",
                "verdict": "PASS",
                "reason": "符合预期",
                "responses": [
                    {"customer": "我现在没空。", "assistant": "好的，您先忙。"},
                    {"customer": "那你们之后怎么联系？", "assistant": "顾问后续联系您。"},
                ],
            }
        ],
    }

    markdown = llm_markdown_report(report)

    assert "# intro_geo 提示词文本模型模拟报告" in markdown
    assert "不等同于真实 Qwen Realtime 电话链路验收" in markdown
    assert "- 模型：`qwen-plus`" in markdown
    assert "- 是否允许打断：`否`" in markdown
    assert "| 用例 | 类别 | 结果 | 客户问题/追问 | 判定原因 | 首轮 AI 回复 |" in markdown
    assert (
        "| `case_001` | `availability` | 通过 | "
        "我现在没空。<br>那你们之后怎么联系？ | 符合预期 | 好的，您先忙。 |"
    ) in markdown
    assert "Prompt LLM Simulation Report" not in markdown
    assert "Barge-in enabled" not in markdown


def test_llm_simulation_report_can_render_visual_html() -> None:
    report = {
        "sceneCode": "intro_geo",
        "mode": "llm_simulation",
        "model": "qwen-plus",
        "promptHash": "sha256:test",
        "openingMessage": "您好张总，请问现在方便吗？",
        "bargeInEnabled": False,
        "summary": {
            "total": 2,
            "passed": 1,
            "failed": 1,
            "verdict": "REVIEW",
        },
        "rows": [
            {
                "id": "case_pass",
                "category": "availability",
                "verdict": "PASS",
                "reason": "符合预期",
                "responses": [
                    {"customer": "我现在没空。", "assistant": "好的，您先忙。"},
                    {"customer": "后面怎么联系？", "assistant": "顾问后续联系您。"},
                ],
            },
            {
                "id": "case_review",
                "category": "off_topic",
                "verdict": "REVIEW",
                "reason": "回答了无关问题",
                "responses": [{"customer": "今天上海天气怎么样？", "assistant": "天气晴。"}],
            },
        ],
    }

    html = llm_html_report(report)

    assert "<!doctype html>" in html
    assert "<title>intro_geo 提示词文本模型模拟报告</title>" in html
    assert "搜索用例、类别、客户问题或回复" in html
    assert 'data-filter="通过"' in html
    assert 'data-filter="需复核"' in html
    assert 'data-category="availability"' in html
    assert "我现在没空。" in html
    assert "后面怎么联系？" in html
    assert "好的，您先忙。" in html
    assert "回答了无关问题" in html
    assert "1 / 2" in html
    assert "开场白" in html
    assert "您好张总，请问现在方便吗？" in html


def test_prompt_reports_use_scene_code_in_title() -> None:
    static_report = {
        "sceneCode": "intro_overseas",
        "promptHash": "sha256:test",
        "openingMessage": "您好张总，请问现在方便吗？",
        "bargeInEnabled": False,
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "verdict": "PASS",
            "mode": "static_prompt_coverage",
        },
        "rows": [],
    }
    llm_report = {
        "sceneCode": "intro_overseas",
        "mode": "llm_simulation",
        "model": "qwen-plus",
        "promptHash": "sha256:test",
        "openingMessage": "您好张总，请问现在方便吗？",
        "bargeInEnabled": False,
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "verdict": "PASS",
        },
        "rows": [],
    }

    assert "# intro_overseas 提示词静态覆盖报告" in static_markdown_report(static_report)
    assert "# intro_overseas 提示词文本模型模拟报告" in llm_markdown_report(llm_report)
    assert "<title>intro_overseas 提示词文本模型模拟报告</title>" in llm_html_report(llm_report)


def test_static_prompt_coverage_knows_document_review_categories() -> None:
    expected_categories = {
        "document_availability",
        "document_basic",
        "document_method",
        "document_rules",
        "document_metrics",
        "document_data_security",
        "document_integration",
        "document_commercial",
        "document_boundary",
        "document_off_topic",
    }

    for category in expected_categories:
        anchors = REQUIRED_ANCHORS_BY_CATEGORY.get(category)
        assert anchors, category


def test_static_prompt_coverage_knows_contract_review_categories() -> None:
    expected_categories = {
        "contract_availability",
        "contract_reject",
        "contract_basic",
        "contract_method",
        "contract_rules",
        "contract_metrics",
        "contract_data_security",
        "contract_integration",
        "contract_commercial",
        "contract_boundary",
        "contract_off_topic",
    }

    for category in expected_categories:
        anchors = REQUIRED_ANCHORS_BY_CATEGORY.get(category)
        assert anchors, category
