from sqlalchemy import JSON, and_, case, or_, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from app.services.ai_call.call_outcome import VOICEMAIL_MARKERS

from .model import AiCallSemanticAnalysisModel


class _AnalysisJson(FunctionElement):
    type = JSON()
    inherit_cache = True


@compiles(_AnalysisJson)
def _compile_analysis_json(element, compiler, **kwargs):
    return f"CAST({compiler.process(element.clauses, **kwargs)} AS JSON)"


@compiles(_AnalysisJson, "postgresql")
def _compile_analysis_json_postgresql(element, compiler, **kwargs):
    return f"CAST({compiler.process(element.clauses, **kwargs)} AS JSONB)"


@compiles(_AnalysisJson, "sqlite")
def _compile_analysis_json_sqlite(element, compiler, **kwargs):
    # SQLite 的 JSON_EXTRACT 可直接读取文本，CAST AS JSON 会把文本转为数字。
    return compiler.process(element.clauses, **kwargs)


def voicemail_expression(call_id):
    analysis = AiCallSemanticAnalysisModel
    result = _AnalysisJson(analysis.analysis_result)
    return (
        select(analysis.id)
        .where(
            analysis.call_id == call_id,
            analysis.analysis_scene_code == "ai_call_semantic_analysis",
            analysis.analysis_status == "2",
            result["valid_dialogue"].as_boolean().is_not(True),
            or_(
                *(
                    result[field].as_string().contains(marker)
                    for field in ("summary", "reason", "tags", "key_points")
                    for marker in VOICEMAIL_MARKERS
                )
            ),
        )
        .exists()
    )


def business_call_result_expression(call_result, call_id):
    """列表筛选与统计复用业务结果口径，不修改线路返回的原始结果。"""
    return case(
        (
            and_(call_result.in_({"connected", "early_hangup"}), voicemail_expression(call_id)),
            "no_answer",
        ),
        else_=call_result,
    )
