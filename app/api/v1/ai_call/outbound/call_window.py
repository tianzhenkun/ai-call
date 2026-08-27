from __future__ import annotations

import json
from datetime import datetime, timezone, tzinfo

from .rule_task_model import AiCallOutboundTaskModel


def task_allows_call_at(
    task: AiCallOutboundTaskModel,
    now: datetime,
    business_timezone: tzinfo,
) -> bool:
    """按任务创建时固化的规则快照判断当前是否允许外呼。"""
    try:
        snapshot = json.loads(task.config_snapshot_json)
        windows = snapshot["rule"].get("callWindows")
    except (KeyError, TypeError, json.JSONDecodeError):
        return False
    if windows is None:
        return True
    if not isinstance(windows, list) or not windows:
        return False
    aware_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    current_time = aware_now.astimezone(business_timezone).strftime("%H:%M")
    return any(
        isinstance(window, dict)
        and isinstance(window.get("startTime"), str)
        and isinstance(window.get("endTime"), str)
        and window["startTime"] <= current_time < window["endTime"]
        for window in windows
    )
