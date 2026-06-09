from datetime import date, datetime

from app.common.response import SuccessResponse, TableResponse


def test_success_response_serializes_date_values() -> None:
    response = SuccessResponse(
        data={
            "createTime": date(2026, 3, 17),
            "updateTime": datetime(2026, 3, 17, 13, 30, 45),
        }
    )

    assert b'"createTime":"2026-03-17"' in response.body
    assert b'"updateTime":"2026-03-17T13:30:45"' in response.body


def test_table_response_serializes_date_values() -> None:
    response = TableResponse(
        rows=[{"createTime": date(2026, 3, 17)}],
        total=1,
    )

    assert b'"createTime":"2026-03-17"' in response.body
