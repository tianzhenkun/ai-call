from datetime import date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from app.common.response import SuccessResponse, TableResponse
from app.core.exceptions import handle_exception


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


def test_sqlalchemy_error_response_does_not_expose_exception_detail() -> None:
    app = FastAPI()
    handle_exception(app)

    @app.get("/boom")
    async def boom() -> None:
        raise SQLAlchemyError("select * from ai_call_record")

    with TestClient(app) as client:
        response = client.get("/boom")

    body = response.json()
    assert response.status_code == 400
    assert body["code"] == 500
    assert body["msg"] == "数据库操作失败: SQLAlchemyError"
    assert body["data"] is None
