"""
测试驼峰命名转换

执行命令: pytest tests/test_camel_case.py -v
"""

import pytest
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class ExampleUserSchema(BaseModel):
    """测试用户 Schema"""

    model_config = ConfigDict(
        from_attributes=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    user_id: int = Field(description="用户ID")
    user_name: str = Field(description="用户名")
    nick_name: str = Field(description="昵称")
    created_time: str | None = Field(default=None, description="创建时间")


def test_camel_case_conversion():
    """测试驼峰命名转换"""
    user = ExampleUserSchema(
        user_id=1, user_name="admin", nick_name="管理员", created_time="2024-01-01 12:00:00"
    )

    result = user.model_dump(by_alias=True)

    assert "userId" in result
    assert "userName" in result
    assert "nickName" in result
    assert "createdTime" in result
    assert result["userId"] == 1
    assert result["userName"] == "admin"
    assert result["nickName"] == "管理员"

    print("\n✅ 驼峰命名转换测试通过:")
    print("   输入: user_id, user_name, nick_name, created_time")
    print(f"   输出: {list(result.keys())}")


def test_snake_case_input():
    """测试下划线格式输入"""
    user = ExampleUserSchema(user_id=2, user_name="test", nick_name="测试用户")

    result = user.model_dump(by_alias=True)

    assert result["userId"] == 2
    assert result["userName"] == "test"
    print("\n✅ 下划线格式输入测试通过")


def test_camel_case_input():
    """测试驼峰格式输入"""
    user = ExampleUserSchema(userId=3, userName="camel", nickName="驼峰用户")

    result = user.model_dump(by_alias=True)

    assert result["userId"] == 3
    assert result["userName"] == "camel"
    print("\n✅ 驼峰格式输入测试通过")


def test_json_parse_snake_case():
    """测试 JSON 解析下划线格式"""

    json_str = '{"user_id": 4, "user_name": "json_test", "nick_name": "JSON测试"}'
    user = ExampleUserSchema.model_validate_json(json_str)

    result = user.model_dump(by_alias=True)

    assert result["userId"] == 4
    assert result["userName"] == "json_test"
    print("\n✅ JSON 下划线格式解析测试通过")


def test_json_parse_camel_case():
    """测试 JSON 解析驼峰格式"""

    json_str = '{"userId": 5, "userName": "json_camel", "nickName": "JSON驼峰"}'
    user = ExampleUserSchema.model_validate_json(json_str)

    result = user.model_dump(by_alias=True)

    assert result["userId"] == 5
    assert result["userName"] == "json_camel"
    print("\n✅ JSON 驼峰格式解析测试通过")


if __name__ == "__main__":
    pytest.main(["-v", "tests/test_camel_case.py"])
