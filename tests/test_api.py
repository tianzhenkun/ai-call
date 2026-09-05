"""
API 接口测试脚本（无需认证）

测试步骤:
1. 直接调用用户列表接口
2. 验证返回数据格式（驼峰命名）

执行命令: python tests/test_api.py
"""

import json
import os

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("AI_CALL_RUN_LIVE_HTTP_TESTS") != "1",
    reason="现场 HTTP 测试需显式设置 AI_CALL_RUN_LIVE_HTTP_TESTS=1",
)

BASE_URL = os.getenv("AI_CALL_INTEGRATION_BASE_URL", "http://127.0.0.1:19013")
API_PREFIX = "/reach-api/v1"


def print_response(name: str, response: httpx.Response):
    """打印响应信息"""
    print(f"\n{'=' * 60}")
    print(f"📡 {name}")
    print(f"{'=' * 60}")
    print(f"状态码: {response.status_code}")
    print("响应内容:")
    try:
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return data
    except Exception:
        print(response.text)
        return None


def test_user_list_no_auth():
    """测试用户列表接口（无需认证）"""
    url = f"{BASE_URL}{API_PREFIX}/system/user/list"

    response = httpx.get(url, timeout=30)

    data = print_response("用户列表接口", response)
    assert response.status_code == 200
    assert data is not None
    assert data["code"] == 200
    assert isinstance(data.get("rows"), list)
    assert isinstance(data.get("total"), int)

    rows = data.get("rows", [])
    total = data.get("total", 0)

    print("\n📊 统计信息:")
    print(f"   总记录数: {total}")
    print(f"   返回记录数: {len(rows)}")

    if rows:
        print("\n📝 字段命名检查:")
        first_row = rows[0]
        for key in list(first_row.keys())[:10]:
            print(f"   - {key}")

        has_camel = any(
            key[0].islower() and "_" not in key and any(c.isupper() for c in key)
            for key in first_row
        )
        has_snake = any("_" in key for key in first_row)

        print(f"\n✅ 驼峰命名: {'是' if has_camel else '否'}")
        print(f"{'❌' if has_snake else '✅'} 下划线命名: {'是' if has_snake else '否'}")

        assert has_camel
        assert not has_snake


def test_dict_list_no_auth():
    """测试字典列表接口（无需认证）"""
    url = f"{BASE_URL}{API_PREFIX}/system/dict/data/type/sys_normal_disable"

    response = httpx.get(url, timeout=30)

    data = print_response("字典列表接口", response)
    assert response.status_code == 200
    assert data is not None
    assert data["code"] == 200
    assert isinstance(data.get("data"), list)

    rows = data.get("data", [])
    if rows:
        print("\n📝 字典字段命名检查:")
        first_row = rows[0]
        for key in list(first_row.keys())[:10]:
            print(f"   - {key}")

        assert "dictLabel" in first_row
        assert "dictValue" in first_row


def main():
    """主测试流程"""
    print("\n" + "=" * 60)
    print("🚀 开始 API 接口测试（无需认证）")
    print("=" * 60)

    # 测试用户列表
    test_user_list_no_auth()

    # 测试字典列表
    test_dict_list_no_auth()

    print("\n" + "=" * 60)
    print("🏁 测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
