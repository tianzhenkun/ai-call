"""
字典接口测试脚本

执行命令: python tests/test_dict_api.py
"""

import json

import httpx

BASE_URL = "http://127.0.0.1:19010"
API_PREFIX = "/ai-call-api/v1"


def print_response(name: str, response: httpx.Response):
    """打印响应信息"""
    print(f"\n{'=' * 60}")
    print(f"📡 {name}")
    print(f"{'=' * 60}")
    print(f"状态码: {response.status_code}")
    try:
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return data
    except Exception:
        print(response.text)
        return None


def test_dict_type_list():
    """测试字典类型列表（从缓存获取）"""
    url = f"{BASE_URL}{API_PREFIX}/system/dict/data/info/sys_normal_disable"
    response = httpx.get(url, timeout=30)
    data = print_response("字典类型列表（缓存）", response)
    assert response.status_code == 200
    assert data is not None
    assert data["code"] == 200
    assert isinstance(data.get("data"), list)
    assert data["data"]


def test_dict_type_list_query():
    """测试字典类型列表（带查询参数）"""
    url = f"{BASE_URL}{API_PREFIX}/system/dict/data/type/sys_normal_disable"
    response = httpx.get(url, timeout=30)
    data = print_response("字典类型列表（查询）", response)
    assert response.status_code == 200
    assert data is not None
    assert data["code"] == 200
    assert isinstance(data.get("data"), list)
    assert data["data"]


def main():
    """主测试流程"""
    print("\n" + "=" * 60)
    print("🚀 开始字典接口测试")
    print("=" * 60)

    test_dict_type_list()
    test_dict_type_list_query()

    print("\n" + "=" * 60)
    print("🏁 测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
