"""
API 接口测试脚本（无需认证）

测试步骤:
1. 直接调用用户列表接口
2. 验证返回数据格式（驼峰命名）

执行命令: python tests/test_api.py
"""

import httpx
import json

BASE_URL = "http://127.0.0.1:19010"
API_PREFIX = "/ai-call-api/v1"


def print_response(name: str, response: httpx.Response):
    """打印响应信息"""
    print(f"\n{'='*60}")
    print(f"📡 {name}")
    print(f"{'='*60}")
    print(f"状态码: {response.status_code}")
    print(f"响应内容:")
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
    
    if data:
        rows = data.get("rows", [])
        total = data.get("total", 0)
        
        print(f"\n📊 统计信息:")
        print(f"   总记录数: {total}")
        print(f"   返回记录数: {len(rows)}")
        
        if rows:
            print(f"\n📝 字段命名检查:")
            first_row = rows[0]
            for key in list(first_row.keys())[:10]:
                print(f"   - {key}")
            
            has_camel = any(key[0].islower() and '_' not in key and any(c.isupper() for c in key) for key in first_row.keys())
            has_snake = any('_' in key for key in first_row.keys())
            
            print(f"\n✅ 驼峰命名: {'是' if has_camel else '否'}")
            print(f"{'❌' if has_snake else '✅'} 下划线命名: {'是' if has_snake else '否'}")
            
            if has_camel and not has_snake:
                print("\n🎉 测试通过：返回数据使用驼峰命名格式")
            else:
                print("\n⚠️ 测试警告：返回数据命名格式不符合预期")
        
        return data
    
    return None


def test_dict_list_no_auth():
    """测试字典列表接口（无需认证）"""
    url = f"{BASE_URL}{API_PREFIX}/system/dict/data/type/sys_normal_disable"
    
    response = httpx.get(url, timeout=30)
    
    data = print_response("字典列表接口", response)
    
    if data:
        rows = data.get("rows", [])
        if rows:
            print(f"\n📝 字典字段命名检查:")
            first_row = rows[0]
            for key in list(first_row.keys())[:10]:
                print(f"   - {key}")
    
    return data


def main():
    """主测试流程"""
    print("\n" + "="*60)
    print("🚀 开始 API 接口测试（无需认证）")
    print("="*60)
    
    # 测试用户列表
    test_user_list_no_auth()
    
    # 测试字典列表
    test_dict_list_no_auth()
    
    print("\n" + "="*60)
    print("🏁 测试完成")
    print("="*60)


if __name__ == "__main__":
    main()
