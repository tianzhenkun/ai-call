"""
OSS 上传功能集成测试脚本（HTTP 接口版）

前置条件：
1. 项目已启动（start.bat）
2. MinIO 服务正常运行
3. sys_oss_config 表中存在 status='0' 的配置记录

执行命令: uv run python tests/test_oss_upload.py
"""

import json
import sys

import httpx

BASE_URL = "http://127.0.0.1:19010"
API_PREFIX = "/ai-call-api/v1"
UPLOAD_URL = f"{BASE_URL}{API_PREFIX}/system/oss/upload"
GET_URL = f"{BASE_URL}{API_PREFIX}/system/oss"


def print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def test_oss_upload() -> None:
    print_section("OSS 上传功能集成测试")

    test_content = b"Hello, OSS upload test from LingChen AI Call Base. " * 50
    original_filename = "test_upload.txt"
    content_type = "text/plain"

    # ---- Step 1: 上传 ----
    print("\n[1] 上传文件")
    print(f"    文件名: {original_filename}")
    print(f"    大小:   {len(test_content)} bytes")

    resp = httpx.post(
        UPLOAD_URL,
        files={"file": (original_filename, test_content, content_type)},
        timeout=30,
    )
    print(f"    状态码: {resp.status_code}")

    body = resp.json()
    print(f"    响应:   {json.dumps(body, ensure_ascii=False, indent=2)}")

    assert resp.status_code == 200, f"上传失败: {resp.text}"
    oss_id = body["data"]["ossId"]
    print(f"    oss_id: {oss_id}  ✓")

    # ---- Step 2: 验证 DB 记录 ----
    print("\n[2] 验证数据库记录")
    resp2 = httpx.get(f"{GET_URL}/{oss_id}", timeout=10)
    assert resp2.status_code == 200, f"查询失败: {resp2.text}"

    record = resp2.json()["data"]
    print(f"    fileName:     {record['fileName']}")
    print(f"    originalName: {record['originalName']}")
    print(f"    fileSuffix:   {record['fileSuffix']}")
    print(f"    url:          {record['url']}")
    print(f"    service:      {record['service']}")

    # ---- Step 3: 断言 ----
    print("\n[3] 断言")
    assert record["originalName"] == original_filename, "originalName 不匹配"
    assert record["fileSuffix"] == ".txt", "fileSuffix 不匹配"
    assert record["service"] == "minio", "service 不匹配"
    assert record["url"].endswith(".txt"), "url 后缀不匹配"
    print("    所有断言通过  ✓")

    print_section("测试通过")


if __name__ == "__main__":
    try:
        test_oss_upload()
    except Exception as e:
        print(f"\n测试失败: {e}")
        sys.exit(1)
