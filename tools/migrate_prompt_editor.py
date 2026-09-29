"""提示词编辑器 PostgreSQL 迁移；默认演练并回滚，--apply 才提交。"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.ai_call.prompt_editor import canonical_content

SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs/livekit-ai-outbound/sql/phase-b4-prompt-editor-postgres.sql"
)


def migrate(connection) -> dict:
    connection.execute("LOCK TABLE ai_call_prompt_profile IN ACCESS EXCLUSIVE MODE")
    names = connection.execute("SELECT id, tenant_id, name, to_jsonb(p)->>'deleted_at' AS deleted_at "
                               "FROM ai_call_prompt_profile p").fetchall()
    active_seen = {}
    for row in names:
        name = row["name"].strip()
        if row["deleted_at"] is None:
            if not name:
                raise ValueError(f"场景 {row['id']} 名称为空，请先处理")
            key = (row["tenant_id"], name)
            if key in active_seen:
                raise ValueError(f"场景名称规范化后重复：{active_seen[key]}、{row['id']}")
            active_seen[key] = row["id"]
    connection.execute(SQL_PATH.read_text())
    for row in names:
        if row["name"] != row["name"].strip():
            connection.execute(
                "UPDATE ai_call_prompt_profile SET name = %s WHERE id = %s",
                (row["name"].strip(), row["id"]),
            )
    report = {
        "profilesChecked": 0,
        "repairedPointers": [],
        "unmatchedPointers": [],
        "incompleteLegacyProfiles": [],
    }
    last_id = 0
    while True:
        profiles = connection.execute(
            "SELECT * FROM ai_call_prompt_profile WHERE id > %s AND creation_key IS NULL ORDER BY id LIMIT 500",
            (last_id,),
        ).fetchall()
        if not profiles:
            return report
        for profile in profiles:
            last_id = profile["id"]
            report["profilesChecked"] += 1
            snapshot = {
                camel: profile[snake]
                for snake, camel in (
                    ("scene_code", "sceneCode"),
                    ("name", "name"),
                    ("provider_key", "providerKey"),
                    ("prompt_text", "promptText"),
                    ("opening_message", "openingMessage"),
                    ("opening_barge_in_enabled", "openingBargeInEnabled"),
                    ("product_info", "productInfo"),
                )
            }
            snapshot["variables"] = json.loads(profile["variables_json"] or "[]")
            versions = connection.execute(
                "SELECT id, snapshot_json FROM ai_call_prompt_profile_version WHERE tenant_id = %s AND profile_id = %s "
                "AND deleted_at IS NULL ORDER BY version_no DESC",
                (profile["tenant_id"], profile["id"]),
            ).fetchall()
            matches = [
                version["id"]
                for version in versions
                if canonical_content(json.loads(version["snapshot_json"]))
                == canonical_content(snapshot)
            ]
            current = profile["current_version_id"]
            selected = current if current in matches else (matches[0] if matches else None)
            if selected != current:
                connection.execute(
                    "UPDATE ai_call_prompt_profile SET current_version_id = %s WHERE id = %s",
                    (selected, profile["id"]),
                )
                report["repairedPointers"].append(str(profile["id"]))
            if selected is None:
                report["unmatchedPointers"].append(str(profile["id"]))
            if profile["provider_key"] == "static_profile" and (
                not profile["opening_message"] or not profile["prompt_text"]
            ):
                report["incompleteLegacyProfiles"].append(str(profile["id"]))


def check_rollback(connection) -> dict:
    counts = connection.execute(
        "SELECT count(*) FILTER (WHERE lifecycle_status = 'DRAFT') AS drafts, "
        "count(*) FILTER (WHERE deleted_at IS NOT NULL) AS deleted FROM ai_call_prompt_profile"
    ).fetchone()
    return {"allowed": counts["drafts"] == 0 and counts["deleted"] == 0, **counts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", default="AI_CALL_TEST_POSTGRES_DSN")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--check-rollback", action="store_true", help="只读检查旧应用回退是否会暴露草稿或已删除场景"
    )
    args = parser.parse_args()
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        parser.error(f"缺少连接环境变量 {args.dsn_env}")
    with psycopg.connect(
        dsn.replace("postgresql+asyncpg://", "postgresql://"), row_factory=dict_row
    ) as connection:
        if args.check_rollback:
            report = check_rollback(connection)
            print(json.dumps(report, ensure_ascii=False))
            if not report["allowed"]:
                raise SystemExit("拒绝回退到无草稿和删除过滤能力的应用；保留当前数据库及兼容后端")
            return
        report = migrate(connection)
        if args.apply:
            connection.commit()
        else:
            connection.rollback()
        print(json.dumps({"applied": args.apply, **report}, ensure_ascii=False))


if __name__ == "__main__":
    main()
