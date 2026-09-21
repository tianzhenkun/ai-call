from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import ClientCursor, sql
from test_ai_call_runtime_control_postgres import _psycopg_dsn


def test_handoff_exception_lease_migration_preserves_legacy_rows_and_is_repeatable():
    schema = f"handoff_migration_{uuid4().hex}"
    migration = (
        Path(__file__).resolve().parents[2]
        / "docs/livekit-ai-outbound/sql/handoff-exception-close-lease.sql"
    ).read_text(encoding="utf-8")
    with psycopg.connect(_psycopg_dsn(), autocommit=True, cursor_factory=ClientCursor) as db:
        db.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("set search_path to {}").format(sql.Identifier(schema)))
            db.execute("create table ai_call_handoff (handoff_id text primary key, status text)")
            db.execute("insert into ai_call_handoff values ('legacy-handoff', 'expired')")
            db.execute(migration)
            db.execute(migration)
            assert db.execute(
                "select handoff_id, status, exception_close_token, "
                "exception_close_expires_at, exception_prompt_completed_at from ai_call_handoff"
            ).fetchone() == ("legacy-handoff", "expired", None, None, None)
        finally:
            db.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(schema)))
