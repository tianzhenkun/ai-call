from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import ClientCursor, sql
from test_ai_call_runtime_control_postgres import _psycopg_dsn


def test_opening_barge_in_migration_defaults_on_and_preserves_saved_false():
    schema = f"opening_barge_in_{uuid4().hex}"
    migration = (
        Path(__file__).resolve().parents[2]
        / "docs/livekit-ai-outbound/sql/prompt-opening-barge-in-postgres.sql"
    ).read_text(encoding="utf-8")
    with psycopg.connect(_psycopg_dsn(), autocommit=True, cursor_factory=ClientCursor) as db:
        db.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("set search_path to {}").format(sql.Identifier(schema)))
            db.execute("create table ai_call_prompt_profile (id bigint primary key)")
            db.execute("insert into ai_call_prompt_profile values (1)")
            db.execute(migration)
            assert db.execute("select opening_barge_in_enabled from ai_call_prompt_profile").fetchone() == (True,)
            db.execute("update ai_call_prompt_profile set opening_barge_in_enabled = false where id = 1")
            db.execute(migration)
            assert db.execute("select opening_barge_in_enabled from ai_call_prompt_profile").fetchone() == (False,)
            db.execute("insert into ai_call_prompt_profile (id) values (2)")
            assert db.execute("select opening_barge_in_enabled from ai_call_prompt_profile where id = 2").fetchone() == (True,)
            with pytest.raises(psycopg.errors.NotNullViolation):
                db.execute("update ai_call_prompt_profile set opening_barge_in_enabled = null where id = 1")
        finally:
            db.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(schema)))
