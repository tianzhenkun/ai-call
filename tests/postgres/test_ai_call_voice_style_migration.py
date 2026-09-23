from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import ClientCursor, sql
from test_ai_call_runtime_control_postgres import _psycopg_dsn


def test_voice_style_migration_preserves_legacy_rows_and_is_repeatable():
    schema = f"voice_style_{uuid4().hex}"
    migration = (
        Path(__file__).resolve().parents[2]
        / "docs/livekit-ai-outbound/sql/voice-speaking-style-postgres.sql"
    ).read_text(encoding="utf-8")
    with psycopg.connect(_psycopg_dsn(), autocommit=True, cursor_factory=ClientCursor) as db:
        db.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        try:
            db.execute(sql.SQL("set search_path to {}").format(sql.Identifier(schema)))
            db.execute("create table ai_call_tenant_voice_profile (id bigint primary key, voice text)")
            db.execute("insert into ai_call_tenant_voice_profile values (1, 'legacy')")
            db.execute(migration)
            db.execute("update ai_call_tenant_voice_profile set speaking_style = 'gentle' where id = 1")
            db.execute(migration)
            assert db.execute("select voice, speaking_style from ai_call_tenant_voice_profile").fetchone() == ("legacy", "gentle")
            db.execute("insert into ai_call_tenant_voice_profile (id, voice) values (2, 'new')")
            assert db.execute("select speaking_style from ai_call_tenant_voice_profile where id = 2").fetchone() == ("natural",)
            with pytest.raises(psycopg.errors.CheckViolation):
                db.execute("update ai_call_tenant_voice_profile set speaking_style = 'arbitrary' where id = 1")
        finally:
            db.execute(sql.SQL("drop schema {} cascade").format(sql.Identifier(schema)))
