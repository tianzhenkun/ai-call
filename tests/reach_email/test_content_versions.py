import asyncio
import json

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import versions
from app.services.reach_email.migrate import migrate_tables
from app.services.reach_email.models import Import, Version
from app.services.reach_email.schema import Content, ContentInput, RestoreInput, TaskInput
from app.services.reach_email.service import EmailService


def test_empty_email_never_creates_history_and_legacy_empty_rows_do_not_take_pages(task_settings):
    async def run():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, "tenant", "owner", "")
            imported = service.add(
                Import, filename="test.xlsx",
                report=json.dumps({"canCreate": True, "validCount": 1,
                                   "records": [{"values": {"邮箱": "a@example.com"}}]}),
            )
            await db.flush()
            data = TaskInput(name="任务", importId=imported.id, settings=task_settings)
            task = await service.create_task(data)
            assert not (await db.scalars(service.query(Version))).all()
            task = await service.update_task(task["id"], data.model_copy(update={"version": 1}))
            assert not (await db.scalars(service.query(Version))).all()
            task = await service.save_content(task["id"], ContentInput(
                version=task["version"], subject="  ", html="<p><br>&nbsp;</p>",
                signature="<p>仅签名</p>", signatureName="签名",
            ))
            assert not (await db.scalars(service.query(Version))).all()
            for subject, html in (("只有主题", ""), ("", "<p>只有正文</p>")):
                task = await service.save_content(task["id"], ContentInput(
                    version=task["version"], subject=subject, html=html,
                ))
            # 历史空记录保留存储，但不挤占可恢复版本及分页总数。
            service.add(Version, task_id=task["id"], version=99,
                        content=json.dumps({"subject": "", "html": "<p>&nbsp;</p>"}))
            await db.flush()
            first = json.loads((await versions(task["id"], service, pageSize=1)).body)["data"]
            second = json.loads((await versions(task["id"], service, page=2, pageSize=1)).body)["data"]
            assert first["total"] == second["total"] == 2
            assert first["items"][0]["content"]["html"] == "<p>只有正文</p>"
            assert second["items"][0]["content"]["subject"] == "只有主题"
        await engine.dispose()

    asyncio.run(run())


def test_settings_signature_and_unchanged_saves_do_not_add_email_versions(task_settings):
    async def run():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, "tenant", "owner", "")
            imported = service.add(
                Import, filename="test.xlsx",
                report=json.dumps({"canCreate": True, "validCount": 1,
                                   "records": [{"values": {"邮箱": "a@example.com"}}]}),
            )
            await db.flush()
            task = await service.create_task(TaskInput(
                name="任务", importId=imported.id, settings=task_settings,
            ))
            task = await service.save_content(task["id"], ContentInput(
                version=task["version"], subject="交流邀请", html="<p>您好</p>",
            ))
            first = (await db.scalars(service.query(Version))).one()
            for change in ({}, {"signature": "<p>商务签名</p>", "signatureName": "公司签名"}):
                task = await service.save_content(task["id"], ContentInput(
                    **{**task["content"], **change}, version=task["version"],
                ))
                assert (await db.scalars(service.query(Version))).all() == [first]
            task = await service.update_task(task["id"], TaskInput(
                name="修改任务名称", importId=imported.id, version=task["version"],
                settings={**task_settings, "dailyLimit": 80},
                content=Content.model_validate(task["content"]),
            ))
            assert (await db.scalars(service.query(Version))).all() == [first]
            for change in ({"subject": "新主题"}, {"html": "<p>新的正文</p>"}):
                task = await service.save_content(task["id"], ContentInput(
                    **{**task["content"], **change}, version=task["version"],
                ))
            assert len((await db.scalars(service.query(Version))).all()) == 3
            for expected_count in (4, 4):
                task = await service.restore(task["id"], RestoreInput(
                    version=task["version"], versionId=first.id,
                ))
                assert task["content"] == json.loads(first.content)
                assert len((await db.scalars(service.query(Version))).all()) == expected_count
        await engine.dispose()

    asyncio.run(run())
