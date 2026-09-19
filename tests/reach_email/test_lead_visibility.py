import asyncio
import json

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.v1.reach_email.controller import leads, tasks
from app.services.reach_email.migrate import migrate_tables
from app.services.reach_email.models import Lead, Task
from app.services.reach_email.service import EmailService


def test_leads_and_task_choices_share_execution_filter_before_pagination():
    async def run():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(migrate_tables)
        async with async_sessionmaker(engine, expire_on_commit=False)() as db:
            service = EmailService(db, "tenant", "owner", "")
            task_ids = {}
            for status in ("unstarted", "scheduled", "running", "ended"):
                task = service.add(
                    Task, name=status, import_id="import", settings="{}", status=status
                )
                task_ids[status] = task.id
                service.add(Lead, task_id=task.id, email=f"{status}@example.com", values="{}")
            other = EmailService(db, "tenant", "other", "")
            foreign = other.add(
                Task, name="foreign", import_id="import", settings="{}", status="running"
            )
            other.add(Lead, task_id=foreign.id, email="foreign@example.com", values="{}")
            await db.flush()

            def data(response):
                return json.loads(response.body)["data"]

            first = data(await leads(service, pageSize=1, classification="following"))
            second = data(await leads(service, page=2, pageSize=1, classification="following"))
            assert first["total"] == second["total"] == 2
            assert {first["items"][0]["taskId"], second["items"][0]["taskId"]} == {
                task_ids["running"], task_ids["ended"]
            }
            assert data(await leads(service, taskId=task_ids["unstarted"]))["total"] == 0
            assert data(await leads(service, email="running"))["total"] == 1
            assert data(await leads(service, classification="interested"))["total"] == 0
            choices = data(await tasks(service, forLeads=True))
            assert {item["id"] for item in choices["items"]} == {
                task_ids["running"], task_ids["ended"]
            }
            assert data(await tasks(service))["total"] == 4
            draft = await service.task(task_ids["unstarted"])
            draft.status = "running"
            await db.flush()
            assert data(await leads(service))["total"] == 3
            draft.status = "unstarted"
            await db.flush()
            assert data(await leads(service))["total"] == 2
        await engine.dispose()

    asyncio.run(run())
