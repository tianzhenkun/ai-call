from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import timedelta, timezone
from io import BytesIO
from pathlib import Path

from sqlalchemy import delete, func, select, update

from app.services.reach_email.ai import PROMPT_VERSION
from app.services.reach_email.content import (
    clean_html,
    has_email_content,
    parse_recipients,
    render_content,
    validate_recipient_domains,
    validate_variables,
)
from app.services.reach_email.models import (
    Account,
    AIJob,
    Attachment,
    Attempt,
    Import,
    Lead,
    Message,
    Suppression,
    Task,
    Version,
    new_id,
    now,
)
from app.services.reach_email.schema import Content, TaskSettings
from app.services.reach_email.security import CredentialCipher


def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def iso(value):
    return value.replace(tzinfo=timezone.utc).isoformat() if value else None


def scope(model, tenant, owner):
    return (model.tenant_id == tenant, model.owner_id == owner)


class EmailService:
    def __init__(self, db, tenant: str, owner: str, encryption_key: str, store=None):
        if not tenant or not owner:
            raise ValueError("缺少有效用户或租户身份")
        self.db, self.tenant, self.owner, self.key, self.store = (
            db,
            str(tenant),
            str(owner),
            encryption_key,
            store,
        )

    def query(self, model):
        return select(model).where(*scope(model, self.tenant, self.owner))

    def add(self, model, **values):
        row = model(id=new_id(), tenant_id=self.tenant, owner_id=self.owner, **values)
        self.db.add(row)
        return row

    async def snapshot_content(self, task):
        content = json.loads(task.content)
        if not has_email_content(content):
            return
        latest = await self.db.scalar(
            self.query(Version).where(Version.task_id == task.id)
            .order_by(Version.version.desc()).limit(1)
        )
        # 邮件版本仅在主题或正文变化时记录，签名、附件及设置变化不单独生成版本。
        if latest and all(
            json.loads(latest.content).get(field, "") == content.get(field, "")
            for field in ("subject", "html")
        ):
            return
        self.add(Version, task_id=task.id, content=task.content, version=task.version)

    async def owned(self, model, identifier, *, lock=False):
        query = self.query(model).where(model.id == identifier)
        if lock:
            query = query.with_for_update()
        row = await self.db.scalar(query)
        if row is None:
            raise ValueError("记录不存在或无权访问")
        return row

    async def task(self, identifier, *, lock=False):
        return await self.owned(Task, identifier, lock=lock)

    async def page(self, model, page=1, page_size=20, filters=()):
        query = self.query(model).where(*filters)
        count = await self.db.scalar(select(func.count()).select_from(query.subquery()))
        rows = (
            await self.db.scalars(
                query
                .order_by(model.created_at.desc(), model.id)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        ).all()
        return rows, {"total": count, "page": page, "pageSize": page_size}

    async def import_file(self, filename, payload):
        report = await asyncio.to_thread(parse_recipients, filename, payload)
        await validate_recipient_domains(report)
        row = self.add(Import, filename=Path(filename).name[:255], report=dump(report))
        await self.db.flush()
        return dict(id=row.id, **{k: v for k, v in report.items() if k != "records"})

    async def import_report(self, identifier):
        row = await self.owned(Import, identifier)
        return json.loads(row.report)

    async def account_json(self, row):
        reason = ""
        if not row.enabled:
            reason = "已停用"
        elif row.smtp_status != "ok" or row.imap_status != "ok":
            reason = "请完成收发连接测试"
        elif row.next_send_at and row.next_send_at > now():
            reason = "等待发送间隔"
        else:
            for window, limit in [
                (timedelta(hours=1), row.hourly_limit),
                (timedelta(hours=24), row.daily_limit),
            ]:
                count = await self.db.scalar(
                    select(func.count())
                    .select_from(Attempt)
                    .where(
                        *scope(Attempt, self.tenant, self.owner),
                        Attempt.account_id == row.id,
                        Attempt.created_at >= now() - window,
                    )
                )
                if count >= limit:
                    reason = "已达发送上限"
                    break
        config = json.loads(row.config)
        if "provider_type" not in config:
            from app.services.reach_email.providers import PROVIDERS

            config["provider_type"] = next((
                key for key, preset in PROVIDERS.items()
                if all(config.get(f"{protocol}_{field}") == preset[f"{protocol}{field.title()}"]
                       for protocol in ("smtp", "imap") for field in ("host", "port", "security"))
                and all(config.get(f"{protocol}_username") == row.email for protocol in ("smtp", "imap"))
            ), "custom")
        return dict(
            id=row.id,
            name=row.name,
            email=row.email,
            **{
                "".join([bits[0], *[p.title() for p in bits[1:]]]): v
                for k, v in config.items()
                for bits in [k.split("_")]
            },
            enabled=row.enabled,
            weight=row.weight,
            hourlyLimit=row.hourly_limit,
            dailyLimit=row.daily_limit,
            intervalSeconds=row.interval_seconds,
            smtpStatus=row.smtp_status,
            imapStatus=row.imap_status,
            lastSyncAt=row.last_sync_at,
            available=not reason,
            unavailableReason=reason,
            smtpSecretConfigured=bool(row.secret),
            imapSecretConfigured=bool(row.secret),
        )

    async def save_account(self, data, identifier=None):
        cipher = CredentialCipher(self.key)
        row = await self.owned(Account, identifier, lock=True) if identifier else None
        if row and row.email != data.email:
            used = await self.db.scalar(
                self.query(Message).where(Message.account_id == row.id).limit(1)
            )
            if used:
                raise ValueError("已用于收发的邮箱地址不能更改，请新增邮箱账号")
        duplicate = await self.db.scalar(self.query(Account).where(Account.email == data.email))
        if duplicate and (not row or duplicate.id != row.id):
            raise ValueError("此邮箱已添加")
        values = data.model_dump()
        secrets = cipher.decrypt(row.secret) if row else {}
        for key in ("smtp_password", "imap_password"):
            if values[key]:
                secrets[key] = values[key]
        if not secrets.get("smtp_password") or not secrets.get("imap_password"):
            raise ValueError("请填写 SMTP 和 IMAP 授权凭据")
        config = {
            k: v
            for k, v in values.items()
            if k.startswith(("smtp_", "imap_")) and not k.endswith("_password")
        }
        config.update(provider_type=data.provider_type, from_name=data.from_name)
        old_config = json.loads(row.config) if row else {}
        changed_protocols = {
            protocol: not row or row.email != data.email or bool(values[f"{protocol}_password"])
            or any(old_config.get(k) != v for k, v in config.items() if k.startswith(protocol + "_"))
            for protocol in ("smtp", "imap")
        }
        if row and (
            row.email != data.email
            or any(old_config.get(k) != config[k] for k in ("imap_host", "imap_port", "imap_username"))
        ):
            row.uidvalidity, row.last_uid, row.last_sync_at = None, 0, None
        if not row:
            row = self.add(
                Account,
                name=data.name.strip() or data.email[:100],
                email=data.email,
                config=dump(config),
                secret=cipher.encrypt(secrets),
            )
        row.name, row.email, row.config, row.secret = (
            data.name.strip() or data.email[:100],
            data.email,
            dump(config),
            cipher.encrypt(secrets),
        )
        if row.weight != values["weight"]:
            row.current_weight = 0
        for key in ("enabled", "weight", "hourly_limit", "daily_limit", "interval_seconds"):
            setattr(row, key, values[key])
        for protocol, changed in changed_protocols.items():
            if changed:
                setattr(row, f"{protocol}_status", "untested")
        row.version = (row.version or 0) + 1
        await self.db.flush()
        return await self.account_json(row)

    async def delete_account(self, identifier):
        row = await self.owned(Account, identifier, lock=True)
        if await self.db.scalar(self.query(Message).where(Message.account_id == row.id).limit(1)):
            raise ValueError("该邮箱已有邮件记录，请停用邮箱，保留会话回复关联")
        await self.db.delete(row)

    async def create_task(self, data):
        imp = await self.owned(Import, data.import_id, lock=True)
        report = json.loads(imp.report)
        if imp.consumed or not report["canCreate"]:
            raise ValueError("名单未通过校验或已被使用，请重新上传")
        task = self.add(
            Task,
            name=data.name,
            import_id=imp.id,
            settings=dump(data.settings.model_dump(by_alias=True)),
            content=dump(data.content.model_dump(by_alias=True)),
            daily_limit=data.settings.daily_limit,
            recipient_count=report["validCount"],
        )
        await self.attachments(data.content.attachment_ids)
        for record in report["records"]:
            self.add(
                Lead, task_id=task.id, email=record["values"]["邮箱"], values=dump(record["values"])
            )
        imp.consumed = True
        await self.db.flush()
        await self.snapshot_content(task)
        await self.db.flush()
        return self.task_json(task)

    def task_json(self, row):
        return {
            "id": row.id,
            "name": row.name,
            "importId": row.import_id,
            "settings": json.loads(row.settings),
            "content": json.loads(row.content),
            "status": row.status,
            "version": row.version,
            "scheduledAt": iso(row.scheduled_at),
            "endedReason": row.ended_reason,
            "recipientCount": row.recipient_count,
            "createdAt": iso(row.created_at),
            "updatedAt": iso(row.updated_at),
        }

    async def editable(self, identifier, version):
        task = await self.task(identifier, lock=True)
        if version != task.version:
            raise ValueError("版本冲突，请刷新后重试")
        if task.status != "unstarted":
            raise ValueError("任务已安排或开始执行，不能编辑")
        return task

    async def update_task(self, identifier, data):
        task = await self.editable(identifier, data.version)
        if task.import_id != data.import_id:
            imp = await self.owned(Import, data.import_id, lock=True)
            report = json.loads(imp.report)
            if imp.consumed or not report["canCreate"]:
                raise ValueError("名单未通过校验或已被使用")
            await self.db.execute(
                delete(Lead).where(*scope(Lead, self.tenant, self.owner), Lead.task_id == task.id)
            )
            for record in report["records"]:
                self.add(
                    Lead,
                    task_id=task.id,
                    email=record["values"]["邮箱"],
                    values=dump(record["values"]),
                )
            imp.consumed = True
            task.import_id = imp.id
            task.recipient_count = report["validCount"]
        await self.attachments(data.content.attachment_ids)
        task.name = data.name
        task.settings = dump(data.settings.model_dump(by_alias=True))
        task.daily_limit = data.settings.daily_limit
        task.content = dump(data.content.model_dump(by_alias=True))
        task.version += 1
        await self.snapshot_content(task)
        await self.db.flush()
        return self.task_json(task)

    async def save_content(self, identifier, data):
        task = await self.editable(identifier, data.version)
        await self.attachments(data.attachment_ids)
        task.content = dump(data.model_dump(by_alias=True, exclude={"version"}))
        task.version += 1
        await self.snapshot_content(task)
        await self.db.flush()
        return self.task_json(task)

    async def restore(self, identifier, data):
        task = await self.editable(identifier, data.version)
        version = await self.owned(Version, data.version_id)
        if version.task_id != task.id:
            raise ValueError("版本不属于此任务")
        task.content = version.content
        task.version += 1
        await self.snapshot_content(task)
        await self.db.flush()
        return self.task_json(task)

    async def delete_task(self, identifier):
        task = await self.task(identifier, lock=True)
        if task.status != "unstarted":
            raise ValueError("只能删除未启动任务")
        for model in (Lead, Version, AIJob):
            await self.db.execute(
                delete(model).where(
                    *scope(model, self.tenant, self.owner), model.task_id == task.id
                )
            )
        await self.db.delete(task)

    async def attachments(self, ids):
        if len(ids) > 5 or len(set(ids)) != len(ids):
            raise ValueError("最多 5 个不重复附件")
        result = [await self.owned(Attachment, identifier) for identifier in ids]
        if sum(a.size for a in result) > 8 * 1024 * 1024:
            raise ValueError("附件合计不能超过 8 MB")
        return result

    async def save_attachment(self, filename, payload, content_type):
        import hashlib
        import mimetypes

        name = Path(filename.replace("\\", "/")).name
        suffix = Path(name).suffix.lower()
        allowed = {
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
        }
        if (
            suffix not in allowed
            or not payload
            or len(payload) > 8 * 1024 * 1024
            or len(name) > 255
        ):
            raise ValueError("附件格式或大小不符合要求")
        # Check real formats; renamed executables are not accepted.
        if suffix == ".pdf" and not payload.startswith(b"%PDF-"):
            raise ValueError("PDF 文件格式不正确")
        if suffix in {".doc", ".xls", ".ppt"} and not payload.startswith(
            bytes.fromhex("d0cf11e0a1b11ae1")
        ):
            raise ValueError("Office 文件格式不正确")
        if suffix in {".docx", ".xlsx", ".pptx"}:
            from zipfile import BadZipFile, ZipFile

            try:
                with ZipFile(BytesIO(payload)) as archive:
                    if (
                        "[Content_Types].xml" not in archive.namelist()
                        or sum(i.file_size for i in archive.infolist()) > 40 * 1024 * 1024
                    ):
                        raise ValueError("Office 文件格式或解压大小不正确")
            except BadZipFile as exc:
                raise ValueError("Office 文件格式不正确") from exc
        if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
            from PIL import Image

            try:
                with Image.open(BytesIO(payload)) as image:
                    image.verify()
            except Exception as exc:
                raise ValueError("图片格式不正确") from exc
        if self.store is None:
            raise ValueError("邮件附件存储未配置")
        key = (
            f"email/{hashlib.sha256(self.tenant.encode()).hexdigest()[:16]}/{self.owner}/{new_id()}"
        )
        mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
        result = await self.store.put(
            key, BytesIO(payload), content_type=mime, expected_size=len(payload)
        )
        row = self.add(
            Attachment,
            name=name,
            size=len(payload),
            content_type=mime,
            object_key=key,
            sha256=result.sha256,
        )
        await self.db.flush()
        return {"id": row.id, "name": row.name, "size": row.size}

    async def preflight(self, identifier):
        task = await self.task(identifier)
        if task.status != "unstarted":
            raise ValueError("仅未启动任务可预检")
        TaskSettings.model_validate(json.loads(task.settings))
        content = Content.model_validate(json.loads(task.content))
        report = await self.import_report(task.import_id)
        validate_variables(
            "\n".join((content.subject, content.html, content.signature)), report["columns"]
        )
        if not content.subject.strip() or not has_email_content({"html": content.html}):
            raise ValueError("请先填写邮件主题和正文")
        warnings = []
        offset = 0
        while True:
            leads = (
                await self.db.scalars(
                    self
                    .query(Lead)
                    .where(Lead.task_id == task.id)
                    .order_by(Lead.id)
                    .offset(offset)
                    .limit(200)
                )
            ).all()
            if not leads:
                break
            for lead in leads:
                values = json.loads(lead.values)
                subject, empty1 = render_content(content.subject, values, html=False)
                _, empty2 = render_content(content.html + "\n" + content.signature, values)
                if "\r" in subject or "\n" in subject or len(subject) > 512:
                    raise ValueError("变量替换后的主题含换行或超过 512 字符")
                if empty1 or empty2:
                    warnings.append({
                        "email": lead.email,
                        "variables": sorted(set(empty1 + empty2)),
                    })
            offset += len(leads)
        return {
            "version": task.version,
            "warnings": warnings,
            "recipientCount": task.recipient_count,
        }

    async def task_result(self, row):
        counts = (
            await self.db.execute(
                select(Message.status, func.count())
                .where(
                    *scope(Message, self.tenant, self.owner),
                    Message.task_id == row.id,
                    Message.direction == "outbound",
                )
                .group_by(Message.status)
            )
        ).all()
        totals = dict(counts)
        content_updated_at = await self.db.scalar(self.query(Version).where(Version.task_id == row.id).with_only_columns(func.max(Version.created_at)))
        return dict(
            **self.task_json(row),
            contentUpdatedAt=iso(content_updated_at or row.created_at),
            acceptedCount=totals.get("accepted", 0),
            failedCount=totals.get("failed", 0),
            unknownCount=totals.get("unknown", 0),
        )

    async def start(self, identifier, data):
        task = await self.task(identifier, lock=True)
        if task.start_key == data.request_id and task.status != "unstarted":
            return self.task_json(task)
        task = await self.editable(identifier, data.version)
        TaskSettings.model_validate(json.loads(task.settings))
        CredentialCipher(self.key)
        content = Content.model_validate(json.loads(task.content))
        if not content.subject.strip() or not has_email_content({"html": content.html}):
            raise ValueError("请先填写邮件主题和正文")
        report = await self.import_report(task.import_id)
        combined = "\n".join((content.subject, content.html, content.signature))
        validate_variables(combined, report["columns"])
        files = await self.attachments(content.attachment_ids)
        for file in files:
            if not self.store or (await self.store.stat(file.object_key)).byte_size != file.size:
                raise ValueError("附件不可读取，请重新上传")
        accounts = (await self.db.scalars(self.query(Account))).all()
        if not any([(await self.account_json(a))["available"] for a in accounts]):
            raise ValueError("没有可用邮箱，请配置并测试连接或等待额度恢复")
        due = now()
        if data.scheduled_at:
            if data.scheduled_at.tzinfo is None:
                raise ValueError("定时时间必须包含时区")
            due = data.scheduled_at.astimezone(timezone.utc).replace(tzinfo=None)
            if due <= now():
                raise ValueError("定时时间必须晚于当前时间")
        warnings = []
        # Render in bounded pages. Lock on task serializes start/withdraw/edit.
        offset = 0
        while True:
            leads = (
                await self.db.scalars(
                    self
                    .query(Lead)
                    .where(Lead.task_id == task.id)
                    .order_by(Lead.id)
                    .offset(offset)
                    .limit(200)
                )
            ).all()
            if not leads:
                break
            for lead in leads:
                values = json.loads(lead.values)
                subject, empty1 = render_content(content.subject, values, html=False)
                html, empty2 = render_content(content.html + "\n" + content.signature, values)
                if "\n" in subject or "\r" in subject or len(subject) > 512:
                    raise ValueError("变量替换后的主题含换行或超过 512 字符")
                if empty1 or empty2:
                    warnings.append({
                        "email": lead.email,
                        "variables": sorted(set(empty1 + empty2)),
                    })
                message = self.add(
                    Message,
                    task_id=task.id,
                    lead_id=lead.id,
                    to_email=lead.email,
                    subject=subject,
                    html=clean_html(html),
                    attachment_ids=dump(content.attachment_ids),
                    message_id=f"<{new_id()}@reach.local>",
                    dedup_key=f"initial:{task.id}:{task.version}:{lead.id}",
                    due_at=due,
                )
                lead.first_message_id = message.id
            offset += len(leads)
        task.status = "scheduled" if data.scheduled_at else "running"
        task.scheduled_at = due if data.scheduled_at else None
        task.start_key = data.request_id
        task.version += 1
        await self.db.flush()
        return dict(**self.task_json(task), warnings=warnings)

    async def transition(self, identifier, version, action):
        task = await self.task(identifier, lock=True)
        if task.version != version:
            raise ValueError("版本冲突，请刷新后重试")
        if action == "withdraw":
            if task.status != "scheduled":
                raise ValueError("任务已开始，不能撤回")
            await self.db.execute(
                delete(Message).where(
                    *scope(Message, self.tenant, self.owner),
                    Message.task_id == task.id,
                    Message.status == "queued",
                )
            )
            await self.db.execute(
                update(Lead)
                .where(*scope(Lead, self.tenant, self.owner), Lead.task_id == task.id)
                .values(first_message_id=None)
            )
            task.status = "unstarted"
            task.start_key = None
            task.scheduled_at = None
        else:
            if task.status not in {"running", "scheduled"}:
                raise ValueError("任务当前不能停止")
            task.status = "ended"
            task.ended_reason = "stopped"
            await self.db.execute(
                update(Message)
                .where(
                    *scope(Message, self.tenant, self.owner),
                    Message.task_id == task.id,
                    Message.status.in_(["queued", "retryable"]),
                )
                .values(status="cancelled")
            )
            await self.db.execute(
                update(Lead)
                .where(*scope(Lead, self.tenant, self.owner), Lead.task_id == task.id)
                .values(next_follow_up_at=None, stopped_reason="task_stopped")
            )
        task.version += 1
        await self.db.flush()
        return self.task_json(task)

    async def ai_job(self, identifier, data):
        task = await self.editable(identifier, data.version)
        settings = json.loads(task.settings)
        report = await self.import_report(task.import_id)
        payload = {
            "action": data.action,
            "subject": data.subject,
            "content": data.content,
            "instruction": data.instruction,
            "context": settings,
            "allowed_variables": report["columns"],
        }
        return await self.enqueue_ai(task, payload)

    async def reply_ai_job(self, identifier, data):
        lead = await self.owned(Lead, identifier)
        task = await self.task(lead.task_id)
        target = None
        if data.inbound_id:
            target = await self.owned(Message, data.inbound_id)
            if target.lead_id != lead.id or target.task_id != task.id or target.direction != "inbound":
                raise ValueError("请选择当前往来的来信")
        query = self.query(Message).where(Message.lead_id == lead.id, Message.task_id == task.id)
        if target:
            query = query.where(Message.created_at <= target.created_at)
        recent = (await self.db.scalars(query.order_by(Message.created_at.desc()).limit(10))).all()
        payload = {
            "action": data.action,
            "subject": data.subject,
            "content": data.content,
            "instruction": data.instruction,
            "context": {
                "mode": "reply",
                "conversationId": lead.id,
                "company": json.loads(task.settings),
                "customer": json.loads(lead.values),
                "replyTo": {"subject": target.subject, "content": target.html[:20000]} if target else None,
                "messages": [{"direction": m.direction, "subject": m.subject, "content": m.html[:10000]} for m in reversed(recent)],
            },
            "allowed_variables": [],
        }
        return await self.enqueue_ai(task, payload)

    async def enqueue_ai(self, task, payload):
        source_hash = hashlib.sha256(
            dump(dict(promptVersion=PROMPT_VERSION, version=task.version, **payload)).encode()
        ).hexdigest()
        job = await self.db.scalar(
            self.query(AIJob).where(AIJob.task_id == task.id, AIJob.source_hash == source_hash)
        )
        if job and job.status not in ("failed", "stale"):
            return self.job_json(job)
        if job:
            job.status = "queued"
            job.error_code = None
            job.result = None
        else:
            job = self.add(
                AIJob,
                task_id=task.id,
                action=payload["action"],
                source_hash=source_hash,
                source_version=task.version,
                payload=dump(payload),
            )
        await self.db.flush()
        return self.job_json(job)

    def job_json(self, row):
        return {
            "id": row.id,
            "status": row.status,
            "result": json.loads(row.result) if row.result else None,
            "errorCode": row.error_code,
            "sourceVersion": row.source_version,
        }

    def message_json(self, row):
        return {
            "id": row.id,
            "taskId": row.task_id,
            "conversationId": row.lead_id,
            "inReplyTo": row.in_reply_to,
            "direction": row.direction,
            "kind": row.kind,
            "status": row.status,
            "subject": row.subject,
            "html": clean_html(row.html),
            "fromEmail": row.from_email,
            "toEmail": row.to_email,
            "attachmentIds": json.loads(row.attachment_ids),
            "errorCode": row.error_code,
            "resolutionNote": row.resolution_note,
            "resolvedAt": iso(row.resolved_at),
            "createdAt": iso(row.created_at),
            "sentAt": iso(row.sent_at),
        }

    async def lead_json(self, row):
        task = await self.task(row.task_id)
        values = json.loads(row.values)
        last = await self.db.scalar(
            self
            .query(Message)
            .where(Message.lead_id == row.id)
            .order_by(Message.created_at.desc())
            .limit(1)
        )
        return {
            "id": row.id,
            "taskId": row.task_id,
            "taskName": task.name,
            "customerName": values.get("客户姓名", ""),
            "email": row.email,
            "companyName": values.get("企业名称", ""),
            "position": values.get("职务", ""),
            "classification": row.classification,
            "latestStatus": last.status if last else "unstarted",
            "lastSentAt": iso(row.last_sent_at),
            "lastReplyAt": iso(row.last_reply_at),
            "replyStatus": (
                "none" if not row.last_reply_at else
                "read" if row.last_read_reply_at and row.last_read_reply_at >= row.last_reply_at else
                "unread"
            ),
            "conversationId": row.id,
        }

    async def do_not_contact(self, identifier, reason):
        lead = await self.owned(Lead, identifier)
        existing = await self.db.scalar(
            self.query(Suppression).where(Suppression.email == lead.email)
        )
        if not existing:
            self.add(Suppression, email=lead.email, reason=reason)
        await self.db.execute(
            update(Message)
            .where(
                *scope(Message, self.tenant, self.owner),
                Message.to_email == lead.email,
                Message.direction == "outbound",
                Message.status.in_(["queued", "retryable"]),
            )
            .values(status="cancelled", error_code="DO_NOT_CONTACT")
        )
        await self.db.execute(
            update(Lead)
            .where(*scope(Lead, self.tenant, self.owner), Lead.email == lead.email)
            .values(next_follow_up_at=None, stopped_reason="do_not_contact")
        )
        await self.db.flush()
        return {"email": lead.email, "stopped": True}

    async def resolve_message(self, identifier, data):
        message = await self.owned(Message, identifier)
        task = await self.task(message.task_id, lock=True)
        message = await self.db.scalar(
            self
            .query(Message)
            .where(Message.id == identifier)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if message.status != "unknown":
            raise ValueError("仅发送结果待核对的邮件可确认")
        message.status = data.outcome
        message.error_code = "MANUALLY_CONFIRMED_" + data.outcome.upper()
        message.resolution_note = data.note.strip()
        message.resolved_at = now()
        await self.db.execute(
            update(Attempt)
            .where(
                *scope(Attempt, self.tenant, self.owner),
                Attempt.message_id == message.id,
                Attempt.status == "unknown",
            )
            .values(status=data.outcome)
        )
        if data.outcome == "accepted":
            message.sent_at = now()
            lead = await self.owned(Lead, message.lead_id, lock=True)
            lead.last_sent_at = message.sent_at
            if message.kind == "follow_up":
                lead.follow_up_count += 1
            settings = json.loads(task.settings)
            if (
                not message.dedup_key.startswith("manual:")
                and task.status == "running"
                and not lead.stopped_reason
                and settings.get("followUpEnabled")
                and lead.follow_up_count < settings.get("followUpCount", 2)
            ):
                lead.next_follow_up_at = now() + timedelta(
                    days=settings.get("followUpIntervalDays", 2)
                )
        # Confirmation never resubmits mail or releases an uncertain attempt's quota.
        await self.db.flush()
        return self.message_json(message)

    async def reply(self, identifier, data, kind):
        lead = await self.owned(Lead, identifier, lock=True)
        existing = await self.db.scalar(
            self.query(Message).where(Message.dedup_key == f"manual:{lead.id}:{data.request_id}")
        )
        if existing:
            return self.message_json(existing)
        if not lead.account_id or not lead.first_message_id:
            raise ValueError("首封尚未发送，不能回复或主动跟进")
        account = await self.owned(Account, lead.account_id)
        if not account.enabled:
            raise ValueError("原发件邮箱已停用")
        if await self.db.scalar(self.query(Suppression).where(Suppression.email == lead.email)):
            raise ValueError("该邮箱已禁止联系")
        parent = await self.owned(
            Message, data.inbound_id if kind == "reply" else lead.first_message_id
        )
        if parent.lead_id != lead.id or (kind == "reply" and parent.direction != "inbound"):
            raise ValueError("回复邮件不属于此会话")
        await self.attachments(data.attachment_ids)
        if "{{" in data.subject + data.html:
            raise ValueError("人工回复请填写实际内容，不支持未替换变量")
        row = self.add(
            Message,
            task_id=lead.task_id,
            lead_id=lead.id,
            account_id=account.id,
            kind=kind,
            from_email=account.email,
            to_email=lead.email,
            subject=data.subject,
            html=data.html,
            attachment_ids=dump(data.attachment_ids),
            message_id=f"<{new_id()}@reach.local>",
            in_reply_to=parent.message_id,
            references=parent.references + " " + parent.message_id,
            dedup_key=f"manual:{lead.id}:{data.request_id}",
        )
        await self.db.flush()
        return self.message_json(row)
