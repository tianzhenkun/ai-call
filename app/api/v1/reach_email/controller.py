from __future__ import annotations

import asyncio
import json
from datetime import timezone
from typing import Annotated

from fastapi import APIRouter, Depends, File, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response, StreamingResponse
from fastapi.routing import APIRoute
from pydantic import ValidationError
from pydantic.alias_generators import to_camel
from sqlalchemy import func, or_, select, update

from app.api.v1.system.auth.schema import AuthSchema
from app.common.response import ErrorResponse, SuccessResponse
from app.config.setting import settings
from app.core.database import async_db_session
from app.core.dependencies import get_current_user
from app.core.exceptions import CustomException
from app.services.reach_email.content import COLUMNS, MAX_BYTES, has_email_content, xlsx_bytes
from app.services.reach_email.limits import MAX_ATTACHMENT_BYTES
from app.services.reach_email.models import (
    Account,
    AIJob,
    Attachment,
    Lead,
    Message,
    Suppression,
    Task,
    Version,
)
from app.services.reach_email.schema import (
    AccountInput,
    AIInput,
    ClassificationInput,
    ContentInput,
    ReadInput,
    ReplyAIInput,
    ReplyInput,
    ResolveInput,
    RestoreInput,
    SettingsInput,
    StartInput,
    SuppressInput,
    TaskInput,
    VersionInput,
)
from app.services.reach_email.security import CredentialCipher, configured_secret
from app.services.reach_email.service import EmailService, scope


def email_validation_message(errors):
    labels = {
        'settings': '邮件设置', 'name': '名称', 'importId': '收件名单',
        'companyName': '公司名称', 'companyWebsite': '公司官网', 'companyDescription': '公司简介',
        'dailyLimit': '24 小时发送上限', 'followUpEnabled': '自动跟进开关',
        'followUpIntervalDays': '跟进间隔', 'followUpCount': '追加跟进次数',
        'subject': '邮件主题', 'html': '邮件正文', 'content': '邮件内容',
        'signature': '邮件签名', 'signatureName': '签名名称', 'attachmentIds': '附件',
        'scheduledAt': '定时执行时间', 'version': '版本号', 'requestId': '请求标识',
        'email': '邮箱', 'smtpPassword': 'SMTP 密码', 'imapPassword': 'IMAP 密码',
        'smtpPort': 'SMTP 端口', 'imapPort': 'IMAP 端口',
        'smtpHost': 'SMTP 服务器', 'imapHost': 'IMAP 服务器',
        'smtpUsername': 'SMTP 用户名', 'imapUsername': 'IMAP 用户名',
        'smtpSecurity': 'SMTP 加密方式', 'imapSecurity': 'IMAP 加密方式',
        'providerType': '邮箱类型', 'fromName': '发件人名称', 'enabled': '启用状态',
        'weight': '邮箱权重', 'hourlyLimit': '每小时发送上限', 'intervalSeconds': '发送间隔',
        'action': '操作类型', 'instruction': 'AI 指令', 'inboundId': '回复目标',
        'versionId': '历史版本', 'lastReplyAt': '最近回复时间', 'classification': '线索分类',
        'outcome': '处理结果', 'note': '处理说明', 'reason': '原因',
    }
    safe_messages = {
        '请完善邮件设置：请输入公司名称', '请完善邮件设置：请输入公司官网',
        '请完善邮件设置：请输入公司简介',
        '请完善邮件设置：公司官网须填写完整的 http 或 https 地址',
        '公司官网须填写包含完整域名的地址，例如 https://example.com',
        '主题不能包含换行', '邮箱格式不正确', '发件人名称不能包含换行',
        '邮箱类型不受支持，请选择已有类型或自定义邮箱',
    }
    messages = []
    for error in errors:
        # 只返回固定校验文案和约束值，不回显 input、未知字段名或异常上下文。
        reason = error['msg'].removeprefix('Value error, ')
        if reason not in safe_messages:
            loc = error['loc']
            field = to_camel(str(loc[-1])) if loc else 'settings'
            label = labels.get(field, '参数')
            if field == 'dailyLimit' and ('settings' in loc or not any(part in loc for part in ('body', 'query'))):
                label = '本任务 24 小时发送上限'
            kind, context = error['type'], error.get('ctx', {})
            if kind == 'missing':
                reason = f'请填写{label}'
            elif kind in ('greater_than_equal', 'less_than_equal'):
                limit = context.get('ge' if kind == 'greater_than_equal' else 'le')
                reason = f'{label}不能{"小于" if kind == "greater_than_equal" else "大于"} {limit}'
            elif kind in ('string_too_long', 'too_long'):
                if field == 'attachmentIds' and kind == 'too_long':
                    reason = f'最多 {context["max_length"]} 个附件，当前选择了 {context["actual_length"]} 个'
                else:
                    reason = f'{label}不能超过 {context["max_length"]} {"字符" if kind == "string_too_long" else "项"}'
            elif kind in ('string_too_short', 'too_short'):
                reason = f'{label}至少需要 {context["min_length"]} {"字符" if kind == "string_too_short" else "项"}'
            elif kind in ('int_type', 'int_parsing', 'int_from_float'):
                reason = f'{label}须填写整数'
            else:
                reason = f'{label}格式不正确'
        if reason not in messages:
            messages.append(reason)
    return '参数校验失败：' + '；'.join(messages)


class EmailRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def safe_handler(request):
            try:
                return await handler(request)
            except RequestValidationError as exc:
                return ErrorResponse(msg=email_validation_message(exc.errors()), status_code=422)

        return safe_handler


EmailRouter = APIRouter(prefix="/email", tags=["REACH 邮件"], route_class=EmailRoute)
LEAD_TASK_STATUSES = ("running", "ended")


def encryption_key():
    return configured_secret(
        settings.REACH_EMAIL_ENCRYPTION_KEY,
        settings.REACH_EMAIL_ENCRYPTION_KEY_FILE,
        "REACH_EMAIL_ENCRYPTION_KEY",
    )


async def service(request: Request, auth: AuthSchema = Depends(get_current_user)):
    # Always require the actual verified platform identity, even in legacy dev mode.
    tenant = request.scope.get("platform_tenant_id")
    owner = request.scope.get("user_id")
    if not tenant or not owner or request.scope.get("product_code") != "reach":
        raise CustomException(msg="邮件功能需要有效的 REACH 用户和租户身份", status_code=401)
    async with async_db_session() as db:
        try:
            yield EmailService(db, str(tenant), str(owner), encryption_key())
            await db.commit()
        except ValidationError as exc:
            await db.rollback()
            raise CustomException(msg=email_validation_message(exc.errors()), status_code=422) from None
        except ValueError as exc:
            await db.rollback()
            message = str(exc)
            status = 409 if "版本" in message else 404 if "不存在或无权" in message else 422
            raise CustomException(msg=message, status_code=status) from exc
        except CustomException:
            await db.rollback()
            raise
        except Exception:
            await db.rollback()
            # Never expose SQL bind values, authorization material or message bodies.
            raise CustomException(
                msg="邮件操作失败，请检查服务和存储配置", status_code=503
            ) from None


@EmailRouter.get('/worker-health')
async def email_worker_health(s: EmailService = Depends(service)):
    from app.services.reach_email.health import worker_health
    data = await worker_health(s.db, s.query(Message))
    data['activeTaskCount'] = await s.db.scalar(
        select(func.count()).select_from(
            s.query(Task).where(Task.status.in_(('scheduled', 'running'))).subquery()
        )
    )
    return SuccessResponse(data=data)


def attach_store(s):
    from app.services.ai_call.knowledge import build_knowledge_store

    try:
        s.store = build_knowledge_store(settings)
    except (RuntimeError, ValueError):
        raise ValueError("邮件附件存储尚未配置") from None


def spreadsheet(payload, filename):
    from urllib.parse import quote

    return Response(
        payload,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}"},
    )


@EmailRouter.get("/imports/template")
async def template(s: Annotated[EmailService, Depends(service)]):
    return spreadsheet(xlsx_bytes([COLUMNS]), "邮件名单模板.xlsx")


@EmailRouter.post("/imports")
async def import_file(
    file: Annotated[UploadFile, File()], s: Annotated[EmailService, Depends(service)]
):
    payload = await file.read(MAX_BYTES + 1)
    return SuccessResponse(data=await s.import_file(file.filename or "", payload))


@EmailRouter.get("/imports/{identifier}/report")
async def import_report(identifier: str, s: Annotated[EmailService, Depends(service)]):
    report = await s.import_report(identifier)
    rows = [["Excel 行号", "邮箱", "问题", "保留行号"]] + [
        [i["row"], i["email"], i["reason"], i.get("keptRow", "")] for i in report["issues"]
    ]
    return spreadsheet(xlsx_bytes(rows), "名单校验报告.xlsx")


@EmailRouter.get("/accounts/provider-presets")
async def account_presets(s: Annotated[EmailService, Depends(service)]):
    from app.services.reach_email.providers import PROVIDERS

    return SuccessResponse(data=PROVIDERS)


@EmailRouter.get("/accounts")
async def accounts(
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 100,
):
    rows, meta = await s.page(Account, page, pageSize)
    items = [await s.account_json(a) for a in rows]
    available = 0
    offset = 0
    while True:
        batch = (
            await s.db.scalars(s.query(Account).order_by(Account.id).offset(offset).limit(100))
        ).all()
        if not batch:
            break
        for account in batch:
            available += int((await s.account_json(account))["available"])
        offset += len(batch)
    return SuccessResponse(data=dict(items=items, availableCount=available, **meta))


@EmailRouter.post("/accounts")
async def add_account(data: AccountInput, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.save_account(data))


@EmailRouter.put("/accounts/{identifier}")
async def edit_account(
    identifier: str, data: AccountInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.save_account(data, identifier))


async def probe_account(identifier, s, protocols):
    from app.services.reach_email.transport import test_account as probe

    account = await s.owned(Account, identifier)
    version = account.version
    config = dict(
        json.loads(account.config),
        **CredentialCipher(s.key).decrypt(account.secret),
        email=account.email,
    )
    await s.db.commit()  # Never hold a database transaction during network IO.
    result = await asyncio.to_thread(probe, config, protocols)
    updated = await s.db.execute(
        update(Account)
        .where(
            *scope(Account, s.tenant, s.owner), Account.id == identifier, Account.version == version
        )
        .values(**{f"{protocol}_status": item["status"] for protocol, item in result.items()})
    )
    if not updated.rowcount:
        raise ValueError("邮箱配置已变更，请重新测试")
    await s.db.refresh(account)
    return SuccessResponse(data=dict(**(await s.account_json(account)), testResult=result))


@EmailRouter.post("/accounts/{identifier}/test")
async def test_account(identifier: str, s: Annotated[EmailService, Depends(service)]):
    return await probe_account(identifier, s, ("smtp", "imap"))


@EmailRouter.post("/accounts/{identifier}/test-smtp")
async def test_smtp(identifier: str, s: Annotated[EmailService, Depends(service)]):
    return await probe_account(identifier, s, ("smtp",))


@EmailRouter.post("/accounts/{identifier}/test-imap")
async def test_imap(identifier: str, s: Annotated[EmailService, Depends(service)]):
    return await probe_account(identifier, s, ("imap",))


@EmailRouter.delete("/accounts/{identifier}")
async def delete_account(identifier: str, s: Annotated[EmailService, Depends(service)]):
    await s.delete_account(identifier)
    return SuccessResponse()


@EmailRouter.post("/accounts/{identifier}/sync-inbox")
async def sync_inbox(identifier: str, s: Annotated[EmailService, Depends(service)]):
    from app.services.reach_email.worker import EmailWorker

    account = await s.owned(Account, identifier)
    if not account.enabled:
        raise ValueError("该邮箱未启用")
    attach_store(s)
    worker = EmailWorker(async_db_session, CredentialCipher(s.key), s.store, None, batch_size=50)
    await s.db.commit()
    try:
        result = await worker.sync_account(identifier, require_worker_lease=False)
    except ValueError:
        raise
    except Exception:
        raise CustomException(msg="收件同步失败，请检查 IMAP 连接与附件存储配置", status_code=503) from None
    return SuccessResponse(data=result)


@EmailRouter.get("/tasks")
async def tasks(
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 20,
    name: str = "",
    status: str = "",
    forLeads: bool = False,
):
    filters = []
    if forLeads:
        filters.append(Task.status.in_(LEAD_TASK_STATUSES))
    if name:
        filters.append(Task.name.contains(name, autoescape=True))
    if status:
        filters.append(Task.status == status)
    rows, meta = await s.page(Task, page, pageSize, filters)
    return SuccessResponse(data=dict(items=[await s.task_result(t) for t in rows], **meta))


@EmailRouter.post("/tasks")
async def create_task(data: TaskInput, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.create_task(data))


@EmailRouter.get("/tasks/{identifier}")
async def task(identifier: str, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.task_result(await s.task(identifier)))


@EmailRouter.put("/tasks/{identifier}")
async def edit_task(identifier: str, data: TaskInput, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.update_task(identifier, data))


@EmailRouter.delete("/tasks/{identifier}")
async def delete_task(identifier: str, s: Annotated[EmailService, Depends(service)]):
    await s.delete_task(identifier)
    return SuccessResponse()


@EmailRouter.put("/tasks/{identifier}/settings")
async def task_settings(
    identifier: str, data: SettingsInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.update_settings(identifier, data))


@EmailRouter.put("/tasks/{identifier}/content")
async def content(
    identifier: str, data: ContentInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.save_content(identifier, data))


@EmailRouter.get("/tasks/{identifier}/files")
async def task_files(identifier: str, s: Annotated[EmailService, Depends(service)]):
    from app.services.reach_email.models import Import

    task = await s.task(identifier)
    source = await s.owned(Import, task.import_id)
    attachments = await s.attachments(json.loads(task.content).get("attachmentIds", []))
    return SuccessResponse(data={
        "importName": source.filename,
        "attachments": [{"id": row.id, "name": row.name, "size": row.size} for row in attachments],
    })


@EmailRouter.get("/tasks/{identifier}/recipients/download")
async def task_recipients_download(identifier: str, s: Annotated[EmailService, Depends(service)]):
    task = await s.task(identifier)
    report = await s.import_report(task.import_id)
    columns = report['columns']
    # 原文件未保留，导出任务实际使用的去重名单，沿用安全的文本单元格写入。
    rows = [columns] + [[record['values'].get(column, '') for column in columns]
                        for record in report['records']]
    return spreadsheet(await asyncio.to_thread(xlsx_bytes, rows), '任务收件名单.xlsx')


@EmailRouter.get("/tasks/{identifier}/versions")
async def versions(
    identifier: str,
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 20,
):
    await s.task(identifier)
    rows = (await s.db.scalars(
        s.query(Version).where(Version.task_id == identifier)
        .order_by(Version.created_at.desc(), Version.id)
    )).all()
    # 兼容已有空快照；过滤后再分页，避免空快照挤占有效版本。
    rows = [row for row in rows if has_email_content(json.loads(row.content))]
    meta = {"total": len(rows), "page": page, "pageSize": pageSize}
    rows = rows[(page - 1) * pageSize:page * pageSize]
    from app.services.reach_email.service import iso

    return SuccessResponse(
        data=dict(
            items=[
                {
                    "id": v.id,
                    "version": v.version,
                    "content": json.loads(v.content),
                    "createdAt": iso(v.created_at),
                }
                for v in rows
            ],
            **meta,
        )
    )


@EmailRouter.post("/tasks/{identifier}/restore")
async def restore(
    identifier: str, data: RestoreInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.restore(identifier, data))


@EmailRouter.get("/tasks/{identifier}/preflight")
async def preflight(identifier: str, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.preflight(identifier))


@EmailRouter.post("/tasks/{identifier}/start")
async def start(identifier: str, data: StartInput, s: Annotated[EmailService, Depends(service)]):
    task = await s.task(identifier)
    if json.loads(task.content).get("attachmentIds"):
        attach_store(s)
    return SuccessResponse(data=await s.start(identifier, data))


@EmailRouter.post("/tasks/{identifier}/withdraw")
async def withdraw(
    identifier: str, data: VersionInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.transition(identifier, data.version, "withdraw"))


@EmailRouter.post("/tasks/{identifier}/stop")
async def stop(identifier: str, data: VersionInput, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.transition(identifier, data.version, "stop"))


@EmailRouter.post("/tasks/{identifier}/ai-jobs")
async def ai_job(identifier: str, data: AIInput, s: Annotated[EmailService, Depends(service)]):
    key = configured_secret(
        settings.REACH_EMAIL_LLM_API_KEY,
        settings.REACH_EMAIL_LLM_API_KEY_FILE,
        "REACH_EMAIL_LLM_API_KEY",
    )
    if not key:
        raise ValueError("邮件 AI 尚未配置")
    return SuccessResponse(data=await s.ai_job(identifier, data), status_code=202)


@EmailRouter.get("/ai-jobs/{identifier}")
async def job(identifier: str, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=s.job_json(await s.owned(AIJob, identifier)))


@EmailRouter.post("/conversations/{identifier}/ai-jobs")
async def reply_ai_job(identifier: str, data: ReplyAIInput, s: Annotated[EmailService, Depends(service)]):
    key = configured_secret(settings.REACH_EMAIL_LLM_API_KEY, settings.REACH_EMAIL_LLM_API_KEY_FILE, "REACH_EMAIL_LLM_API_KEY")
    if not key:
        raise ValueError("邮件 AI 尚未配置")
    return SuccessResponse(data=await s.reply_ai_job(identifier, data), status_code=202)


@EmailRouter.post("/attachments")
async def attachment(
    file: Annotated[UploadFile, File()], s: Annotated[EmailService, Depends(service)]
):
    attach_store(s)
    payload = await file.read(MAX_ATTACHMENT_BYTES + 1)
    return SuccessResponse(
        data=await s.save_attachment(file.filename or "", payload, file.content_type)
    )


@EmailRouter.get("/attachments/{identifier}")
async def download(identifier: str, s: Annotated[EmailService, Depends(service)]):
    from urllib.parse import quote

    row = await s.owned(Attachment, identifier)
    attach_store(s)
    opened = await s.store.open(row.object_key)
    return StreamingResponse(
        opened.body,
        media_type=row.content_type,
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(row.name)}",
            "X-Content-Type-Options": "nosniff",
        },
    )


@EmailRouter.get("/leads")
async def leads(
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 20,
    taskId: str = "",
    classification: str = "",
    email: str = "",
):
    filters = [
        Lead.task_id.in_(
            s.query(Task).with_only_columns(Task.id).where(Task.status.in_(LEAD_TASK_STATUSES))
        )
    ]
    if taskId:
        filters.append(Lead.task_id == taskId)
    if classification:
        filters.append(Lead.classification == classification)
    if email:
        filters.append(Lead.email.contains(email, autoescape=True))
    rows, meta = await s.page(Lead, page, pageSize, filters)
    return SuccessResponse(data=dict(items=[await s.lead_json(row) for row in rows], **meta))


@EmailRouter.patch("/leads/{identifier}/classification")
async def classify(
    identifier: str, data: ClassificationInput, s: Annotated[EmailService, Depends(service)]
):
    lead = await s.owned(Lead, identifier, lock=True)
    lead.classification = data.classification
    return SuccessResponse(data=await s.lead_json(lead))


@EmailRouter.get("/messages/summary")
async def summary(
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 20,
    taskId: str = "",
    keyword: str = "",
):
    from app.services.reach_email.reporting import summarize

    attempted = select(Message.task_id).where(
        *scope(Message, s.tenant, s.owner), Message.direction == "outbound",
        Message.attempt_count > 0,
    )
    query = s.query(Task).where(Task.id.in_(attempted))
    if taskId:
        query = query.where(Task.id == taskId)
    if keyword:
        matching_sender = attempted.where(Message.from_email.contains(keyword, autoescape=True))
        query = query.where(or_(Task.name.contains(keyword, autoescape=True), Task.id.in_(matching_sender)))
    total = await s.db.scalar(select(func.count()).select_from(query.subquery()))
    tasks = (await s.db.scalars(query.order_by(Task.created_at.desc(), Task.id)
        .offset((page - 1) * pageSize).limit(pageSize))).all()
    rows = (await s.db.scalars(s.query(Message).where(Message.task_id.in_([task.id for task in tasks])))).all()
    items = [dict(taskId=task.id, taskName=task.name,
        **summarize([row for row in rows if row.task_id == task.id])) for task in tasks]
    return SuccessResponse(data={"items": items, "total": total, "page": page, "pageSize": pageSize})


@EmailRouter.get("/messages/recipients")
async def recipient_summary(
    s: Annotated[EmailService, Depends(service)], taskId: str, senderEmail: str = "",
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 10,
):
    from app.services.reach_email.reporting import summarize

    await s.task(taskId)
    addresses = s.query(Message).where(Message.task_id == taskId, Message.direction == "outbound", Message.attempt_count > 0).with_only_columns(Message.from_email, Message.to_email).distinct()
    if senderEmail:
        addresses = addresses.where(Message.from_email == senderEmail)
    total = await s.db.scalar(select(func.count()).select_from(addresses.subquery()))
    pairs = (await s.db.execute(addresses.order_by(Message.from_email, Message.to_email).offset((page - 1) * pageSize).limit(pageSize))).all()
    emails = [recipient for _, recipient in pairs]
    # 收件人分页后仅加载该批会话及回执，复用与总表一致的证据统计。
    lead_ids = s.query(Lead).where(Lead.task_id == taskId, Lead.email.in_(emails)).with_only_columns(Lead.id)
    rows = (await s.db.scalars(s.query(Message).where(Message.task_id == taskId,
        or_(Message.lead_id.in_(lead_ids), Message.to_email.in_(emails), Message.delivery_report.is_not(None))))).all()
    pair_set = set(pairs)
    first_sent = {}
    for message in rows:
        if (message.direction == "outbound" and message.kind == "initial"
                and message.sent_at is not None and message.resolved_at is None):
            pair = (message.from_email, message.to_email)
            first_sent[pair] = min(first_sent.get(pair, message.sent_at), message.sent_at)
    details = [dict(senderEmail=sender, **row) for sender in sorted({sender for sender, _ in pairs})
        for row in summarize(rows, sender_email=sender) if (sender, row['recipientEmail']) in pair_set]
    for detail in details:
        sent_at = first_sent.get((detail['senderEmail'], detail['recipientEmail']))
        detail['firstSentAt'] = sent_at.replace(tzinfo=timezone.utc).isoformat() if sent_at else None
    return SuccessResponse(data=dict(items=details, total=total, page=page, pageSize=pageSize))


@EmailRouter.get("/messages")
async def messages(
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 20,
    taskId: str = "",
):
    rows, meta = await s.page(
        Message, page, pageSize, [Message.task_id == taskId] if taskId else []
    )
    return SuccessResponse(data=dict(items=[s.message_json(row) for row in rows], **meta))


@EmailRouter.get("/conversations/{identifier}")
async def conversation(
    identifier: str,
    s: Annotated[EmailService, Depends(service)],
    page: Annotated[int, Query(ge=1)] = 1,
    pageSize: Annotated[int, Query(ge=1, le=100)] = 50,
):
    lead = await s.owned(Lead, identifier)
    rows, meta = await s.page(Message, page, pageSize, [Message.lead_id == lead.id])
    from app.services.reach_email.reporting import delivery_evidence
    all_messages = (await s.db.scalars(s.query(Message).where(Message.lead_id == lead.id))).all()
    latest_inbound = max(
        (row for row in all_messages if row.direction == 'inbound' and row.kind == 'reply'),
        key=lambda row: (row.created_at, row.id), default=None,
    )
    delivered, failed, _ = delivery_evidence(all_messages)
    parent_headers = {row.in_reply_to for row in rows if row.in_reply_to}
    parents = list((await s.db.scalars(s.query(Message).where(
        Message.lead_id == lead.id, Message.message_id.in_(parent_headers)
    ))).all()) if parent_headers else []
    parent_map = {}
    for parent in parents:
        parent_map.setdefault(parent.message_id, []).append(parent)
    attachment_ids = {identifier for row in [*rows, *parents, *([latest_inbound] if latest_inbound else [])]
                      for identifier in json.loads(row.attachment_ids)}
    attachments = list((await s.db.scalars(s.query(Attachment).where(Attachment.id.in_(attachment_ids)))).all()) if attachment_ids else []
    files = {row.id: {"id": row.id, "name": row.name, "size": row.size} for row in attachments}

    def present(row):
        return dict(s.message_json(row),
            deliveryStatus="delivered" if row.id in delivered else "failed" if row.id in failed else "unconfirmed",
            attachments=[files[i] for i in json.loads(row.attachment_ids) if i in files])
    messages = []
    for row in reversed(rows):
        matches = parent_map.get(row.in_reply_to, [])
        messages.append(dict(present(row), replyTo=present(matches[0]) if len(matches) == 1 else None))
    first, send_blocked_reason = await s.manual_send_context(lead)
    return SuccessResponse(
        data=dict(
            **(await s.lead_json(lead)),
            messages=messages,
            variables=json.loads(lead.values),
            firstMessage=present(first) if first else None,
            latestInbound=present(latest_inbound) if latest_inbound else None,
            canSend=not send_blocked_reason,
            sendBlockedReason=send_blocked_reason,
            doNotContact=bool(
                await s.db.scalar(s.query(Suppression).where(Suppression.email == lead.email))
            ),
            **meta,
        )
    )


@EmailRouter.post("/conversations/{identifier}/read")
async def mark_conversation_read(identifier: str, data: ReadInput, s: Annotated[EmailService, Depends(service)]):
    lead = await s.owned(Lead, identifier)
    observed = data.last_reply_at
    if observed.tzinfo:
        observed = observed.astimezone(timezone.utc).replace(tzinfo=None)
    # 只标记用户实际打开的回复，期间新到的回复仍保持未查看。
    await s.db.execute(update(Lead).where(
        *scope(Lead, s.tenant, s.owner), Lead.id == lead.id,
        Lead.last_reply_at == observed,
    ).values(last_read_reply_at=observed))
    return SuccessResponse(data={"id": lead.id})


@EmailRouter.post("/conversations/{identifier}/reply")
async def reply(identifier: str, data: ReplyInput, s: Annotated[EmailService, Depends(service)]):
    return SuccessResponse(data=await s.reply(identifier, data, "reply"), status_code=202)


@EmailRouter.post("/conversations/{identifier}/follow-up")
async def follow_up(
    identifier: str, data: ReplyInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(
        data=await s.reply(identifier, data, "manual_follow_up"), status_code=202
    )


@EmailRouter.post("/messages/{identifier}/resolve")
async def resolve(
    identifier: str, data: ResolveInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.resolve_message(identifier, data))


@EmailRouter.post("/leads/{identifier}/do-not-contact")
async def suppress(
    identifier: str, data: SuppressInput, s: Annotated[EmailService, Depends(service)]
):
    return SuccessResponse(data=await s.do_not_contact(identifier, data.reason))
