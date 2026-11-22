"""Outlook 个人邮箱：单次授权流程及跨进程共享的加密 MSAL 缓存。"""

import asyncio
import hashlib
import json
import logging
import secrets
import time
from datetime import timedelta
from urllib.parse import urlsplit
from uuid import UUID

import msal
import requests
from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError

from app.config.setting import settings
from app.services.reach_email import transport
from app.services.reach_email.content import email_address
from app.services.reach_email.models import Account, OutlookAuthorization, WorkerLease, new_id, now
from app.services.reach_email.schema import AccountInput
from app.services.reach_email.security import CredentialCipher, configured_secret

PROVIDER = "outlook_personal"
STATE_PREFIX = "reach-outlook."
AUTHORITY = "https://login.microsoftonline.com/consumers"
CONSUMER_TENANT = "9188040d-6c67-4c5b-b112-36a304b66dad"
SCOPES = ["https://outlook.office.com/IMAP.AccessAsUser.All", "https://outlook.office.com/SMTP.Send"]
IMAP_SCOPES = SCOPES[:1]
GRAPH_SCOPES = ["https://graph.microsoft.com/" + name for name in ("User.Read", "Mail.Send", "Mail.ReadWrite")]
# 应用开发环境会接管标准库 DEBUG 日志，禁止 MSAL 输出授权请求和缓存事件。
logging.getLogger("msal").setLevel(logging.WARNING)


class OutlookError(ValueError):
    def __init__(self, code, message, *, reauthorize=False):
        super().__init__(message)
        self.code = code
        self.reauthorize = reauthorize

    def diagnostic(self):
        return {"status": "failed", "stage": "authorize", "errorCode": self.code, "message": str(self),
                    "suggestion": "请重新连接微软账号。" if self.reauthorize else "请稍后重试或联系平台管理员。",
                    "checkedAt": now().isoformat() + "+00:00"}


def configuration():
    try:
        client_id = settings.REACH_EMAIL_OUTLOOK_CLIENT_ID.strip()
        UUID(client_id)
        secret = configured_secret(settings.REACH_EMAIL_OUTLOOK_CLIENT_SECRET,
            settings.REACH_EMAIL_OUTLOOK_CLIENT_SECRET_FILE, "REACH_EMAIL_OUTLOOK_CLIENT_SECRET")
        redirect = settings.REACH_EMAIL_OUTLOOK_REDIRECT_URI.strip()
        url = urlsplit(redirect)
        local = url.hostname in ("localhost", "127.0.0.1")
        if (not secret or not url.hostname or url.username or url.password or url.fragment
            or url.scheme not in (("http", "https") if local else ("https",))
            or url.path != "/reach/email" or url.query):
            raise ValueError("invalid configuration")
        return client_id, secret, redirect
    except ValueError:
        raise OutlookError("OUTLOOK_NOT_CONFIGURED", "平台尚未完成微软应用配置，请联系平台管理员。") from None


def available():
    try:
        configuration()
        return True
    except OutlookError:
        return False


def client(config, cache):
    return msal.ConfidentialClientApplication(config[0], client_credential=config[1],
        authority=AUTHORITY, token_cache=cache, timeout=10, instance_discovery=False,
        enable_pii_log=False)


async def microsoft_call(fn):
    try:
        return await asyncio.to_thread(fn)
    except (requests.RequestException, OSError):
        raise OutlookError("OUTLOOK_UNAVAILABLE", "暂时无法连接微软授权服务，请稍后重试。") from None
    except OutlookError:
        raise
    except (ValueError, RuntimeError):
        # MSAL 原始错误可能包含授权码、用户资料或应用配置，不能透传。
        raise OutlookError("OUTLOOK_AUTH_INVALID", "微软授权校验失败，请重新发起连接；若持续失败，请联系平台管理员。") from None


def checked_token(result, scopes=SCOPES):
    if not result or not result.get("access_token"):
        error = (result or {}).get("error")
        if result is None or error in ("invalid_grant", "interaction_required", "login_required", "consent_required"):
            raise OutlookError("OUTLOOK_REAUTHORIZE", "微软授权已失效，需要重新连接。", reauthorize=True)
        raise OutlookError("OUTLOOK_AUTH_FAILED", "微软未完成授权，请重试；若持续失败，请联系平台管理员检查应用配置。")
    # MSAL 已按账号、应用和所需 scopes 匹配缓存；缓存命中响应不再附带 scope。
    if result.get("token_source") == "cache" and "scope" not in result:
        return result["access_token"]
    granted = {s.rsplit("/", 1)[-1].lower() for s in result.get("scope", "").split()}
    if not {s.rsplit("/", 1)[-1].lower() for s in scopes} <= granted:
        raise OutlookError("OUTLOOK_SCOPE_MISSING", "微软授权缺少邮件收发权限，请重新连接并同意所需权限。", reauthorize=True)
    return result["access_token"]


def grant_secret(cache, result, client_id, home_account_id=None, *, graph_result=None):
    token = checked_token(result, IMAP_SCOPES if graph_result else SCOPES)
    graph_token = checked_token(graph_result, GRAPH_SCOPES) if graph_result else None
    if not list(cache.search(msal.TokenCache.CredentialType.REFRESH_TOKEN)):
        raise OutlookError("OUTLOOK_OFFLINE_MISSING", "微软未授予持续访问权限，请重新连接。", reauthorize=True)
    accounts = list(cache.search(msal.TokenCache.CredentialType.ACCOUNT))
    if not home_account_id:
        if len(accounts) != 1:
            raise OutlookError("OUTLOOK_ACCOUNT_INVALID", "无法确认微软授权账号，请重新连接。")
        home_account_id = accounts[0]["home_account_id"]
    account = next((a for a in accounts if a["home_account_id"] == home_account_id), {})
    try:
        smtp_username = email_address(account.get("username", ""))
    except ValueError:
        raise OutlookError("OUTLOOK_ACCOUNT_INVALID", "无法确认微软登录邮箱，请重新连接。", reauthorize=True) from None
    expires = min(int(item.get("expires_in", 0)) for item in (result, graph_result) if item)
    return {"msal_cache": cache.serialize(), "client_id": client_id, "home_account_id": home_account_id,
                "access_token": token, "smtp_username": smtp_username,
                "expires_at": time.time() + expires,
                **({"send_transport": "graph", "graph_access_token": graph_token} if graph_token else {})}


def flow_id(state):
    return hashlib.sha256(state.encode()).hexdigest()[:32]


async def start(s, data):
    config = configuration()
    account = data.account
    if account.provider_type != PROVIDER or account.smtp_password or account.imap_password:
        raise ValueError("Outlook 个人邮箱请使用微软授权连接，无需填写密码")
    row = await s.owned(Account, data.account_id) if data.account_id else None
    if row and (row.email != account.email or json.loads(row.config).get("provider_type") != PROVIDER):
        raise ValueError("重新授权须使用原 Outlook 邮箱地址；更换邮箱请新增账号")
    from app.services.reach_email.providers import mailbox_service

    duplicate = await s.db.scalar(s.query(Account).where(
        Account.email == account.email, Account.service_key == mailbox_service(account.model_dump())))
    if duplicate and (not row or duplicate.id != row.id):
        raise ValueError("此邮箱已添加，请在原邮箱上重新授权")
    version = row.version if row else None
    await s.db.commit()
    state = STATE_PREFIX + secrets.token_urlsafe(32)
    flow = await microsoft_call(lambda: client(config, msal.SerializableTokenCache()).initiate_auth_code_flow(
        GRAPH_SCOPES + IMAP_SCOPES, redirect_uri=config[2], state=state, prompt="select_account"))
    if not flow.get("auth_uri"):
        raise OutlookError("OUTLOOK_AUTH_FAILED", "无法创建微软授权请求，请联系平台管理员。")
    await s.db.execute(delete(OutlookAuthorization).where(OutlookAuthorization.expires_at <= now()))
    pending = s.add(OutlookAuthorization, expires_at=now() + timedelta(minutes=10),
        payload=CredentialCipher(s.key).encrypt({"flow": flow, "account": account.model_dump(),
            "account_id": data.account_id, "version": version, "client_id": config[0], "redirect_uri": config[2],
            "send_transport": "graph"}))
    pending.id = flow_id(state)
    return {"authorizationUrl": flow["auth_uri"]}


async def complete(s, data):
    row = await s.db.scalar(s.query(OutlookAuthorization).where(
        OutlookAuthorization.id == flow_id(data.state), OutlookAuthorization.expires_at > now()))
    if not row or not data.state.startswith(STATE_PREFIX):
        raise ValueError("授权已过期或已使用，请返回邮箱列表重新连接")
    payload = CredentialCipher(s.key).decrypt(row.payload)
    if not secrets.compare_digest(payload["flow"]["state"], data.state):
        raise ValueError("授权校验失败，请重新连接")
    consumed = await s.db.execute(delete(OutlookAuthorization).where(
        OutlookAuthorization.id == row.id, OutlookAuthorization.expires_at > now()))
    await s.db.commit()  # 授权码只允许消费一次，包括取消和网络失败。
    if not consumed.rowcount:
        raise ValueError("授权已过期或已使用，请重新连接")
    if data.error or not data.code:
        raise ValueError("未完成微软授权，邮箱配置未改变；请重新连接并同意邮件权限")
    config = configuration()
    if (payload["client_id"], payload["redirect_uri"]) != (config[0], config[2]):
        raise ValueError("平台微软应用配置已变更，请重新连接")
    cache = msal.SerializableTokenCache()
    graph = payload.get("send_transport") == "graph"
    result = await microsoft_call(lambda: client(config, cache).acquire_token_by_auth_code_flow(
        payload["flow"], {"state": data.state, "code": data.code}, scopes=GRAPH_SCOPES if graph else SCOPES))
    checked_token(result, GRAPH_SCOPES if graph else SCOPES)
    if result.get("id_token_claims", {}).get("tid") != CONSUMER_TENANT:
        raise ValueError("请选择 Outlook 个人账号，当前入口不支持 Microsoft 365 企业账号")
    graph_result = result if graph else None
    if graph:
        # 一次同意两种资源权限；分别换取令牌，Graph 令牌不能用于 IMAP。
        accounts = list(cache.search(msal.TokenCache.CredentialType.ACCOUNT))
        if len(accounts) != 1:
            raise OutlookError("OUTLOOK_ACCOUNT_INVALID", "无法确认微软授权账号，请重新连接。")
        result = await microsoft_call(lambda: client(config, cache).acquire_token_silent_with_error(
            IMAP_SCOPES, account=accounts[0]))
        checked_token(result, IMAP_SCOPES)
    credential = grant_secret(cache, result, config[0], graph_result=graph_result)
    account = AccountInput(**payload["account"])
    # 旧授权继续验证 SMTP；新授权验证 Graph 身份和权限，IMAP 始终验证所填邮箱。
    tested = await asyncio.to_thread(transport.test_account,
        {**account.model_dump(), "smtp_username": credential["smtp_username"],
         "access_token": credential["access_token"],
         **({"send_transport": "graph", "graph_access_token": credential["graph_access_token"]} if graph else {})})
    current = await s.owned(Account, payload["account_id"], lock=True) if payload["account_id"] else None
    if current and current.version != payload["version"]:
        raise ValueError("邮箱配置版本已变更，请重新授权")
    failed = any(item["status"] != "ok" for item in tested.values())
    connection_error = None
    if failed:
        details = []
        for protocol, label in (("smtp", "发信（Graph）" if graph else "发信（SMTP）"), ("imap", "收信（IMAP）")):
            item = tested[protocol]
            if item["status"] == "ok":
                details.append(f"{label}：验证通过。")
            else:
                codes = " / ".join(str(item[key]) for key in ("errorCode", "serverCode", "enhancedCode") if item.get(key))
                details.append(f"{label}：{item['message']} {item.get('suggestion', '')}"
                               + (f"（错误码：{codes}）" if codes else ""))
        details = "\n".join(details)
        if current and current.oauth_status == "connected" and current.secret:
            return {**(await s.account_json(current)), "connectionError":
                "微软授权已完成，但本次连接验证未通过；原邮箱配置已保留。\n" + details}
        connection_error = "微软授权已完成，但连接验证未通过。已在列表保留停用的邮箱记录，请重新连接。\n" + details
    saved = await s.save_account(account, payload["account_id"],
        oauth_secret=None if failed else credential, oauth_pending=failed)
    current = await s.owned(Account, saved["id"])
    for protocol, item in tested.items():
        item["checkedAt"] = now().isoformat() + "+00:00"
        setattr(current, f"{protocol}_status", item["status"])
        setattr(current, f"{protocol}_test_result", json.dumps(item, ensure_ascii=False))
    result = await s.account_json(current)
    return {**result, "connectionError": connection_error} if connection_error else result


async def disconnect(s, identifier):
    row = await s.owned(Account, identifier, lock=True)
    if json.loads(row.config).get("provider_type") != PROVIDER:
        raise ValueError("此邮箱未使用微软授权")
    row.secret, row.oauth_status, row.enabled = "", "disconnected", False
    row.smtp_status = row.imap_status = "untested"
    row.smtp_test_result = row.imap_test_result = None
    row.version += 1
    await s.db.flush()
    return await s.account_json(row)


async def connection_config(sessions, cipher, identifier, tenant, owner, version):
    condition = (Account.id == identifier, Account.tenant_id == tenant, Account.owner_id == owner,
                 Account.version == version)
    async with sessions() as db:
        row = await db.scalar(select(Account).where(*condition))
        if row is None:
            raise OutlookError("ACCOUNT_CHANGED", "邮箱配置已变更，请重试。")
        config = dict(json.loads(row.config), email=row.email)
        if config.get("provider_type") != PROVIDER:
            return {**config, **cipher.decrypt(row.secret)}
        if row.oauth_status != "connected" or not row.secret:
            raise OutlookError("OUTLOOK_REAUTHORIZE", "请先重新连接微软账号。", reauthorize=True)
        oauth = configuration()
        credential = cipher.decrypt(row.secret)
        client_changed = credential.get("client_id") != oauth[0]
        graph = config.get("send_transport") == "graph"
        if (not client_changed and credential.get("expires_at", 0) > time.time() + 300
            and (not graph or credential.get("graph_access_token"))):
            return {**config, "smtp_username": credential.get("smtp_username", config["smtp_username"]),
                    **{k: credential[k] for k in ("access_token", "graph_access_token") if k in credential}}
        old_secret = row.secret
        token = new_id()
        locked = await db.execute(update(WorkerLease).where(
            WorkerLease.id == identifier, WorkerLease.expires_at <= now()
        ).values(token=token, expires_at=now() + timedelta(seconds=120)))
        if not locked.rowcount:
            try:
                async with db.begin_nested():
                    db.add(WorkerLease(id=identifier, token=token, expires_at=now() + timedelta(seconds=120)))
                    await db.flush()
            except IntegrityError:
                raise OutlookError("OUTLOOK_REFRESH_BUSY", "微软授权正在更新，请稍后重试。") from None
        await db.commit()
    try:
        cache = msal.SerializableTokenCache()
        cache.deserialize(credential["msal_cache"])

        def refresh():
            app = client(oauth, cache)
            accounts = app.get_accounts()
            account = next((a for a in accounts if a["home_account_id"] == credential["home_account_id"]), None)
            if not account:
                return {"error": "invalid_grant"}, None
            graph_result = app.acquire_token_silent_with_error(GRAPH_SCOPES, account=account) if graph else None
            if graph:
                checked_token(graph_result, GRAPH_SCOPES)
            return app.acquire_token_silent_with_error(IMAP_SCOPES if graph else SCOPES, account=account), graph_result

        updates, failure = {}, None
        try:
            if client_changed:
                raise OutlookError("OUTLOOK_APP_CHANGED", "平台微软应用已变更，请重新连接。", reauthorize=True)
            result, graph_result = await microsoft_call(refresh)
            renewed = grant_secret(cache, result, oauth[0], credential["home_account_id"], graph_result=graph_result)
            updates["secret"] = cipher.encrypt(renewed)
        except OutlookError as exc:
            if not exc.reauthorize:
                raise
            failure = exc
            updates = {"oauth_status": "reauthorize", "secret": "", "version": version + 1,
                "smtp_status": "failed", "imap_status": "failed",
                "smtp_test_result": json.dumps(exc.diagnostic(), ensure_ascii=False),
                "imap_test_result": json.dumps(exc.diagnostic(), ensure_ascii=False)}
        async with sessions() as db, db.begin():
            lease = await db.scalar(select(WorkerLease).where(WorkerLease.id == identifier,
                WorkerLease.token == token, WorkerLease.expires_at > now()).with_for_update())
            if not lease:
                raise OutlookError("OUTLOOK_REFRESH_EXPIRED", "微软授权更新超时，请重试。")
            saved = await db.execute(update(Account).where(*condition,
                Account.secret == old_secret, Account.oauth_status == "connected").values(**updates))
            if not saved.rowcount:
                raise OutlookError("ACCOUNT_CHANGED", "邮箱配置已变更，请重试。")
        if failure:
            raise failure
        return {**config, "smtp_username": renewed["smtp_username"],
                **{k: renewed[k] for k in ("access_token", "graph_access_token") if k in renewed}}
    finally:
        async with sessions() as db, db.begin():
            await db.execute(delete(WorkerLease).where(WorkerLease.id == identifier, WorkerLease.token == token))
