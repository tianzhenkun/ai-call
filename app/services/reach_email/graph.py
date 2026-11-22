"""Outlook Graph 发信：显式指定发件地址，分块上传附件，区分提交失败和结果未知。"""

import base64
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlsplit

import requests

ROOT = 'https://graph.microsoft.com/v1.0'
CHUNK_BYTES = 3 * 1024 * 1024


class GraphError(ValueError):
    def __init__(self, code, message, *, status='failed', stage='authorize', server_code=None,
                 category='rejected', retry_after=None, operation=None, network_error=None):
        super().__init__(message)
        self.code, self.status, self.stage = code, status, stage
        self.server_code, self.category, self.retry_after = server_code, category, retry_after
        self.operation, self.network_error = operation, network_error

    def diagnostic(self):
        return {'stage': self.stage, 'serverCode': self.server_code, 'category': self.category,
                'message': str(self), 'transport': 'graph',
                **({'operation': self.operation, 'networkError': self.network_error} if self.network_error else {}),
                **({'scope': 'message', 'retryAfterSeconds': self.retry_after}
                   if self.category == 'rate_limit' else {})}


def retry_after(value):
    try:
        seconds = int(value) if str(value).isdigit() else int(
            (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        return seconds if 0 < seconds <= 365 * 86400 else None
    except (ValueError, TypeError, OverflowError):
        return None


def request(method, path, token, deadline, *, expected, stage, operation=None, read_json=True, **kwargs):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise GraphError('GRAPH_PREPARE_TIMEOUT', '邮件准备已超时，尚未提交发送。',
                         status='retryable', stage='prepare')
    headers = kwargs.pop('headers', {})
    if token:
        headers['Authorization'] = 'Bearer ' + token
    try:
        response = requests.request(method, path if path.startswith('https://') else ROOT + path,
            headers=headers, timeout=min(20, remaining), allow_redirects=False, **kwargs)
    except requests.RequestException as exc:
        # 最终发送调用可能已被微软接受；不能因读超时或连接中断重新提交。
        # 只记录固定操作名和异常类型；URL 与异常原文可能包含临时上传凭据。
        submitting = stage == 'submit'
        raise GraphError('GRAPH_SUBMISSION_UNKNOWN' if submitting else 'GRAPH_UNAVAILABLE',
            '微软发信结果尚未确认，请核对已发送邮件后人工处理。' if submitting else '暂时无法连接微软邮件服务，请稍后重试。',
            status='unknown' if submitting else 'retryable', stage=stage,
            operation=operation, network_error=type(exc).__name__) from None
    code = response.status_code
    if code in expected:
        if code in (202, 204) or not read_json:
            return {}
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError()
            return data
        except ValueError:
            raise GraphError('GRAPH_INVALID_RESPONSE', '微软邮件服务返回了无效响应。', stage=stage) from None
    if code == 429:
        raise GraphError('PROVIDER_RATE_LIMIT', '微软限制了请求频率，稍后按服务商等待时间重试。',
            status='retryable', stage=stage, server_code=code, category='rate_limit',
            retry_after=retry_after(response.headers.get('Retry-After')))
    if code in (401, 403):
        raise GraphError('GRAPH_AUTH_FAILED', '微软未允许当前邮件操作，请重新授权；若仍失败，请核查邮箱状态。',
            stage=stage, server_code=code, category='authentication')
    uncertain = stage == 'submit' and (code >= 500 or code == 408 or 200 <= code < 300)
    raise GraphError('GRAPH_SUBMISSION_UNKNOWN' if uncertain else f'GRAPH_HTTP_{code}',
        '微软发信结果尚未确认，请核对已发送邮件后人工处理。' if uncertain else
        '微软未接受此邮件请求，请检查邮件内容、附件大小和邮箱状态。',
        status='unknown' if uncertain else 'retryable' if code >= 500 or code == 408 else 'failed',
        stage=stage, server_code=code)


def token(config):
    if config.get('provider_type') != 'outlook_personal' or not config.get('graph_access_token'):
        raise GraphError('OUTLOOK_REAUTHORIZE', '请重新连接微软账号并同意邮件权限。', category='authentication')
    return config['graph_access_token']


def probe(config):
    try:
        access = token(config)
        deadline = time.monotonic() + 40
        # 个人账号的资料邮箱可能是外部登录名；比较实际收件箱，不能把登录别名当作发件地址。
        inbox = request('GET', '/me/mailFolders/inbox', access, deadline, expected=(200,), stage='authorize',
                        operation='read_inbox', params={'$select': 'id'})
        try:
            mailbox = request('GET', f"/users/{quote(config['email'], safe='')}/mailFolders/inbox",
                access, deadline, expected=(200,), stage='authorize', operation='verify_mailbox', params={'$select': 'id'})
        except GraphError as exc:
            if exc.server_code != 404:
                raise
            raise GraphError('OUTLOOK_MAILBOX_MISMATCH', '微软授权账号与所填 Outlook 邮箱不一致，请核对邮箱地址并重新连接。') from None
        if not isinstance(inbox.get('id'), str) or not inbox['id'] or mailbox.get('id') != inbox['id']:
            raise GraphError('OUTLOOK_MAILBOX_MISMATCH', '微软授权账号与所填 Outlook 邮箱不一致，请核对邮箱地址并重新连接。')
        return {'status': 'ok', 'stage': 'complete', 'errorCode': None, 'transport': 'graph',
                'message': 'Graph 身份、邮件访问及发信授权已验证；尚未发送邮件。', 'suggestion': ''}
    except GraphError as exc:
        return {'status': 'failed', 'errorCode': exc.code, **exc.diagnostic(),
                'suggestion': '请核对 Outlook 邮箱地址和微软授权，实际投递需通过发信验收确认。'}


def upload_attachment(path, item, access, deadline):
    content = item['content']
    if len(content) < CHUNK_BYTES:
        request('POST', path + '/attachments', access, deadline, expected=(201,), stage='prepare',
            operation='attach_file',
            json={'@odata.type': '#microsoft.graph.fileAttachment', 'name': item['filename'],
                  'contentType': item['content_type'], 'contentBytes': base64.b64encode(content).decode('ascii')})
        return
    session = request('POST', path + '/attachments/createUploadSession', access, deadline,
        expected=(201,), stage='prepare', operation='create_upload_session', json={'AttachmentItem': {'attachmentType': 'file',
            'name': item['filename'], 'size': len(content), 'contentType': item['content_type']}})
    url = session.get('uploadUrl', '')
    # 个人邮箱实测也返回 office365 主机；只允许这两个微软主机，不改写含临时授权的 URL。
    try:
        parsed = urlsplit(url)
        valid = (parsed.scheme == 'https' and parsed.hostname in ('outlook.office.com', 'outlook.office365.com')
            and parsed.port in (None, 443)
            and not (parsed.username or parsed.password or parsed.fragment) and parsed.path.startswith('/api/'))
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise GraphError('GRAPH_UPLOAD_URL_INVALID', '微软附件上传地址校验失败。', stage='prepare')
    for start in range(0, len(content), CHUNK_BYTES):
        chunk = content[start:start + CHUNK_BYTES]
        end = start + len(chunk)
        reply = request('PUT', url, None, deadline, expected=(201,) if end == len(content) else (200,),
            stage='prepare', operation='upload_chunk', read_json=end < len(content), headers={'Content-Type': 'application/octet-stream',
                'Content-Range': f'bytes {start}-{end - 1}/{len(content)}', 'Content-Length': str(len(chunk))},
            data=chunk)
        if end < len(content) and reply.get('nextExpectedRanges') not in ([str(end)], [f'{end}-']):
            raise GraphError('GRAPH_UPLOAD_RANGE_INVALID', '微软附件上传进度不一致，尚未提交邮件。', stage='prepare')


def verify_draft(draft, sender):
    for field in ('from', 'sender'):
        identity = draft.get(field)
        address = identity.get('emailAddress') if isinstance(identity, dict) else None
        value = address.get('address') if isinstance(address, dict) else None
        if not isinstance(value, str) or value.casefold() != sender.casefold():
            raise GraphError('GRAPH_SENDER_MISMATCH',
                '微软保存的发件地址与所选邮箱不一致，已停止发送，请联系平台支持核查。', stage='prepare')
    provider_id = draft.get('internetMessageId')
    if not isinstance(provider_id, str) or not re.fullmatch(r'<[^<>\s]{1,510}>', provider_id):
        raise GraphError('GRAPH_MESSAGE_ID_MISSING', '微软未返回邮件关联编号，尚未提交发送。', stage='prepare')
    return provider_id


def send(config, message, attachments):
    draft_path, provider_id = None, None
    submitted, keep_draft = False, False
    deadline = config.get('submission_deadline', time.monotonic() + 90)
    try:
        access = token(config)
        sender = message['From'].addresses[0]
        body = message.get_body(preferencelist=('html', 'plain'))
        # MIME 创建会将部分登录别名邮箱的 From 改写；JSON 保留显式指定的发件地址。
        payload = {'subject': str(message['Subject']),
            'body': {'contentType': 'HTML' if body.get_content_subtype() == 'html' else 'Text',
                     'content': body.get_content()},
            'from': {'emailAddress': {'address': sender.addr_spec, 'name': sender.display_name}},
            'toRecipients': [{'emailAddress': {'address': address.addr_spec, 'name': address.display_name}}
                             for address in message['To'].addresses]}
        # JSON 的自定义 Internet 头只允许 x- 前缀；标准回复头使用微软对应的 MAPI 属性。
        reply_headers = [{'id': prop, 'value': str(message[header])}
            for header, prop in (('In-Reply-To', 'String 0x1042'), ('References', 'String 0x1039'))
            if message[header]]
        if reply_headers:
            payload['singleValueExtendedProperties'] = reply_headers
        draft = request('POST', '/me/messages', access, deadline, expected=(201,), stage='prepare',
            operation='create_draft', json=payload)
        if not isinstance(draft.get('id'), str) or not draft['id'] or draft.get('isDraft') is not True:
            raise GraphError('GRAPH_DRAFT_INVALID', '微软未返回可发送的邮件草稿。', stage='prepare')
        draft_path = '/me/messages/' + quote(draft['id'], safe='')
        provider_id = verify_draft(draft, sender.addr_spec)
        for item in attachments:
            upload_attachment(draft_path, item, access, deadline)
        saved = request('GET', draft_path, access, deadline, expected=(200,), stage='prepare',
            operation='verify_draft', params={'$select': 'id,isDraft,from,sender,internetMessageId'})
        if saved.get('id') != draft['id'] or saved.get('isDraft') is not True:
            # 状态已变化或无法确认时，不再发送，也不能把可能已发送的项目当草稿删除。
            keep_draft = True
            raise GraphError('GRAPH_DRAFT_INVALID', '微软草稿状态已变化，已停止发送，请核查邮箱记录。', stage='prepare')
        provider_id = verify_draft(saved, sender.addr_spec)
        if deadline <= time.monotonic():
            raise GraphError('GRAPH_PREPARE_TIMEOUT', '邮件准备已超时，尚未提交发送。', status='retryable', stage='prepare')
        submitted = True
        request('POST', draft_path + '/send', access, deadline, expected=(202,), stage='submit', operation='send_draft')
        keep_draft = True
        return {'status': 'accepted', 'errorCode': None, 'submitted': True,
                'providerMessageId': provider_id,
                'diagnostic': {'transport': 'graph', 'stage': 'submit', 'serverCode': 202,
                    'message': '微软已接受发送请求，实际投递结果以收件及退信为准。'}}
    except GraphError as exc:
        keep_draft = keep_draft or exc.status == 'unknown'
        return {'status': exc.status, 'errorCode': exc.code, 'diagnostic': exc.diagnostic(),
                'submitted': submitted and exc.stage == 'submit', 'providerMessageId': provider_id}
    except Exception:
        # 提交之后的非预期异常也不能降为确定失败，避免人工重试导致重复邮件。
        keep_draft = submitted
        error = GraphError('GRAPH_SUBMISSION_UNKNOWN' if submitted else 'GRAPH_PREPARE_FAILED',
            '微软发信结果尚未确认，请核对已发送邮件后人工处理。' if submitted else '邮件准备失败，尚未提交发送。',
            status='unknown' if submitted else 'failed', stage='submit' if submitted else 'prepare')
        return {'status': error.status, 'errorCode': error.code, 'diagnostic': error.diagnostic(),
                'submitted': submitted, 'providerMessageId': provider_id}
    finally:
        if draft_path and not keep_draft:
            try:
                request('DELETE', draft_path, access, time.monotonic() + 10, expected=(204,), stage='cleanup', operation='cleanup_draft')
            except Exception:
                # 仅清理本次创建的未发送草稿；失败时保留草稿，不影响原始错误结果。
                pass
