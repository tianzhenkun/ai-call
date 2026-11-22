"""Bounded, TLS-only SMTP/IMAP operations; callers run these off the event loop."""
from __future__ import annotations

import html as html_module
import imaplib
import ipaddress
import re
import smtplib
import socket
import ssl
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import formatdate, getaddresses

import bleach

from app.services.reach_email.content import strip_non_content_html
from app.services.reach_email.limits import (
    ATTACHMENT_ERROR_CODES,
    MAX_ATTACHMENT_BYTES,
    MAX_ATTACHMENT_COUNT,
    MAX_INBOUND_MESSAGE_BYTES,
    MAX_OUTBOUND_MESSAGE_BYTES,
)

TIMEOUT = 20
ATTACHMENT_EXTENSIONS = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                         '.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp'}


def sanitize_html(value: str) -> str:
    return bleach.clean(strip_non_content_html(value), tags=['p', 'br', 'div', 'span', 'b', 'strong', 'i', 'em',
                                     'u', 'ul', 'ol', 'li', 'blockquote', 'a', 'table',
                                     'thead', 'tbody', 'tr', 'th', 'td'],
                        attributes={'a': ['href', 'title']},
                        protocols=['https', 'http', 'mailto'], strip=True)


def validate_endpoint(host: str, port: int) -> list[str]:
    """Resolve once; connections must use the returned public addresses."""
    if not isinstance(host, str) or not host or any(c.isspace() for c in host):
        raise ValueError('INVALID_ENDPOINT')
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError('INVALID_ENDPOINT')
    try:
        addresses = list(dict.fromkeys(row[4][0] for row in socket.getaddrinfo(
            host, port, type=socket.SOCK_STREAM)))
    except (OSError, UnicodeError):
        raise ValueError('DNS_FAILED') from None
    if not addresses or any(not ipaddress.ip_address(addr).is_global for addr in addresses):
        raise ValueError('UNSAFE_ENDPOINT')
    return addresses


def _socket(host, port, timeout):
    addresses = validate_endpoint(host, port)
    for address in addresses:
        try:
            # A numeric address prevents a second, attacker-controlled DNS answer.
            return socket.create_connection((address, port), timeout)
        except OSError:
            if address == addresses[-1]:
                raise
    raise OSError('CONNECT_FAILED')


class _SMTP(smtplib.SMTP):
    def _get_socket(self, host, port, timeout):
        return _socket(host, port, timeout)


class _SMTPSSL(smtplib.SMTP_SSL):
    def _get_socket(self, host, port, timeout):
        return self.context.wrap_socket(_socket(host, port, timeout), server_hostname=host)


class _IMAP(imaplib.IMAP4):
    def _create_socket(self, timeout):
        return _socket(self.host, self.port, timeout)


class _IMAPSSL(imaplib.IMAP4_SSL):
    def _create_socket(self, timeout):
        return self.ssl_context.wrap_socket(_socket(self.host, self.port, timeout),
                                            server_hostname=self.host)


def _connect(config, protocol):
    host, port = config[f'{protocol}_host'], config[f'{protocol}_port']
    security = config.get(f'{protocol}_security')
    if security not in ('SSL_TLS', 'STARTTLS'):
        raise ValueError('TLS_REQUIRED')
    context = ssl.create_default_context()
    if protocol == 'imap' and host.lower().rstrip('.') in ('imap.139.com', 'imap.10086.cn'):
        # 139 的 IMAP 实测需要 RSA-AES-GCM；仅限官方主机，保留 TLS 1.2 下限及证书校验。
        context.set_ciphers(':'.join(c['name'] for c in context.get_ciphers()) + ':AES128-GCM-SHA256')
    conn = None
    stage = 'connect'
    try:
        if protocol == 'smtp':
            conn = (_SMTPSSL(host, port, timeout=TIMEOUT, context=context)
                    if security == 'SSL_TLS' else _SMTP(host, port, timeout=TIMEOUT))
            stage = 'greeting'
            conn.ehlo()
            if security == 'STARTTLS':
                stage = 'tls'
                conn.starttls(context=context)
                stage = 'greeting'
                conn.ehlo()
        else:
            conn = (_IMAPSSL(host, port, timeout=TIMEOUT, ssl_context=context)
                    if security == 'SSL_TLS' else _IMAP(host, port, timeout=TIMEOUT))
            if security == 'STARTTLS':
                stage = 'tls'
                conn.starttls(ssl_context=context)
        stage = 'authenticate'
        if config.get('provider_type') == 'outlook_personal':
            from app.services.reach_email.providers import PROVIDERS

            preset = PROVIDERS['outlook_personal']
            if any(config[f'{protocol}_{field}'] != preset[f'{protocol}{field.title()}']
                   for field in ('host', 'port', 'security')) or not config.get('access_token'):
                raise ValueError('INVALID_CONFIG_OR_MESSAGE')
            sasl = f"user={config[f'{protocol}_username']}\x01auth=Bearer {config['access_token']}\x01\x01"
            if protocol == 'smtp':
                conn.auth('XOAUTH2', lambda challenge=None: sasl if challenge is None else '')
            else:
                conn.authenticate('XOAUTH2', lambda challenge: sasl.encode() if not challenge else b'')
        else:
            conn.login(config[f'{protocol}_username'], config[f'{protocol}_password'])
        # 网易等服务商要求声明客户端身份后才允许访问收件箱。
        if protocol == 'imap' and 'ID' in conn.capabilities:
            stage = 'identify'
            status, _ = conn.xatom('ID', '("name" "REACH" "version" "1.0")')
            if status != 'OK':
                raise ValueError('IMAP_ID_FAILED')
        return conn
    except Exception as exc:
        # 保留原异常类型供发送/同步路径处理，仅附加连接测试所需的阶段。
        exc.mail_stage = stage
        if conn is not None:
            _close(conn)
        raise


def _close(conn):
    try:
        if isinstance(conn, imaplib.IMAP4):
            conn.logout()
        else:
            conn.close()
    except Exception:
        pass


def _error(exc):
    if isinstance(exc, ValueError) and str(exc) in {
        'INVALID_ENDPOINT', 'DNS_FAILED', 'UNSAFE_ENDPOINT', 'TLS_REQUIRED',
        'INVALID_MESSAGE', 'ATTACHMENT_LIMIT', 'MESSAGE_TOO_LARGE', 'IMAP_ID_FAILED', 'IMAP_SELECT_FAILED',
    } | ATTACHMENT_ERROR_CODES:
        return str(exc)
    if isinstance(exc, (smtplib.SMTPAuthenticationError, imaplib.IMAP4.error)):
        return 'AUTH_OR_PROTOCOL_FAILED'
    if isinstance(exc, ssl.SSLError):
        return 'TLS_FAILED'
    if isinstance(exc, (KeyError, TypeError, ValueError)):
        return 'INVALID_CONFIG_OR_MESSAGE'
    return 'CONNECTION_FAILED'


def _probe_error(exc, stage, provider_type=None):
    stage = getattr(exc, 'mail_stage', stage)
    code = _error(exc)
    if isinstance(exc, ssl.SSLError):
        stage = 'tls'
    elif isinstance(exc, TimeoutError):
        code = 'CONNECTION_TIMEOUT'
    elif isinstance(exc, ConnectionRefusedError):
        code = 'CONNECTION_REFUSED'
    elif isinstance(exc, (imaplib.IMAP4.abort, smtplib.SMTPServerDisconnected)):
        code = 'CONNECTION_CLOSED'
    elif isinstance(exc, (imaplib.IMAP4.error, smtplib.SMTPException)):
        code = {'authenticate': 'AUTH_FAILED', 'identify': 'IMAP_ID_FAILED',
                'select': 'IMAP_SELECT_FAILED'}.get(stage, 'PROTOCOL_FAILED')
        # 189 等服务商会明确说明协议未开启，不能误报为授权码错误。
        if isinstance(exc, imaplib.IMAP4.error) and re.search(r'\bimap (?:is )?disabled?\b', str(exc), re.I):
            code = 'IMAP_DISABLED'
    messages = {
        'DNS_FAILED': ('无法解析邮箱服务器地址。', '请核对邮箱类型和服务器地址，并检查服务器的 DNS。'),
        'UNSAFE_ENDPOINT': ('邮箱服务器解析到了受限制的网络地址。', '请核对邮箱服务器地址与 DNS，使用服务商提供的公网邮件服务器。'),
        'INVALID_ENDPOINT': ('邮箱服务器地址或端口无效。', '请按服务商官方指引核对连接参数。'),
        'TLS_REQUIRED': ('连接未使用受支持的加密方式。', '请按官方参数选择 SSL/TLS 或 STARTTLS。'),
        'TLS_FAILED': ('加密连接或服务器证书校验失败。', '请核对加密方式和端口；由管理员检查证书与系统时间，不要关闭证书校验。'),
        'AUTH_FAILED': ('邮箱服务器拒绝了登录认证。', '请检查完整邮箱地址、专用密码，以及管理员和成员的客户端协议权限。'),
        'IMAP_DISABLED': ('邮箱服务商尚未为此账号开启 IMAP。', '请在邮箱网页版的设置中开启 IMAP/SMTP 服务并完成安全验证，再重新测试；生成专用密码不会自动开启 IMAP。'),
        'IMAP_ID_FAILED': ('服务器未接受客户端身份声明。', '请确认管理员允许第三方 IMAP 客户端访问；若已开放，请提供此错误码给技术支持。'),
        'IMAP_SELECT_FAILED': ('已登录，但无法打开收件箱。', '请检查 IMAP 服务和收件箱访问权限，不能仅凭 SMTP 通过判断收信可用。'),
        'CONNECTION_TIMEOUT': ('连接或等待服务器响应超时。', '请检查邮件服务器端口、网络和访问白名单，稍后重试。'),
        'CONNECTION_REFUSED': ('邮件服务器拒绝了网络连接。', '请核对服务器地址、端口，以及防火墙或网络访问限制。'),
        'CONNECTION_CLOSED': ('邮件服务器提前断开连接。', '请稍后重试；若持续发生，请检查访问频率、客户端权限和服务器连接限制。'),
        'PROTOCOL_FAILED': ('邮件服务器未接受协议请求。', '请核对邮箱类型和协议配置，并联系管理员确认客户端支持情况。'),
        'INVALID_CONFIG_OR_MESSAGE': ('邮箱连接配置不完整或无效。', '请编辑邮箱，核对连接参数和授权凭据后重新测试。'),
    }
    message, suggestion = messages.get(code, ('未能完成邮箱服务器连接。', '请检查网络、服务器地址和端口，稍后重新测试。'))
    if provider_type == 'outlook_personal':
        if code == 'AUTH_FAILED':
            message = '微软未接受此邮箱的授权登录。'
            suggestion = '请使用此 Outlook 邮箱所属的微软账号重新连接；若仍失败，请联系平台支持核查授权与连接详情。'
        elif code == 'IMAP_DISABLED':
            suggestion = '请在 Outlook 网页版的“设置 → 邮件 → 转发和 IMAP”中开启 IMAP，再重新连接微软账号。'
    # 只暴露安全协议码和固定说明，服务商原始响应可能包含用户名或授权凭据。
    server_code = str(exc.smtp_code) if isinstance(exc, smtplib.SMTPResponseException) and isinstance(exc.smtp_code, int) else None
    enhanced = re.search(r'\b[245]\.\d{1,3}\.\d{1,3}\b', str(exc.smtp_error)) if server_code else None
    return {'status': 'failed', 'stage': stage, 'errorCode': code, 'message': message,
            'suggestion': suggestion, 'serverCode': server_code, 'enhancedCode': enhanced.group() if enhanced else None}


def test_account(config, protocols=('smtp', 'imap')) -> dict:
    result = {}
    for protocol in protocols:
        if protocol not in ('smtp', 'imap'):
            raise ValueError('INVALID_PROTOCOL')
        if protocol == 'smtp' and config.get('send_transport') == 'graph':
            from app.services.reach_email.graph import probe

            result[protocol] = probe(config)
            continue
        conn = None
        stage = 'connect'
        try:
            conn = _connect(config, protocol)
            if protocol == 'imap':
                stage = 'select'
                if conn.select('INBOX', readonly=True)[0] != 'OK':
                    raise ValueError('IMAP_SELECT_FAILED')
            result[protocol] = {'status': 'ok', 'stage': 'complete', 'errorCode': None,
                                'message': f'{protocol.upper()} 连接测试通过。', 'suggestion': ''}
        except Exception as exc:
            result[protocol] = _probe_error(exc, stage, config.get('provider_type'))
        finally:
            if conn is not None:
                _close(conn)
    return result


def _send_rejection(exc, stage, config):
    """仅从明确拒绝识别限流；原文只在内存分类，落库固定摘要和协议码。"""
    code = exc.smtp_code
    reply = exc.smtp_error.decode('utf-8', errors='replace') if isinstance(exc.smtp_error, bytes) else str(exc.smtp_error)
    reply = ' '.join(reply.lower().split())
    stage = getattr(exc, 'mail_stage', stage)
    enhanced = re.search(r'\b[245]\.\d{1,3}\.\d{1,3}\b', reply)
    diagnostic = {'stage': stage, 'serverCode': code,
        'enhancedCode': enhanced.group() if enhanced else None}
    result = {'status': 'retryable' if 400 <= code < 500 else 'failed',
        'errorCode': f'SMTP_{code}', 'diagnostic': diagnostic}
    if isinstance(exc, smtplib.SMTPAuthenticationError) or code in (530, 535):
        diagnostic.update(category='authentication', message='邮件服务器未接受身份认证，请检查授权凭据或客户端权限。')
        return result
    if diagnostic['enhancedCode'] in ('4.2.2', '5.2.2', '5.1.1') or re.search(
        r'mailbox (?:is )?full|(?:storage|mailbox) quota|recipient (?:does not exist|not found)', reply
    ):
        diagnostic.update(category='recipient', message='收件地址或收件箱容量受限，请核对收件方状态。')
        return result
    limited = re.search(
        r'\b(?:too frequently sending|rate limit exceeded|sending rate exceeded|too many messages)\b'
        r'|\b(?:user|ip) (?:quarter|today) (?:mail|rcpt) count exceeded\b'
        r'|\b(?:daily|hourly|sender|sending|outbound) (?:user |sending |send |mail |message |recipient )*(?:limit|quota) (?:exceeded|reached)\b'
        r'|(?:发信|发送)(?:过于频繁|频率过高|频率超限|数量超限|限额已用尽)', reply
    )
    if limited and stage not in ('authenticate', 'prepare'):
        # 只有声明范围的响应才扩大暂停范围；未知服务商的笼统限制仅等待当前邮件。
        scope = 'message'
        if re.search(r'\b(?:recipient|receiving|destination) domain\b|收件域', reply):
            scope = 'recipient_domain'
        elif re.search(r'\brecipient (?:rate|sending)\b|收件人(?:速率|频率)', reply):
            scope = 'recipient'
        elif re.search(r'\b(?:tenant|organization|sender domain)\b|企业(?:发送|发信)', reply):
            scope = 'enterprise'
        elif re.search(r'\b(?:client |outbound )?ip (?:quarter |today |daily |sending |rate |mail )*(?:count|limit|quota)|\bip today (?:mail|rcpt) count', reply):
            scope = 'server'
        elif re.search(r'\b(?:user|sender|account)\b', reply):
            scope = 'account'
        elif 'too frequently sending' in reply and config.get('smtp_host', '').lower().rstrip('.') in (
            'smtp.em.dingtalk.com', 'smtp.qiye.aliyun.com',
        ):
            scope = 'account'
        daily = bool(re.search(r'\b(?:daily|today|per day)\b|当日|每日|当天', reply))
        result['errorCode'] = 'PROVIDER_DAILY_LIMIT' if daily else 'PROVIDER_RATE_LIMIT'
        result['status'] = 'retryable'
        delay = re.search(r'\b(?:retry|try again|wait)(?: again)?(?: after| in| for)?\s+(\d+)\s*(seconds?|minutes?|hours?)\b', reply)
        seconds = None
        if delay:
            seconds = int(delay[1]) * {'s': 1, 'm': 60, 'h': 3600}[delay[2][0]]
            # 不截短服务商明确给出的等待期；不合理的时间转人工处理。
            if not 0 < seconds <= 365 * 86400:
                seconds = None
                diagnostic['manualReview'] = True
        diagnostic.update(category='rate_limit', scope=scope, retryAfterSeconds=seconds,
            message='服务商明确拒绝：当日发送额度已达上限。' if daily else '服务商明确拒绝：发送频率或数量受限。')
    else:
        diagnostic.update(category='temporary' if result['status'] == 'retryable' else 'rejected',
            message='邮件服务器暂时拒绝，尚无足够证据确认限流。' if result['status'] == 'retryable'
            else '邮件服务器拒绝本次发送，尚无足够证据确认限流。')
    return result


def send_message(config, *, recipient, subject, html, attachments, message_id,
                 in_reply_to=None, references=None) -> dict:
    conn = None
    submitting = False
    stage = 'prepare'
    try:
        sender = config.get('email') or config.get('email_address') or config['smtp_username']
        if any('\r' in str(v) or '\n' in str(v) for v in
               [sender, recipient, subject, message_id, in_reply_to or '', references or '']):
            raise ValueError('INVALID_MESSAGE')
        if not recipient or not message_id or len(subject) > 512:
            raise ValueError('INVALID_MESSAGE')
        for address in (sender, recipient):
            if not re.fullmatch(r'[^\s<>@,;]+@[^\s<>@,;]+\.[^\s<>@,;]+', address):
                raise ValueError('INVALID_MESSAGE')
        if len(attachments) > MAX_ATTACHMENT_COUNT:
            raise ValueError('ATTACHMENT_COUNT_LIMIT')
        for item in attachments:
            name = item['filename']
            if len(name) > 255:
                raise ValueError('ATTACHMENT_NAME_TOO_LONG')
            if '/' in name or '\\' in name or any(ord(c) < 32 for c in name):
                raise ValueError('INVALID_MESSAGE')
            if '.' + name.rsplit('.', 1)[-1].lower() not in ATTACHMENT_EXTENSIONS:
                raise ValueError('ATTACHMENT_TYPE_UNSUPPORTED')
            if not item['content']:
                raise ValueError('ATTACHMENT_EMPTY')
            if len(item['content']) > MAX_ATTACHMENT_BYTES:
                raise ValueError('ATTACHMENT_TOO_LARGE')
        if sum(len(a['content']) for a in attachments) > MAX_ATTACHMENT_BYTES:
            raise ValueError('ATTACHMENT_TOTAL_TOO_LARGE')
        msg = EmailMessage(policy=policy.SMTP)
        from email.utils import formataddr

        from_name = config.get('from_name', '')
        if '\r' in from_name or '\n' in from_name:
            raise ValueError('INVALID_MESSAGE')
        # 回复头可能被客户端省略，用原会话的随机编号提供可核验的补充关联。
        thread_id = re.fullmatch(r'<([0-9a-f]{32})@reach\.local>', in_reply_to or message_id)
        if not thread_id and config.get('send_transport') == 'graph':
            # 微软父邮件编号不含平台编号，此时用当前邮件作唯一回退标记，避免多标记歧义。
            thread_id = re.fullmatch(r'<([0-9a-f]{32})@reach\.local>', message_id)
            subject = re.sub(r'\s*\[REACH:[0-9a-f]{32}\]', '', subject).strip()
        if thread_id:
            marker = f'[REACH:{thread_id.group(1)}]'
            if marker not in subject:
                subject = f'{subject} {marker}'
        msg['From'], msg['To'], msg['Subject'] = formataddr((from_name, sender)), recipient, subject
        msg['Message-ID'], msg['Date'] = message_id, formatdate(localtime=False)
        if in_reply_to:
            msg['In-Reply-To'] = in_reply_to
        if references:
            msg['References'] = ' '.join(references) if isinstance(references, list) else references
        clean = sanitize_html(html)
        msg.set_content(html_module.unescape(bleach.clean(clean, tags=[], strip=True)))
        msg.add_alternative(clean, subtype='html')
        for item in attachments:
            main, sub = item['content_type'].split('/', 1)
            msg.add_attachment(item['content'], maintype=main, subtype=sub,
                               filename=item['filename'])
        wire = msg.as_bytes()
        if len(wire) > MAX_OUTBOUND_MESSAGE_BYTES:
            raise ValueError('MESSAGE_TOO_LARGE')
        if config.get('send_transport') == 'graph':
            from app.services.reach_email.graph import send

            return send(config, msg, attachments)
        stage = 'connect'
        conn = _connect(config, 'smtp')
        dsn = conn.has_extn('dsn')
        stage = 'mail'
        code, reply = conn.mail(sender, options=['RET=HDRS']) if dsn else conn.mail(sender)
        if code != 250:
            raise smtplib.SMTPResponseException(code, reply)
        stage = 'rcpt'
        code, reply = conn.rcpt(recipient, options=['NOTIFY=SUCCESS,FAILURE,DELAY']) if dsn else conn.rcpt(recipient)
        if code not in (250, 251):
            raise smtplib.SMTPResponseException(code, reply)
        submitting = True
        stage = 'data'
        code, reply = conn.data(wire)
        if code != 250:
            raise smtplib.SMTPResponseException(code, reply)
        return {'status': 'accepted', 'errorCode': None}
    except smtplib.SMTPResponseException as exc:
        return _send_rejection(exc, stage, config)
    except Exception as exc:
        status = 'unknown' if submitting else (
            'retryable' if isinstance(exc, (OSError, smtplib.SMTPServerDisconnected)) else 'failed')
        return {'status': status, 'errorCode': _error(exc)}
    finally:
        if conn is not None:
            _close(conn)


def tencent_bounce(sender: str, subject: str, body: str) -> dict | None:
    """识别腾讯未提供标准 DSN 的退信；关联仍由 worker 核对账号和原邮件。"""
    local, _, domain = sender.lower().partition('@')
    if local != 'postmaster' or not domain or subject != f'来自{domain}的退信':
        return None
    plain = html_module.unescape(bleach.clean(body, tags=[], strip=True))
    plain = re.sub(r'\s+', ' ', plain)
    reason = re.search(r'退信原因(.*?)解决方案', plain)
    recipients = set(re.findall(r'无法发送到\s*([^\s<>@，；（）]+@[^\s<>@，；（）]+)', plain))
    if '很抱歉您发送的邮件被退回' not in plain or not reason or len(recipients) != 1:
        return None
    diagnostic = reason.group(1).strip()
    status = re.search(r'\b5\d\d[ -]+(5\.\d{1,3}\.\d{1,3})\b', diagnostic)
    if status:
        status = status.group(1)
    elif re.search(r'\b550 No such user\(TB\)', diagnostic) and '不存在' in diagnostic:
        status = '5.1.1'
    else:
        return None
    recipient = next(iter(recipients)).lower()
    tokens = set(re.findall(r'\[REACH:([0-9a-f]{32})\]', plain))
    return dict(bounce=True, permanent_bounce=True,
        original_message_id='',
        trace_message_id=f'<{next(iter(tokens))}@reach.local>' if len(tokens) == 1 else '',
        failed_recipients=[recipient], delivery_reports=[dict(recipient=recipient,
            action='failed', status=status, diagnostic=diagnostic)])


def _parse_message(raw: bytes, uid: int) -> dict:
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    parts, attachments = {'text': [], 'html': []}, []
    for part in msg.walk():
        if part.is_multipart():
            continue
        if part.get_content_disposition() == 'attachment' or part.get_filename():
            name = (part.get_filename() or 'attachment').replace('\\', '/').split('/')[-1]
            name = re.sub(r'[\x00-\x1f\x7f]', '', name)[:255] or 'attachment'
            attachments.append({'filename': name, 'content': part.get_payload(decode=True) or b'',
                                'content_type': part.get_content_type()})
        elif part.get_content_type() in ('text/plain', 'text/html'):
            body = (part.get_payload(decode=True) or b'').decode(
                part.get_content_charset() or 'utf-8', errors='replace')
            kind = 'html' if part.get_content_type() == 'text/html' else 'text'
            parts[kind].append(body)
    auto = str(msg.get('Auto-Submitted', '')).lower()
    permanent_bounce = any(
        str(block.get('Status', '')).startswith('5.')
        for part in msg.walk() if part.get_content_type() == 'message/delivery-status'
        for block in (part.get_payload() if isinstance(part.get_payload(), list) else [])
    )
    failed_recipients = []
    delivery_reports = []
    original_message_id = ''
    for part in msg.walk():
        if part.get_content_type() == 'message/delivery-status':
            for block in part.get_payload() if isinstance(part.get_payload(), list) else []:
                original_message_id = str(block.get('Original-Message-ID', original_message_id))
                recipient = str(block.get('Final-Recipient', '')).partition(';')[2].strip().lower()
                if recipient:
                    delivery_reports.append({'recipient': recipient,
                        'action': str(block.get('Action', '')).lower(),
                        'status': str(block.get('Status', ''))})
                    if block.get('Diagnostic-Code'):
                        delivery_reports[-1]['diagnostic'] = str(block['Diagnostic-Code'])
                if str(block.get('Status', '')).startswith('5.'):
                    recipient = str(block.get('Final-Recipient', '')).partition(';')[2].strip()
                    if recipient:
                        failed_recipients.append(recipient.lower())
        elif part.get_content_type() == 'message/rfc822' and isinstance(part.get_payload(), list):
            original_message_id = str(part.get_payload()[0].get('Message-ID', original_message_id))
        elif part.get_content_type() == 'text/rfc822-headers':
            headers = BytesParser(policy=policy.default).parsebytes(part.get_payload(decode=True) or b'')
            original_message_id = str(headers.get('Message-ID', original_message_id))
    result = {'uid': uid, 'message_id': str(msg.get('Message-ID', '')),
            'in_reply_to': str(msg.get('In-Reply-To', '')),
            'references': str(msg.get('References', '')),
            'from_email': next((a for _, a in getaddresses(msg.get_all('From', []))), ''),
            'to_emails': [a for _, a in getaddresses(msg.get_all('To', []) + msg.get_all('Cc', []))],
            'subject': str(msg.get('Subject', '')), 'date': str(msg.get('Date', '')),
            'html': sanitize_html('\n'.join(parts['html'])), 'text': '\n'.join(parts['text']),
            'auto_reply': bool(auto and auto != 'no') or bool(msg.get('X-Autoreply')),
            'bounce': any(p.get_content_type() == 'message/delivery-status' for p in msg.walk()),
            'permanent_bounce': permanent_bounce,
            'failed_recipients': failed_recipients, 'original_message_id': original_message_id,
            'delivery_reports': delivery_reports,
            'attachments': attachments}
    if not result['bounce']:
        report = tencent_bounce(result['from_email'], result['subject'], result['html'] or result['text'])
        if report:
            report['original_message_id'] = original_message_id
            result.update(report)
    return result


def sync_messages(config, *, uidvalidity=None, last_uid=0, limit=50) -> dict:
    conn = None
    try:
        conn = _connect(config, 'imap')
        status, _ = conn.select('INBOX', readonly=True)
        if status != 'OK':
            raise ValueError('IMAP_SELECT_FAILED')
        _, validity = conn.response('UIDVALIDITY')
        if not validity or not validity[0]:
            raise ValueError('IMAP_UIDVALIDITY_MISSING')
        current = validity[0].decode('ascii')
        reset = uidvalidity is not None and str(uidvalidity) != current
        cursor = 0 if reset else max(0, int(last_uid))
        status, values = conn.uid('search', None, 'UID', f'{cursor + 1}:*')
        if status != 'OK':
            raise ValueError('IMAP_SEARCH_FAILED')
        uids = sorted({int(u) for u in (values[0] or b'').split() if int(u) > cursor})
        messages = []
        for uid in uids[:max(1, min(int(limit), 100))]:
            status, data = conn.uid('fetch', str(uid), f'(BODY.PEEK[]<0.{MAX_INBOUND_MESSAGE_BYTES + 1}>)')
            if status != 'OK':
                raise ValueError('IMAP_FETCH_FAILED')
            raw = next((row[1] for row in data if isinstance(row, tuple)), None)
            if raw is None:
                raise ValueError('IMAP_FETCH_FAILED')
            if len(raw) > MAX_INBOUND_MESSAGE_BYTES:
                raise ValueError('IMAP_MESSAGE_TOO_LARGE')
            messages.append(_parse_message(raw, uid))
            cursor = uid
        return {'uidvalidity': current, 'uidvalidity_changed': reset,
                'drained': len(uids) <= max(1, min(int(limit), 100)),
                'last_uid': cursor, 'messages': messages}
    except (ValueError, UnicodeError, LookupError) as exc:
        if str(exc).startswith('IMAP_'):
            raise ValueError(str(exc)) from None
        raise ValueError('IMAP_MESSAGE_INVALID') from None
    except Exception:
        raise ValueError('IMAP_SYNC_FAILED') from None
    finally:
        if conn is not None:
            _close(conn)
