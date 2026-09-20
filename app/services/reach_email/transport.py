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
    return bleach.clean(value, tags=['p', 'br', 'div', 'span', 'b', 'strong', 'i', 'em',
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
            pass
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
    conn = None
    try:
        if protocol == 'smtp':
            conn = (_SMTPSSL(host, port, timeout=TIMEOUT, context=context)
                    if security == 'SSL_TLS' else _SMTP(host, port, timeout=TIMEOUT))
            conn.ehlo()
            if security == 'STARTTLS':
                conn.starttls(context=context)
                conn.ehlo()
        else:
            conn = (_IMAPSSL(host, port, timeout=TIMEOUT, ssl_context=context)
                    if security == 'SSL_TLS' else _IMAP(host, port, timeout=TIMEOUT))
            if security == 'STARTTLS':
                conn.starttls(ssl_context=context)
        conn.login(config[f'{protocol}_username'], config[f'{protocol}_password'])
        # 网易等服务商要求声明客户端身份后才允许访问收件箱。
        if protocol == 'imap' and 'ID' in conn.capabilities:
            status, _ = conn.xatom('ID', '("name" "REACH" "version" "1.0")')
            if status != 'OK':
                raise ValueError('IMAP_ID_FAILED')
        return conn
    except Exception:
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


def test_account(config, protocols=('smtp', 'imap')) -> dict:
    result = {}
    for protocol in protocols:
        if protocol not in ('smtp', 'imap'):
            raise ValueError('INVALID_PROTOCOL')
        conn = None
        try:
            conn = _connect(config, protocol)
            if protocol == 'imap' and conn.select('INBOX', readonly=True)[0] != 'OK':
                raise ValueError('IMAP_SELECT_FAILED')
            result[protocol] = {'status': 'ok', 'errorCode': None}
        except Exception as exc:
            result[protocol] = {'status': 'failed', 'errorCode': _error(exc)}
        finally:
            if conn is not None:
                _close(conn)
    return result


def send_message(config, *, recipient, subject, html, attachments, message_id,
                 in_reply_to=None, references=None) -> dict:
    conn = None
    submitting = False
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
        conn = _connect(config, 'smtp')
        dsn = conn.has_extn('dsn')
        code, _ = conn.mail(sender, options=['RET=HDRS']) if dsn else conn.mail(sender)
        if code != 250:
            raise smtplib.SMTPResponseException(code, b'')
        code, _ = conn.rcpt(recipient, options=['NOTIFY=SUCCESS,FAILURE,DELAY']) if dsn else conn.rcpt(recipient)
        if code not in (250, 251):
            raise smtplib.SMTPResponseException(code, b'')
        submitting = True
        code, _ = conn.data(wire)
        if code != 250:
            raise smtplib.SMTPResponseException(code, b'')
        return {'status': 'accepted', 'errorCode': None}
    except smtplib.SMTPResponseException as exc:
        return {'status': 'retryable' if 400 <= exc.smtp_code < 500 else 'failed',
                'errorCode': f'SMTP_{exc.smtp_code}'}
    except Exception as exc:
        status = 'unknown' if submitting else (
            'retryable' if isinstance(exc, (OSError, smtplib.SMTPServerDisconnected)) else 'failed')
        return {'status': status, 'errorCode': _error(exc)}
    finally:
        if conn is not None:
            _close(conn)


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
                if str(block.get('Status', '')).startswith('5.'):
                    recipient = str(block.get('Final-Recipient', '')).partition(';')[2].strip()
                    if recipient:
                        failed_recipients.append(recipient.lower())
        elif part.get_content_type() == 'message/rfc822' and isinstance(part.get_payload(), list):
            original_message_id = str(part.get_payload()[0].get('Message-ID', original_message_id))
        elif part.get_content_type() == 'text/rfc822-headers':
            headers = BytesParser(policy=policy.default).parsebytes(part.get_payload(decode=True) or b'')
            original_message_id = str(headers.get('Message-ID', original_message_id))
    return {'uid': uid, 'message_id': str(msg.get('Message-ID', '')),
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
