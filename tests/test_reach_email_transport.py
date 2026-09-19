import smtplib
from unittest.mock import Mock, patch

import pytest

from app.services.reach_email import transport


def test_imap_declares_identity_before_opening_inbox():
    conn = Mock()
    conn.capabilities = ('IMAP4REV1', 'ID')
    conn.xatom.return_value = ('OK', [])
    conn.select.return_value = ('OK', [])
    config = {'imap_host': 'imap.example.com', 'imap_port': 993,
              'imap_security': 'SSL_TLS', 'imap_username': 'test', 'imap_password': 'secret'}
    with patch.object(transport, '_IMAPSSL', return_value=conn):
        result = transport.test_account(config, ('imap',))
    assert result['imap']['status'] == 'ok'
    conn.xatom.assert_called_once_with('ID', '("name" "REACH" "version" "1.0")')
    conn.select.assert_called_once_with('INBOX', readonly=True)


def test_imap_probe_rejects_inaccessible_inbox():
    conn = Mock()
    conn.select.return_value = ('NO', [b'private server response'])
    with patch.object(transport, '_connect', return_value=conn):
        result = transport.test_account({}, ('imap',))
    assert result['imap']['status'] == 'failed'
    assert 'private' not in str(result)


def test_dns_rebinding_and_mixed_addresses_blocked():
    with patch.object(transport.socket, 'getaddrinfo', return_value=[
        (2, 1, 6, '', ('8.8.8.8', 465)), (2, 1, 6, '', ('127.0.0.1', 465))]):
        with pytest.raises(ValueError, match='UNSAFE_ENDPOINT'):
            transport.validate_endpoint('example.test', 465)
    with patch.object(transport, 'validate_endpoint', return_value=['8.8.8.8']), \
         patch.object(transport.socket, 'create_connection') as connect:
        transport._socket('example.test', 465, 20)
        connect.assert_called_once_with(('8.8.8.8', 465), 20)


@pytest.mark.parametrize('error,expected', [
    (smtplib.SMTPServerDisconnected('secret'), 'unknown'),
    (smtplib.SMTPDataError(451, b'secret'), 'retryable'),
    (smtplib.SMTPDataError(550, b'secret'), 'failed'),
    (None, 'accepted'),
])
def test_smtp_outcomes_and_mime(error, expected):
    smtp = Mock()
    smtp.has_extn.return_value = False
    smtp.mail.return_value = smtp.rcpt.return_value = smtp.data.return_value = (250, b'ok')
    smtp.data.side_effect = error
    with patch.object(transport, '_connect', return_value=smtp):
        result = transport.send_message({'smtp_username': 'sender@example.com'},
            recipient='recipient@example.com', subject='你好', html='<p>Hello</p><img src="https://tracker">',
            attachments=[{'filename': 'test.pdf', 'content': b'%PDF', 'content_type': 'application/pdf'}],
            message_id='<stable@example.com>', in_reply_to='<first@example.com>')
    assert result['status'] == expected
    assert 'secret' not in str(result)
    wire = smtp.data.call_args.args[0]
    assert b'In-Reply-To: <first@example.com>' in wire
    assert b'tracker' not in wire
    assert b'filename="test.pdf"' in wire


def test_imap_uid_reset_cursor_and_html():
    conn = Mock()
    conn.select.return_value = ('OK', [])
    conn.response.return_value = ('UIDVALIDITY', [b'new'])
    raw = (b'From: a@example.com\r\nTo: b@example.com\r\nMessage-ID: <one>\r\n'
           b'Auto-Submitted: auto-replied\r\nContent-Type: text/html\r\n\r\n'
           b'<p>Hello</p><img src="https://tracker"><a href="javascript:evil">link</a>')
    conn.uid.side_effect = [('OK', [b'1 2']), ('OK', [(b'1', raw), b')'])]
    with patch.object(transport, '_connect', return_value=conn):
        result = transport.sync_messages({}, uidvalidity='old', last_uid=200, limit=1)
    assert result['uidvalidity_changed'] and result['last_uid'] == 1
    assert result['messages'][0]['auto_reply']
    assert 'javascript' not in result['messages'][0]['html']
    assert 'tracker' not in result['messages'][0]['html']
    conn.uid.assert_any_call('search', None, 'UID', '1:*')


def test_connection_errors_do_not_expose_password():
    with patch.object(transport, '_connect', side_effect=RuntimeError('password=secret')):
        assert 'secret' not in str(transport.test_account({}))


def test_sender_display_name_keeps_envelope_address():
    from email import policy
    from email.parser import BytesParser

    smtp = Mock()
    smtp.has_extn.return_value = False
    smtp.mail.return_value = smtp.rcpt.return_value = smtp.data.return_value = (250, b'ok')
    with patch.object(transport, '_connect', return_value=smtp):
        result = transport.send_message({'email': 'sender@example.com', 'from_name': '测试发件人'},
            recipient='recipient@example.com', subject='test', html='test', attachments=[],
            message_id='<test@example.com>')
    assert result['status'] == 'accepted'
    smtp.mail.assert_called_once_with('sender@example.com')
    message = BytesParser(policy=policy.default).parsebytes(smtp.data.call_args.args[0])
    assert message['From'].addresses[0].display_name == '测试发件人'


def test_outgoing_subject_carries_stable_thread_marker():
    from email import policy
    from email.parser import BytesParser

    smtp = Mock()
    smtp.has_extn.return_value = False
    smtp.mail.return_value = smtp.rcpt.return_value = smtp.data.return_value = (250, b'ok')
    first = '<0123456789abcdef0123456789abcdef@reach.local>'
    with patch.object(transport, '_connect', return_value=smtp):
        for message_id, parent in [(first, None), ('<fedcba9876543210fedcba9876543210@reach.local>', first)]:
            result = transport.send_message({'email': 'sender@example.com'},
                recipient='recipient@example.com', subject='合作沟通', html='test', attachments=[],
                message_id=message_id, in_reply_to=parent)
            assert result['status'] == 'accepted'
            message = BytesParser(policy=policy.default).parsebytes(smtp.data.call_args.args[0])
            assert str(message['Subject']) == '合作沟通 [REACH:0123456789abcdef0123456789abcdef]'
            assert str(message['Message-ID']) == message_id


def test_pre_submission_disconnect_is_retryable():
    smtp = Mock()
    smtp.has_extn.return_value = False
    smtp.mail.side_effect = smtplib.SMTPServerDisconnected('private server response')
    with patch.object(transport, '_connect', return_value=smtp):
        result = transport.send_message({'email': 'sender@example.com'},
            recipient='recipient@example.com', subject='Hello', html='Hello',
            attachments=[], message_id='<stable@example.com>')
    assert result == {'status': 'retryable', 'errorCode': 'CONNECTION_FAILED'}
    smtp.data.assert_not_called()


def test_imap_failure_hides_server_response():
    with patch.object(transport, '_connect', side_effect=RuntimeError('password=secret')):
        with pytest.raises(ValueError, match='^IMAP_SYNC_FAILED$'):
            transport.sync_messages({})


def test_tls_uses_original_hostname_after_ip_pinning():
    with patch.object(transport, '_socket') as connect:
        client = object.__new__(transport._SMTPSSL)
        client.context = Mock()
        client._get_socket('smtp.example.com', 465, 20)
        client.context.wrap_socket.assert_called_once_with(
            connect.return_value, server_hostname='smtp.example.com')


@pytest.mark.parametrize('action,status,permanent', [('failed', '5.1.1', True), ('delayed', '4.2.0', False), ('delivered', '2.0.0', False)])
def test_dsn_permanent_status_and_original_evidence(action, status, permanent):
    raw = (f'Content-Type: multipart/report; boundary="dsn"; report-type=delivery-status\r\n\r\n'
        f'--dsn\r\nContent-Type: text/plain\r\n\r\nDelivery report\r\n'
        f'--dsn\r\nContent-Type: message/delivery-status\r\n\r\n'
        f'Reporting-MTA: dns; mail.example.com\r\nOriginal-Message-ID: <original>\r\n\r\n'
        f'Final-Recipient: rfc822; lead@example.com\r\nAction: {action}\r\nStatus: {status}\r\n\r\n'
        f'--dsn--\r\n').encode()
    parsed = transport._parse_message(raw, 1)
    assert parsed['bounce'] and parsed['permanent_bounce'] is permanent
    assert parsed['original_message_id'] == '<original>'
    assert parsed['failed_recipients'] == (['lead@example.com'] if permanent else [])
    assert parsed['delivery_reports'] == [{'recipient': 'lead@example.com', 'action': action, 'status': status}]


def test_requests_delivery_notifications_only_with_server_support():
    smtp = Mock()
    smtp.has_extn.return_value = True
    smtp.mail.return_value = smtp.rcpt.return_value = smtp.data.return_value = (250, b'ok')
    with patch.object(transport, '_connect', return_value=smtp):
        assert transport.send_message({'email': 'sender@example.com'}, recipient='lead@example.com',
            subject='test', html='test', attachments=[], message_id='<test>')['status'] == 'accepted'
    smtp.mail.assert_called_once_with('sender@example.com', options=['RET=HDRS'])
    smtp.rcpt.assert_called_once_with('lead@example.com', options=['NOTIFY=SUCCESS,FAILURE,DELAY'])
