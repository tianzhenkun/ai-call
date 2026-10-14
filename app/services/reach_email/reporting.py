"""按原邮件关联回复和 DSN，统计有证据的送达下限。"""

import json
import re
from collections import defaultdict


def canonical_reply_reference(subject, references, *, headerless):
    """只恢复与唯一主题标记一致的编号，保留原始信头。"""
    tokens = set(re.findall(r'\[REACH:([0-9a-f]{32})\]', subject))
    rewritten = {match[1] for reference in references
        if (match := re.fullmatch(r'<[^<>\s@+]+\+([0-9a-f]{32})@reach\.local>', reference))}
    if len(tokens) == 1 and (headerless or tokens <= rewritten):
        return f'<{next(iter(tokens))}@reach.local>'
    return None


def reply_parents(messages):
    """在已按租户和用户隔离的会话中，为详情和送达统计解析同一父邮件。"""
    headers = defaultdict(list)
    for row in messages:
        if row.message_id:
            headers[(row.account_id, row.message_id)].append(row)
    parents = {}
    for row in messages:
        # References 只取最近父邮件，不能把更早的线程邮件当作直接回复对象。
        refs = re.findall(r'<[^<>\s]+>', row.in_reply_to or '')
        if not refs:
            refs = re.findall(r'<[^<>\s]+>', row.references or '')[-1:]
        marker = None
        if row.direction == 'inbound' and row.kind in ('reply', 'auto_reply'):
            marker = canonical_reply_reference(row.subject, refs,
                headerless=not row.in_reply_to and not row.references)
            if marker:
                refs.append(marker)
        candidates = [parent for ref in set(refs) for parent in headers[(row.account_id, ref)]
            if row.lead_id and parent.lead_id == row.lead_id and parent.task_id == row.task_id
            and (not marker or (parent.direction == 'outbound' and parent.status == 'accepted'
                and parent.from_email.lower() == row.to_email.lower()
                and parent.to_email.lower() == row.from_email.lower()))]
        if len(candidates) == 1:
            parents[row.id] = candidates[0]
    return parents


def delivery_evidence(messages):
    outbound = [m for m in messages if m.direction == "outbound" and m.attempt_count > 0]
    headers = defaultdict(list)
    for row in outbound:
        if row.message_id:
            headers[(row.account_id, row.message_id)].append(row)
    parents = reply_parents(messages)
    delivered, failed, replied = set(), set(), set()
    for incoming in messages:
        if incoming.direction != "inbound":
            continue
        report = json.loads(incoming.delivery_report) if incoming.delivery_report else None
        if report:
            candidates = headers[(incoming.account_id, report["messageId"])]
            if len(candidates) != 1:
                continue
            source = candidates[0]
            for recipient in report["recipients"]:
                if recipient["recipient"].lower() != source.to_email.lower():
                    continue
                if recipient["action"] == "delivered" and recipient["status"].startswith("2."):
                    delivered.add(source.id)
                elif recipient["action"] == "failed" and recipient["status"].startswith("5."):
                    failed.add(source.id)
            continue
        if incoming.kind not in ("reply", "auto_reply") or not incoming.lead_id:
            continue
        parent = parents.get(incoming.id)
        if (parent and parent.direction == 'outbound' and parent.attempt_count > 0
                and parent.to_email.lower() == incoming.from_email.lower()
                and parent.from_email.lower() == incoming.to_email.lower()):
            delivered.add(parent.id)
        # 自动回复可以证明收件，但不代表客户实际回复。
        if incoming.kind == "reply":
            replied.add(incoming.id)
    failed.update(m.id for m in outbound if m.status == "failed")
    return delivered - failed, failed, replied


def summarize(messages, sender_email=None):
    outbound = [m for m in messages if m.direction == "outbound" and m.attempt_count > 0]
    delivered, failed, replied = delivery_evidence(messages)
    details = []
    if sender_email is not None:
        outbound = [m for m in outbound if m.from_email == sender_email]
    addresses = {m.to_email if sender_email is not None else m.from_email for m in outbound}
    for sender in sorted(addresses):
        rows = [m for m in outbound if (m.to_email if sender_email is not None else m.from_email) == sender]
        ids = {m.id for m in rows}
        failures = len({m.id for m in rows if m.status == "failed"} | (ids & failed))
        # 相互矛盾的结果不作为成功证据。
        confirmed = len({m.id for m in rows if m.id in delivered - failed and m.status != "failed"})
        details.append({("recipientEmail" if sender_email is not None else "senderEmail"): sender, "sentCount": len(rows),
            "deliveredCount": confirmed, "failedCount": failures,
            "unconfirmedCount": len(rows) - confirmed - failures,
            "repliedCount": sum(m.id in replied and m.to_email.lower() == (sender_email or sender).lower()
                and (sender_email is None or m.from_email.lower() == sender.lower()) for m in messages)})
    if sender_email is not None:
        return details
    return {**{key: sum(row[key] for row in details) for key in (
        "sentCount", "deliveredCount", "failedCount", "unconfirmedCount", "repliedCount")},
        "senderEmails": [row["senderEmail"] for row in details], "senders": details}
