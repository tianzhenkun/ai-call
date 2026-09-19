"""按原邮件关联回复和 DSN，统计有证据的送达下限。"""

import json
import re
from collections import defaultdict


def delivery_evidence(messages):
    outbound = [m for m in messages if m.direction == "outbound" and m.attempt_count > 0]
    headers = defaultdict(list)
    for row in outbound:
        if row.message_id:
            headers[(row.account_id, row.message_id)].append(row)
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
        # References 的最后一项是最近的父邮件，不把整个线程都算成收到。
        refs = re.findall(r"<[^<>\s]+>", incoming.in_reply_to or "")
        if not refs:
            refs = re.findall(r"<[^<>\s]+>", incoming.references or "")[-1:]
        if not refs and not incoming.in_reply_to and not incoming.references:
            tokens = set(re.findall(r"\[REACH:([0-9a-f]{32})\]", incoming.subject))
            if len(tokens) == 1:
                refs = [f"<{next(iter(tokens))}@reach.local>"]
        candidates = [m for ref in set(refs) for m in headers[(incoming.account_id, ref)]
            if m.lead_id == incoming.lead_id and m.task_id == incoming.task_id
            and m.to_email.lower() == incoming.from_email.lower()
            and m.from_email.lower() == incoming.to_email.lower()]
        if len(candidates) == 1:
            delivered.add(candidates[0].id)
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
