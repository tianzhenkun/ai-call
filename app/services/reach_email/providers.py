"""REACH 的 SMTP / IMAP 邮箱预设。"""

import json


def mailbox_service(config):
    """服务身份取实际 SMTP 主机；切换预设名称、端口或凭据不会产生新邮箱。"""
    return config.get('smtp_host', '').strip().lower().rstrip('.')


def mailbox_label(config):
    host = mailbox_service(config)
    if host == 'smtp.sina.cn':
        return PROVIDERS['sina']['label']
    return next((preset['label'] for preset in PROVIDERS.values()
                 if preset['smtpHost'] == host), f'自定义邮箱 · {host}' if host else '自定义邮箱')


PROVIDERS = {
    key: {
        "label": label,
        "smtpHost": f"smtp.{domain}",
        "smtpPort": 465,
        "smtpSecurity": "SSL_TLS",
        "imapHost": f"imap.{domain}",
        "imapPort": 993,
        "imapSecurity": "SSL_TLS",
    }
    for key, label, domain in [
        ("qq_personal", "QQ邮箱", "qq.com"),
        ("tencent_exmail", "腾讯企业邮箱", "exmail.qq.com"),
        ("aliyun_enterprise", "阿里企业邮箱", "qiye.aliyun.com"),
        ("dingtalk_enterprise", "钉钉企业邮箱", "em.dingtalk.com"),
        ("feishu_enterprise", "飞书邮箱", "feishu.cn"),
        ("netease_enterprise", "网易企业邮箱", "qiye.163.com"),
        ("netease_163", "163邮箱", "163.com"),
        ("netease_126", "126邮箱", "126.com"),
        ("netease_yeah", "yeah.net邮箱", "yeah.net"),
        ("mail_139", "139邮箱", "139.com"),
        ("mail_189", "189邮箱", "189.cn"),
        ("sina", "新浪邮箱", "sina.com"),
        ("gmail", "Gmail", "gmail.com"),
        ("icloud", "iCloud邮箱", "mail.me.com"),
        ("yahoo", "Yahoo邮箱", "mail.yahoo.com"),
    ]
}
PROVIDERS["icloud"].update(smtpPort=587, smtpSecurity="STARTTLS")
PROVIDERS["outlook_personal"] = {
    "label": "Outlook个人邮箱",
    "smtpHost": "smtp-mail.outlook.com", "smtpPort": 587, "smtpSecurity": "STARTTLS",
    "imapHost": "outlook.office365.com", "imapPort": 993, "imapSecurity": "SSL_TLS",
    "authType": "oauth2",
    # 只排除明确由其他服务商托管的个人邮箱，避免误拦微软地区域名和历史自定义域名。
    "unsupportedEmailDomains": [
        "qq.com", "foxmail.com", "163.com", "126.com", "yeah.net", "139.com", "189.cn",
        "sina.com", "sina.cn", "gmail.com", "googlemail.com", "icloud.com", "me.com", "mac.com",
        "yahoo.com", "dingtalk.com",
    ],
}

_SENDING_POLICIES = {
    "mail_139": (
        "官方公布单账号每天最多250封、收件人最多450人。REACH额外按滚动24小时不超过250次发送控制；服务商日窗口及外部客户端发送无法由平台完整统计。",
        "https://help.mail.10086.cn/statichtml/7/Content/1234600.html", 250),
    "dingtalk_enterprise": (
        "官方公布个人6000封/天，对外收件人数及企业总量依版本另有限制；另有不公开的频率风控。公开额度不保证账号一定能发满。",
        "https://alidocs.dingtalk.com/i/p/nb9XJKaNwORJ3myA/docs/G1DKw2zgV2KnvL4kFqbznBOqJB5r9YAn", None),
    "qq_personal": (
        "官方说明单封收件人数受反垃圾策略影响；固定SMTP小时、每日额度尚未确认。",
        "https://service.mail.qq.com/detail/0/496", None),
    "tencent_exmail": (
        "公开发信额度涉及企业版本及账号数量，不能作为每个邮箱的独立额度；请以企业后台及实际拒绝反馈为准。",
        "https://exmail.qq.com/cgi-bin/loginpage?t=new_index/versionCmp", None),
    "aliyun_enterprise": (
        "免费版企业总量与付费版个人额度的统计范围不同；另有不公开的分钟、小时及每日风控阈值。",
        "https://help.aliyun.com/zh/document_detail/36644.html", None),
    "feishu_enterprise": (
        "发信总数、外部收件人数及企业总量依版本和认证状态不同，不能仅按邮箱类型确定账号额度。",
        "https://www.feishu.cn/hc/zh-CN/articles/329462350714", None),
    "gmail": (
        "个人Gmail与Google Workspace额度不同，并有收件人数及反滥用限制；当前配置不代表Google保证的可用额度。",
        "https://support.google.com/mail/answer/22839?hl=zh-Hans", None),
    "icloud": (
        "官方公布每天最多1000封及1000名收件人；仍受反滥用限制，平台计数不包括其他客户端发送。",
        "https://support.apple.com/en-ca/102198", None),
    "yahoo": (
        "官方不披露统一发信数量限制；应按实际拒绝提示暂停，不能从连接测试推断额度。",
        "https://uk.help.yahoo.com/kb/new-yahoo-mail/limits-sending-email-yahoo-mail-sln3353.html", None),
}
for _key, _preset in PROVIDERS.items():
    _summary, _url, _cap = _SENDING_POLICIES.get(_key, (
        "当前适用的发信小时、每日具体额度尚未确认。以下为REACH发送控制，不代表服务商允许额度。", None, None))
    _preset['sendingPolicy'] = {'summary': _summary, 'sourceUrl': _url, 'checkedAt': '2026-10-28'}
    if _cap is not None:
        _preset['sendingPolicy']['rollingDailyCap'] = _cap


def published_daily_cap(account):
    config = json.loads(account.config)
    # 按实际SMTP主机识别，旧账号及使用自定义入口的同一服务也遵守相同控制。
    if config.get('smtp_host', '').lower().rstrip('.') == 'smtp.139.com':
        return 250
    return None


def provider_settings(provider: str, email: str):
    preset = PROVIDERS[provider]
    if provider == "sina" and email.strip().rsplit("@", 1)[-1].lower() == "sina.cn":
        return {**preset, "smtpHost": "smtp.sina.cn", "imapHost": "imap.sina.cn"}
    return preset
