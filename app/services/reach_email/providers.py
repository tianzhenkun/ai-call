"""与 Sales 授权界面一致的邮箱预设，由 REACH 独立提供。"""

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
        ("netease_163", "163邮箱", "163.com"),
        ("netease_126", "126邮箱", "126.com"),
        ("gmail", "Gmail", "gmail.com"),
    ]
}
