"""只从邮件专用配置读取加密密钥；不回退至平台或模型密钥。"""

import json
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken


def configured_secret(value: str, filename: str, label: str) -> str:
    if value and filename:
        raise ValueError(f"{label} 与文件引用不能同时配置")
    if filename:
        try:
            value = Path(filename).read_text().strip()
        except OSError as exc:
            raise ValueError(f"{label} 密钥文件无法读取") from exc
    return value.strip()


class CredentialCipher:
    def __init__(self, key: str):
        try:
            self.cipher = Fernet(key.encode())
        except (ValueError, TypeError) as exc:
            raise ValueError("邮件凭据加密密钥未配置或格式错误") from exc

    def encrypt(self, data: dict) -> str:
        return self.cipher.encrypt(json.dumps(data).encode()).decode()

    def decrypt(self, value: str) -> dict:
        try:
            return json.loads(self.cipher.decrypt(value.encode()))
        except (InvalidToken, ValueError, TypeError) as exc:
            raise ValueError("邮件凭据无法解密，请检查加密密钥") from exc
