from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator
from pydantic.alias_generators import to_camel

from app.services.reach_email.content import clean_html, email_address


class Input(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid")


class ReadInput(Input):
    last_reply_at: datetime


class AccountInput(Input):
    name: str = Field(default="", max_length=100)
    provider_type: str = "custom"
    from_name: str = Field(default="", max_length=100)
    email: str
    smtp_host: str = Field(min_length=1, max_length=253)
    smtp_port: int = Field(default=465, ge=1, le=65535)
    smtp_username: str = Field(min_length=1, max_length=254)
    smtp_password: str = Field(default="", max_length=4096)
    smtp_security: Literal["SSL_TLS", "STARTTLS"] = "SSL_TLS"
    imap_host: str = Field(min_length=1, max_length=253)
    imap_port: int = Field(default=993, ge=1, le=65535)
    imap_username: str = Field(min_length=1, max_length=254)
    imap_password: str = Field(default="", max_length=4096)
    imap_security: Literal["SSL_TLS", "STARTTLS"] = "SSL_TLS"
    weight: int = Field(default=1, ge=1, le=100)
    enabled: bool = True
    hourly_limit: int = Field(default=50, ge=1, le=200)
    daily_limit: int = Field(default=100, ge=1, le=500)
    interval_seconds: int = Field(default=60, ge=10, le=3600)

    _email = field_validator("email")(email_address)

    @model_validator(mode="before")
    @classmethod
    def apply_provider(cls, data):
        from app.services.reach_email.providers import PROVIDERS

        values = dict(data)
        provider = values.pop("provider_type", values.get("providerType", "custom"))
        values["providerType"] = provider
        if provider != "custom":
            if provider not in PROVIDERS:
                raise ValueError("邮箱类型不受支持，请选择已有类型或自定义邮箱")
            for key, value in PROVIDERS[provider].items():
                if key != "label":
                    # 同时支持 Python 字段名与 HTTP 字段名，固定预设不可被覆盖。
                    snake = "".join("_" + c.lower() if c.isupper() else c for c in key)
                    values.pop(snake, None)
                    values[key] = value
            values.pop("smtp_username", None)
            values.pop("imap_username", None)
            values["smtpUsername"] = values["imapUsername"] = str(values.get("email", "")).strip()
            password = values.get("smtpPassword", values.get("smtp_password", ""))
            values.pop("imap_password", None)
            values["imapPassword"] = password
        return values

    @field_validator("from_name")
    @classmethod
    def safe_from_name(cls, value):
        if "\r" in value or "\n" in value:
            raise ValueError("发件人名称不能包含换行")
        return value.strip()


class TaskSettings(Input):
    company_name: str = Field(default="", max_length=255)
    company_website: str = Field(default="", max_length=1024)
    company_description: str = Field(default="", max_length=12000)
    follow_up_enabled: bool = False
    follow_up_interval_days: int = Field(default=2, ge=1, le=365)
    follow_up_count: int = Field(default=2, ge=0, le=5)
    daily_limit: int = Field(default=50, ge=1, le=100000)

    @model_validator(mode="after")
    def require_company_details(self):
        for field, label in (
            ("company_name", "公司名称"),
            ("company_website", "公司官网"),
            ("company_description", "公司简介"),
        ):
            value = getattr(self, field).strip()
            if not value:
                raise ValueError(f"请完善邮件设置：请输入{label}")
            setattr(self, field, value)
        try:
            if not self.company_website.lower().startswith(("http://", "https://")):
                raise ValueError("缺少完整协议前缀")
            HttpUrl(self.company_website)
        except ValueError:
            raise ValueError("请完善邮件设置：公司官网须填写完整的 http 或 https 地址") from None
        return self


class Content(Input):
    subject: str = Field(default="", max_length=512)
    html: str = Field(default="", max_length=100000)
    signature: str = Field(default="", max_length=20000)
    signature_name: str = Field(default="", max_length=255)
    attachment_ids: list[str] = Field(default_factory=list, max_length=5)

    @field_validator("subject")
    @classmethod
    def subject_safe(cls, value):
        if "\r" in value or "\n" in value:
            raise ValueError("主题不能包含换行")
        return value

    _html = field_validator("html", "signature")(clean_html)


class TaskInput(Input):
    name: str = Field(min_length=1, max_length=120)
    import_id: str
    settings: TaskSettings
    content: Content = Field(default_factory=Content)
    version: int | None = None


class VersionInput(Input):
    version: int = Field(ge=1)


class ContentInput(Content):
    version: int = Field(ge=1)


class StartInput(VersionInput):
    request_id: str = Field(min_length=1, max_length=128)
    scheduled_at: datetime | None = None


class RestoreInput(VersionInput):
    version_id: str


class AIInput(VersionInput):
    action: Literal["generate", "modify", "translate"]
    subject: str = Field(default="", max_length=512)
    content: str = Field(default="", max_length=100000)
    instruction: str = Field(default="", max_length=5000)


class ReplyAIInput(Input):
    action: Literal["generate", "translate"]
    subject: str = Field(default="", max_length=512)
    content: str = Field(default="", max_length=100000)
    instruction: str = Field(default="", max_length=5000)
    inbound_id: str | None = None


class ReplyInput(Input):
    subject: str = Field(min_length=1, max_length=512)
    html: str = Field(min_length=1, max_length=100000)
    attachment_ids: list[str] = Field(default_factory=list, max_length=5)
    inbound_id: str | None = None
    request_id: str = Field(min_length=1, max_length=128)
    _html = field_validator("html")(clean_html)
    _subject = field_validator("subject")(Content.subject_safe.__func__)


class ClassificationInput(Input):
    classification: Literal["following", "interested", "low_value"]


class ResolveInput(Input):
    outcome: Literal["accepted", "failed"]
    note: str = Field(min_length=5, max_length=1000)

    @field_validator("note", mode="before")
    @classmethod
    def trim_note(cls, value):
        return value.strip() if isinstance(value, str) else value


class SuppressInput(Input):
    reason: str = Field(default="manual", min_length=1, max_length=40)
