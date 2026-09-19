import dns.resolver
import dns.rrset
import pytest


@pytest.fixture
def task_settings():
    return {
        "companyName": "测试公司",
        "companyWebsite": "https://example.com",
        "companyDescription": "提供产品演示服务",
    }


@pytest.fixture
def valid_email_dns(monkeypatch):
    """业务生命周期测试隔离外部 DNS；域名失败路径由导入专项测试覆盖。"""
    monkeypatch.setattr(
        dns.resolver.Resolver,
        "resolve",
        lambda self, domain, kind, **kwargs: dns.rrset.from_text(
            domain, 60, "IN", "MX", "10 mx.example.com."
        ),
    )
