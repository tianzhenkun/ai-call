from __future__ import annotations

import json
import os
import shutil
import socket
import ssl
import struct
import subprocess
import textwrap
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

DEPLOY = Path(__file__).resolve().parents[1] / "deploy/ai-call-118"


@pytest.fixture
def turn_deployment(tmp_path, monkeypatch):
    deploy = tmp_path / "deploy"
    for directory in ("config", "scripts"):
        shutil.copytree(DEPLOY / directory, deploy / directory)
    shutil.copy(DEPLOY / "compose.yml", deploy / "compose.yml")
    (deploy / ".env").write_text(
        "LIVEKIT_API_KEY=test-key\n"
        "LIVEKIT_API_SECRET=test-secret-only-not-a-production-key\n"
        "REDIS_PASSWORD=test-redis-only\n"
        "LIVEKIT_TURN_DOMAIN=turn.example.test\n",
        encoding="utf-8",
    )
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Isolated TURN test CA")])
    ca = (
        x509
        .CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=2))
        .not_valid_after(now + timedelta(days=60))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    ca_file = tmp_path / "test-ca.pem"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    monkeypatch.setenv("SSL_CERT_FILE", str(ca_file))
    cert_dir = deploy / "runtime/turn"
    cert_dir.mkdir(parents=True)

    def issue_certificate(*, domain="turn.example.test", days=30):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cert = (
            x509
            .CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, domain)]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=2))
            .not_valid_after(now + timedelta(days=days))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(domain)]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256())
        )
        (cert_dir / "fullchain.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (cert_dir / "privkey.pem").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        (cert_dir / "privkey.pem").chmod(0o600)

    issue_certificate()
    return deploy, issue_certificate


def render(deploy: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", str(deploy / "scripts/render-configs.sh"), *args],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )


def rendered_compose(deploy: Path) -> dict:
    result = render(deploy)
    assert result.returncode == 0, result.stderr + result.stdout
    # 复用 Compose 的 YAML 解析器，一次校验部署模型及渲染结果。
    compose_result = subprocess.run(
        ["docker", "compose", "--env-file", ".env", "-f", "-", "config", "--format", "json"],
        input=(deploy / "compose.yml").read_text()
        + "\nx-rendered-livekit:\n"
        + textwrap.indent((deploy / "runtime/livekit.yaml").read_text(), "  "),
        cwd=deploy,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert compose_result.returncode == 0, compose_result.stderr
    return json.loads(compose_result.stdout)


def test_rendered_turn_keeps_direct_media_and_publishes_tls_443(turn_deployment):
    deploy, _ = turn_deployment
    compose = rendered_compose(deploy)
    config = compose["x-rendered-livekit"]
    assert config["turn"] == {
        "enabled": True,
        "domain": "turn.example.test",
        "tls_port": 443,
        "udp_port": 0,
        "external_tls": False,
        "cert_file": "/etc/livekit/turn/fullchain.pem",
        "key_file": "/etc/livekit/turn/privkey.pem",
    }
    assert config["rtc"]["tcp_port"] == 7881
    assert config["rtc"]["port_range_start"] == 50000
    assert config["rtc"]["port_range_end"] == 50100
    livekit = compose["services"]["livekit"]
    assert any(
        port["target"] == 443 and port["published"] == "443" and port["protocol"] == "tcp"
        for port in livekit["ports"]
    )
    assert any(
        volume["source"] == str(deploy / "runtime/turn")
        and volume["target"] == "/etc/livekit/turn"
        and volume["read_only"]
        for volume in livekit["volumes"]
    )
    assert (deploy / "runtime/livekit.yaml").stat().st_mode & 0o777 == 0o600
    assert (deploy / "runtime/egress.yaml").stat().st_mode & 0o777 == 0o640


def test_livekit_turn_tls_starts_with_verified_certificate_and_answers_stun(turn_deployment):
    deploy, _ = turn_deployment
    compose = rendered_compose(deploy)
    image = compose["services"]["livekit"]["image"]
    subprocess.run(["docker", "image", "inspect", image], capture_output=True, check=True)
    config = compose["x-rendered-livekit"]
    # 仅隔离外部依赖；TURN 部分原样交给实际部署版本解析和启动。
    config.pop("redis")
    config.pop("webhook")
    config["rtc"]["use_external_ip"] = False
    config_path = deploy / "runtime/livekit-test.json"
    config_path.write_text(json.dumps(config))
    container = f"ai-call-turn-test-{uuid4().hex}"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container,
            "--publish",
            "127.0.0.1::443",
            "--volume",
            f"{config_path}:/etc/livekit.yaml:ro",
            "--volume",
            f"{deploy / 'runtime/turn'}:/etc/livekit/turn:ro",
            image,
            "--config",
            "/etc/livekit.yaml",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        endpoint = subprocess.run(
            ["docker", "port", container, "443/tcp"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        port = int(endpoint.rsplit(":", 1)[1])
        context = ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
        deadline = time.monotonic() + 15
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
                    with context.wrap_socket(
                        connection, server_hostname="turn.example.test"
                    ) as tls:
                        transaction = os.urandom(12)
                        tls.sendall(struct.pack("!HHI12s", 0x0001, 0, 0x2112A442, transaction))
                        with tls.makefile("rb") as response:
                            header = response.read(20)
                        kind, _length, magic, returned_transaction = struct.unpack(
                            "!HHI12s", header
                        )
                        assert (kind, magic, returned_transaction) == (
                            0x0101,
                            0x2112A442,
                            transaction,
                        )
                break
            except (ConnectionError, TimeoutError, ssl.SSLEOFError):
                if time.monotonic() >= deadline:
                    logs = subprocess.run(
                        ["docker", "logs", container], capture_output=True, text=True
                    )
                    pytest.fail(f"隔离 LiveKit TURN/TLS 未就绪：{logs.stdout}{logs.stderr}")
                time.sleep(0.1)
    finally:
        subprocess.run(
            ["docker", "stop", "--time", "2", container], capture_output=True, check=True
        )


def test_turn_check_does_not_write_runtime_configs(turn_deployment):
    deploy, _ = turn_deployment
    result = render(deploy, "--check")
    assert result.returncode == 0, result.stderr + result.stdout
    assert not (deploy / "runtime/livekit.yaml").exists()


@pytest.mark.parametrize(
    "failure",
    [
        "missing_domain",
        "invalid_domain",
        "missing_cert",
        "missing_key",
        "wrong_domain",
        "wrong_key",
        "expired",
        "expiring",
        "untrusted",
    ],
)
def test_turn_config_fails_closed_before_overwriting_runtime(turn_deployment, failure, monkeypatch):
    deploy, issue_certificate = turn_deployment
    cert_dir = deploy / "runtime/turn"
    if failure in {"missing_domain", "invalid_domain"}:
        env_file = deploy / ".env"
        env_file.write_text(
            env_file.read_text().replace(
                "turn.example.test",
                "" if failure == "missing_domain" else "https://turn.example.test",
            )
        )
    elif failure == "missing_cert":
        (cert_dir / "fullchain.pem").unlink()
    elif failure == "missing_key":
        (cert_dir / "privkey.pem").unlink()
    elif failure == "wrong_domain":
        issue_certificate(domain="other.example.test")
    elif failure == "wrong_key":
        key_file = cert_dir / "privkey.pem"
        old_key = key_file.read_bytes()
        issue_certificate()
        key_file.write_bytes(old_key)
    elif failure in {"expired", "expiring"}:
        issue_certificate(days=-1 if failure == "expired" else 3)
    elif failure == "untrusted":
        monkeypatch.delenv("SSL_CERT_FILE")
    config = deploy / "runtime/livekit.yaml"
    config.write_text("existing config must survive failed validation\n")
    result = render(deploy)
    assert result.returncode != 0
    assert "TURN" in result.stderr + result.stdout
    assert config.read_text() == "existing config must survive failed validation\n"
    assert "test-secret-only" not in result.stderr + result.stdout


def test_generated_turn_secrets_are_ignored_by_git():
    result = subprocess.run(
        [
            "git",
            "check-ignore",
            "deploy/ai-call-118/runtime/turn/privkey.pem",
            "deploy/ai-call-118/runtime/livekit.yaml",
        ],
        cwd=DEPLOY.parents[1],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert len(result.stdout.splitlines()) == 2
