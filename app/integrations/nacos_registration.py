from __future__ import annotations

import ipaddress
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from lingchen_sdk.nacos import (
    NacosClientSettings,
    NacosRuntime,
    NacosServiceInstance,
)

from app.common.enums import EnvironmentEnum
from app.config.setting import Settings, settings
from app.core.logger import log

if TYPE_CHECKING:
    from fastapi import FastAPI

REACH_NACOS_SERVICE_NAME = "lingchen-reach-python"
REACH_NACOS_CLUSTER_NAME = "DEFAULT"
REACH_NACOS_CONTEXT_PATH = "/reach-api/v1"

NacosConnector = Callable[[NacosClientSettings], Awaitable[NacosRuntime]]


def _validated_instance_ip(value: str, *, production: bool) -> str:
    try:
        address = ipaddress.ip_address(value.strip())
    except ValueError as exc:
        raise RuntimeError("NACOS_INSTANCE_IP必须是有效IPv4或IPv6地址") from exc
    if address.is_unspecified or address.is_multicast:
        raise RuntimeError("NACOS_INSTANCE_IP不能是未指定地址或组播地址")
    if production and (address.is_loopback or address.is_link_local):
        raise RuntimeError("生产NACOS_INSTANCE_IP必须是其他服务可路由访问的地址")
    return str(address)


def resolve_nacos_instance_ip(
    configured_ip: str,
    *,
    environment: EnvironmentEnum,
) -> str:
    production = environment == EnvironmentEnum.PROD
    if configured_ip.strip():
        return _validated_instance_ip(configured_ip, production=production)
    try:
        candidates = {
            str(row[4][0])
            for row in socket.getaddrinfo(
                socket.gethostname(),
                None,
                socket.AF_UNSPEC,
                socket.SOCK_STREAM,
            )
            if row[4]
        }
    except OSError as exc:
        raise RuntimeError("无法解析Nacos实例地址，请配置NACOS_INSTANCE_IP") from exc
    valid: list[str] = []
    for candidate in candidates:
        try:
            valid.append(_validated_instance_ip(candidate, production=production))
        except RuntimeError:
            continue
    if len(valid) == 1:
        return valid[0]
    if not production:
        return "127.0.0.1"
    raise RuntimeError("无法唯一解析生产Nacos实例地址，请配置NACOS_INSTANCE_IP")


@dataclass(slots=True)
class ReachNacosRegistration:
    runtime: NacosRuntime
    instance: NacosServiceInstance
    registered: bool = True

    @property
    def ready(self) -> bool:
        return self.registered

    async def close(self) -> None:
        if not self.registered:
            return
        try:
            await self.runtime.close()
        finally:
            self.registered = False


def _client_settings(config: Settings) -> NacosClientSettings:
    cache_dir = Path("storage/nacos/cache")
    log_dir = Path("storage/nacos/log")
    namespace_id = config.NACOS_DISCOVERY_NAMESPACE_ID.strip() or config.NACOS_NAMESPACE_ID
    (cache_dir / "naming" / namespace_id).mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    return NacosClientSettings(
        server_address=config.NACOS_SERVER_ADDRESS,
        namespace_id=namespace_id,
        group_name=config.NACOS_GROUP_NAME,
        username=config.NACOS_USERNAME.strip() or None,
        password=config.NACOS_PASSWORD.strip() or None,
        cache_dir=str(cache_dir),
        log_dir=str(log_dir),
    )


def _service_instance(config: Settings, instance_ip: str) -> NacosServiceInstance:
    return NacosServiceInstance(
        service_name=REACH_NACOS_SERVICE_NAME,
        ip=instance_ip,
        port=config.SERVER_PORT,
        group_name=config.NACOS_GROUP_NAME,
        cluster_name=REACH_NACOS_CLUSTER_NAME,
        metadata={
            "application": REACH_NACOS_SERVICE_NAME,
            "context-path": REACH_NACOS_CONTEXT_PATH,
            "productCode": "reach",
            "role": "api",
            "version": config.VERSION,
        },
        ephemeral=True,
    )


async def register_reach_api_with_nacos(
    app: FastAPI,
    *,
    config: Settings = settings,
    connector: NacosConnector | None = None,
) -> ReachNacosRegistration:
    if not config.NACOS_ENABLE:
        raise RuntimeError("Reach API禁止在未启用Nacos注册时执行服务注册")

    instance_ip = resolve_nacos_instance_ip(
        config.NACOS_INSTANCE_IP,
        environment=config.ENVIRONMENT,
    )
    runtime = await (connector or NacosRuntime.connect)(_client_settings(config))
    instance = _service_instance(config, instance_ip)
    try:
        await runtime.register_instance(instance)
    except BaseException:
        try:
            await runtime.close()
        except Exception as close_error:
            log.warning(
                "reach_nacos_cleanup_failed errorType={}",
                type(close_error).__name__,
            )
        raise

    registration = ReachNacosRegistration(runtime=runtime, instance=instance)
    app.state.nacos_registration = registration
    log.info(
        "reach_nacos_registered serviceName={} instanceIp={} port={} namespace={} group={}",
        instance.service_name,
        instance.ip,
        instance.port,
        config.NACOS_DISCOVERY_NAMESPACE_ID.strip() or config.NACOS_NAMESPACE_ID,
        config.NACOS_GROUP_NAME,
    )
    return registration


def nacos_registration_ready(app: FastAPI) -> bool:
    registration = getattr(app.state, "nacos_registration", None)
    return isinstance(registration, ReachNacosRegistration) and registration.ready


__all__ = [
    "REACH_NACOS_CONTEXT_PATH",
    "REACH_NACOS_SERVICE_NAME",
    "ReachNacosRegistration",
    "nacos_registration_ready",
    "register_reach_api_with_nacos",
    "resolve_nacos_instance_ip",
]
