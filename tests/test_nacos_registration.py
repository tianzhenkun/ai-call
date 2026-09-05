from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI

from app.config.setting import Settings
from app.integrations.nacos_registration import (
    REACH_NACOS_CONTEXT_PATH,
    REACH_NACOS_SERVICE_NAME,
    _client_settings,
    register_reach_api_with_nacos,
)
from app.plugin.init_app import validate_reach_api_runtime_contract


def test_registration_uses_gateway_discovery_namespace() -> None:
    config = Settings(
        NACOS_NAMESPACE_ID="reach-dev",
        NACOS_DISCOVERY_NAMESPACE_ID="local",
    )

    assert _client_settings(config).namespace_id == "local"


@pytest.mark.anyio
async def test_register_reach_api_publishes_stable_gateway_contract() -> None:
    app = FastAPI()
    runtime = AsyncMock()
    connector = AsyncMock(return_value=runtime)
    config = Settings(
        NACOS_ENABLE=True,
        NACOS_INSTANCE_IP="127.0.0.1",
        NACOS_NAMESPACE_ID="local",
        SERVER_PORT=19010,
    )

    registration = await register_reach_api_with_nacos(
        app,
        config=config,
        connector=connector,
    )

    runtime.register_instance.assert_awaited_once()
    instance = runtime.register_instance.await_args.args[0]
    assert instance.service_name == REACH_NACOS_SERVICE_NAME
    assert instance.metadata["context-path"] == REACH_NACOS_CONTEXT_PATH
    assert instance.metadata["productCode"] == "reach"
    assert app.state.nacos_registration is registration

    await registration.close()
    runtime.close.assert_awaited_once()


def test_production_api_contract_requires_platform_auth_and_nacos() -> None:
    config = Settings(
        ENVIRONMENT="prod",
        ROOT_PATH="/reach-api/v1",
        JWT_ENABLE=True,
        NACOS_ENABLE=False,
        AI_CALL_STANDALONE_ENABLE=False,
    )

    with pytest.raises(RuntimeError, match="Nacos"):
        validate_reach_api_runtime_contract(config)


def test_runtime_contract_rejects_half_configured_legacy_tenant_mapping() -> None:
    config = Settings(
        AI_CALL_PLATFORM_TENANT_ID="960001",
        AI_CALL_LEGACY_DATA_TENANT_ID="",
    )

    with pytest.raises(RuntimeError, match="必须同时配置"):
        validate_reach_api_runtime_contract(config)
