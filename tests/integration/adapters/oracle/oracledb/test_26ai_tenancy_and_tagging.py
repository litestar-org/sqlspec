"""Live Oracle 26ai tenancy, configuration provider, and session reuse contracts."""

import base64
import inspect
import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import (
    OracleAsyncClientContextStrategy,
    OracleAsyncConfig,
    OracleSessionContextConfig,
    OracleSyncClientContextStrategy,
    OracleSyncConfig,
    synthesize_session_state_tag,
)
from sqlspec.exceptions import SQLSpecError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext

pytestmark = pytest.mark.xdist_group("oracle")

_IDENTITY = "SELECT SYS_CONTEXT('USERENV', 'SID') || ':' || SYS_CONTEXT('USERENV', 'SESSIONID') FROM DUAL"


async def _invoke(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = function(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


def _config(mode: str, service: OracleService, **features: Any) -> Any:
    config_type = OracleSyncConfig if mode == "sync" else OracleAsyncConfig
    return config_type(
        connection_config={
            "host": service.host,
            "port": service.port,
            "service_name": service.service_name,
            "user": service.user,
            "password": service.password,
            "min": 1,
            "max": 1,
        },
        driver_features=features,
    )


@asynccontextmanager
async def _session(config: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
    if isinstance(config, OracleAsyncConfig):
        async with config.provide_session(**kwargs) as session:
            yield session
    else:
        with config.provide_session(**kwargs) as session:
            yield session


async def _raw_values(config: Any, statement: str) -> tuple[Any, ...]:
    pool = await _invoke(config.create_pool)
    connection = await _invoke(pool.acquire)
    try:
        with connection.cursor() as cursor:
            await _invoke(cursor.execute, statement)
            return tuple(await _invoke(cursor.fetchone))
    finally:
        await _invoke(pool.release, connection)


def _admin(service: OracleService, statement: str) -> None:
    code, output = service.exec_sql(statement, user="system")
    if code:
        raise RuntimeError(output.decode("utf-8", errors="replace"))


@pytest.mark.parametrize("mode", ("sync", "async"))
@pytest.mark.parametrize("tenant_bypass", (False, True))
async def test_zero_rtt_clientcontext_and_current_schema_restore(
    mode: str, tenant_bypass: bool, oracle_26ai_service: OracleService
) -> None:
    schema = f"ALT_{uuid4().hex[:8].upper()}"
    _admin(oracle_26ai_service, f'CREATE USER {schema} IDENTIFIED BY "Tenant_native_42" QUOTA UNLIMITED ON USERS;')
    try:
        _admin(
            oracle_26ai_service,
            f"CREATE TABLE {schema}.context_probe (value NUMBER);\n"
            f"INSERT INTO {schema}.context_probe VALUES (42);\n"
            f"GRANT SELECT ON {schema}.context_probe TO {oracle_26ai_service.user};\nCOMMIT;",
        )
        strategy_type = OracleSyncClientContextStrategy if mode == "sync" else OracleAsyncClientContextStrategy
        tenant_features = {"tenant_strategy": strategy_type()} if tenant_bypass else {}
        config = _config(
            mode,
            oracle_26ai_service,
            **tenant_features,
            session_context_hook=OracleSessionContextConfig(
                contexts={"CLIENTCONTEXT": {"tier": "enterprise", "region": "us-east"}},
                clientinfo="oracle26ai-test",
                dbop="context-probe",
                current_schema=schema,
            ),
        )
        try:
            async with _session(config, bypass_tenant=tenant_bypass) as session:
                identity = await _invoke(session.select_value, _IDENTITY)
                assert await _invoke(session.select_value, "SELECT value FROM context_probe") == 42
                row = await _invoke(
                    session.select_one,
                    "SELECT SYS_CONTEXT('CLIENTCONTEXT', 'tier') AS tier, "
                    "SYS_CONTEXT('CLIENTCONTEXT', 'region') AS region, "
                    "SYS_CONTEXT('USERENV', 'CLIENT_INFO') AS clientinfo FROM DUAL",
                )
                assert row == {"tier": "enterprise", "region": "us-east", "clientinfo": "oracle26ai-test"}
            raw = await _raw_values(
                config,
                "SELECT SYS_CONTEXT('USERENV', 'SID') || ':' || SYS_CONTEXT('USERENV', 'SESSIONID'), "
                "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'), SYS_CONTEXT('CLIENTCONTEXT', 'tier'), "
                "SYS_CONTEXT('CLIENTCONTEXT', 'region') FROM DUAL",
            )
            assert raw == (identity, oracle_26ai_service.user.upper(), None, None)
        finally:
            await _invoke(config.close_pool)
    finally:
        _admin(oracle_26ai_service, f"DROP USER {schema} CASCADE;")


@pytest.mark.parametrize("mode", ("sync", "async"))
async def test_client_context_tenant_strategy_variables_and_failed_reset_pool_drop(
    mode: str, oracle_26ai_service: OracleService
) -> None:
    strategy_type = OracleSyncClientContextStrategy if mode == "sync" else OracleAsyncClientContextStrategy
    config = _config(mode, oracle_26ai_service, tenant_strategy=strategy_type(variable_keys=("region", "tier")))
    try:
        with pytest.raises(TenantContextError):
            async with _session(config):
                pass
        for tenant in ("tenant-a", "tenant-b"):
            with TenantContext.scope(tenant, variables={"region": "us-east", "tier": "enterprise"}):
                async with _session(config) as session:
                    row = await _invoke(
                        session.select_one,
                        "SELECT SYS_CONTEXT('CLIENTCONTEXT', 'TENANT_ID') AS tenant, "
                        "SYS_CONTEXT('CLIENTCONTEXT', 'region') AS region FROM DUAL",
                    )
                    assert row == {"tenant": tenant, "region": "us-east"}
        with TenantContext.scope("tenant-a", variables={"unknown": "secret"}), pytest.raises(TenantContextError):
            async with _session(config):
                pass
        assert await _raw_values(config, "SELECT SYS_CONTEXT('CLIENTCONTEXT', 'TENANT_ID') FROM DUAL") == (None,)
    finally:
        await _invoke(config.close_pool)

    class FailingSyncStrategy(OracleSyncClientContextStrategy):
        __slots__ = ()

        def checkin(self, connection: Any, state: Any) -> None:
            raise RuntimeError("cleanup failure")

    class FailingAsyncStrategy(OracleAsyncClientContextStrategy):
        __slots__ = ()

        async def checkin(self, connection: Any, state: Any) -> None:
            raise RuntimeError("cleanup failure")

    failing_strategy = FailingSyncStrategy() if mode == "sync" else FailingAsyncStrategy()
    config = _config(mode, oracle_26ai_service, tenant_strategy=failing_strategy)
    original: Any = None
    try:
        with TenantContext.scope("tenant-a"), pytest.raises(TenantCleanupError):
            async with _session(config) as session:
                original = await _invoke(session.select_value, _IDENTITY)
        with TenantContext.bypass():
            async with _session(config) as session:
                assert await _invoke(session.select_value, _IDENTITY) != original
    finally:
        await _invoke(config.close_pool)


@pytest.mark.parametrize("mode", ("sync", "async"))
async def test_config_file_uri_and_refresh_config_cache(
    mode: str, oracle_26ai_service: OracleService, tmp_path: Path
) -> None:
    path = tmp_path / "oracle.json"
    path.write_text(
        json.dumps({
            "primary": {
                "connect_descriptor": f"{oracle_26ai_service.host}:{oracle_26ai_service.port}/{oracle_26ai_service.service_name}",
                "user": oracle_26ai_service.user,
                "password": {
                    "type": "base64",
                    "value": base64.b64encode(oracle_26ai_service.password.encode()).decode(),
                },
                "config_time_to_live": 120,
            }
        })
    )
    config_type = OracleSyncConfig if mode == "sync" else OracleAsyncConfig
    config = config_type(connection_config={"dsn": f"file://{path}?key=primary", "min": 1, "max": 1})
    try:
        async with _session(config) as session:
            assert await _invoke(session.select_value, "SELECT 1 FROM DUAL") == 1
        await _invoke(config.close_pool)
        path.unlink()
        async with _session(config) as session:
            assert await _invoke(session.select_value, "SELECT 2 FROM DUAL") == 2
        await _invoke(config.close_pool)
        config.refresh_config_cache(remove_permanently=True)
        with pytest.raises(SQLSpecError):
            async with _session(config):
                pass
    finally:
        await _invoke(config.close_pool)


@pytest.mark.parametrize("mode", ("sync", "async"))
async def test_auto_session_state_tagging_and_thin_release_hygiene(
    mode: str, oracle_26ai_service: OracleService
) -> None:
    hook = OracleSessionContextConfig(time_zone="UTC")
    assert synthesize_session_state_tag(hook, explicit_tag="ROLE=ANALYST") == "ROLE=ANALYST;TIME_ZONE=UTC"
    config = _config(mode, oracle_26ai_service, session_context_hook=hook, enable_auto_state_tagging=True)
    try:
        identities = []
        for _ in range(2):
            async with _session(config, tag="ROLE=ANALYST", match_any_tag=True) as session:
                assert session.connection.thin is True
                assert await _invoke(session.select_value, "SELECT SESSIONTIMEZONE FROM DUAL") == "UTC"
                identities.append(await _invoke(session.select_value, _IDENTITY))
        assert identities[0] == identities[1]
    finally:
        await _invoke(config.close_pool)
