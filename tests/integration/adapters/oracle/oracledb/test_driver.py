"""OracleDB sync and async driver-specific integration tests."""

import inspect
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any, cast
from uuid import uuid4

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OraclePoolParams,
    OracleSessionContextConfig,
    OracleSyncConfig,
    OracleSyncDriver,
)
from sqlspec.adapters.oracledb.tenant import (
    OracleAsyncIdentifierStrategy,
    OracleAsyncVPDStrategy,
    OracleSyncIdentifierStrategy,
    OracleSyncVPDStrategy,
)
from sqlspec.exceptions import SQLBuilderError, TenantCleanupError, TenantContextError
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.tenant import TenantContext

pytestmark = pytest.mark.xdist_group("oracle")

OracleFamilyDriver = OracleSyncDriver | OracleAsyncDriver


@pytest.fixture(params=("sync", "async"))
async def oracle_family_session(
    request: pytest.FixtureRequest, oracle_sync_session: OracleSyncDriver, oracle_async_session: OracleAsyncDriver
) -> AsyncGenerator[OracleFamilyDriver, None]:
    """Provide equivalent Oracle sync and async sessions."""
    yield oracle_sync_session if request.param == "sync" else oracle_async_session


@pytest.mark.parametrize("mode", ("sync", "async"))
async def test_connection(mode: str, oracle_23ai_service: OracleService) -> None:
    """Test native pool and cursor behavior for both Oracle modes."""
    base_config = OraclePoolParams(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
        retry_count=5,
        retry_delay=1,
    )
    pooled_config = OraclePoolParams(**base_config)
    pooled_config["min"] = 1
    pooled_config["max"] = 5
    for pool_params in (base_config, pooled_config):
        if mode == "sync":
            config = OracleSyncConfig(connection_config=pool_params)
            pool = config.create_pool()
            try:
                with pool.acquire() as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT 1 FROM dual")
                    assert cursor.fetchone() == (1,)
            finally:
                pool.close()
        else:
            async_config = OracleAsyncConfig(connection_config=pool_params)
            async_pool = await async_config.create_pool()
            try:
                async with async_pool.acquire() as connection, connection.cursor() as cursor:
                    await cursor.execute("SELECT 1 FROM dual")
                    assert await cursor.fetchone() == (1,)
            finally:
                await async_pool.close()


async def test_for_update_nowait(oracle_family_session: OracleFamilyDriver) -> None:
    """Test Oracle FOR UPDATE NOWAIT in both driver modes."""
    table = _table_name(oracle_family_session)
    value = f"{_mode_name(oracle_family_session)}_nowait"
    await _create_lock_table(oracle_family_session, table)
    await _invoke(
        _method(oracle_family_session, "execute"),
        f"INSERT INTO {table} (id, name, value) VALUES (1, :1, :2)",
        (value, 200),
    )
    try:
        await _invoke(_method(oracle_family_session, "begin"))
        result = await _invoke(
            _method(oracle_family_session, "select_one"),
            sql.select("*").from_(table).where_eq("name", value).for_update(nowait=True),
        )
        assert result is not None
        assert result["name"] == value
        await _invoke(_method(oracle_family_session, "commit"))
    except Exception:
        await _invoke(_method(oracle_family_session, "rollback"))
        raise
    finally:
        await _drop_lock_table(oracle_family_session, table)


async def test_for_share_locking_unsupported(oracle_family_session: OracleFamilyDriver) -> None:
    """Test that Oracle rejects FOR SHARE in both driver modes."""
    table = _table_name(oracle_family_session)
    value = f"{_mode_name(oracle_family_session)}_share"
    await _create_lock_table(oracle_family_session, table)
    await _invoke(
        _method(oracle_family_session, "execute"),
        f"INSERT INTO {table} (id, name, value) VALUES (1, :1, :2)",
        (value, 300),
    )
    try:
        await _invoke(_method(oracle_family_session, "begin"))
        with pytest.raises(SQLBuilderError, match="does not support FOR SHARE"):
            await _invoke(
                _method(oracle_family_session, "select_one"),
                sql.select("id", "name", "value").from_(table).where_eq("name", value).for_share(),
            )
        await _invoke(_method(oracle_family_session, "rollback"))
    except Exception:
        await _invoke(_method(oracle_family_session, "rollback"))
        raise
    finally:
        await _drop_lock_table(oracle_family_session, table)


async def _invoke(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _method(driver: OracleFamilyDriver, name: str) -> Callable[..., Any]:
    return cast("Callable[..., Any]", getattr(driver, name))


def _mode_name(driver: OracleFamilyDriver) -> str:
    return "async" if isinstance(driver, OracleAsyncDriver) else "sync"


def _table_name(driver: OracleFamilyDriver) -> str:
    return f"test_table_oracledb_{_mode_name(driver)}"


async def _create_lock_table(driver: OracleFamilyDriver, table: str) -> None:
    await _invoke(
        _method(driver, "execute_script"),
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
    )
    await _invoke(
        _method(driver, "execute_script"),
        f"CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50), value NUMBER)",
    )


async def _drop_lock_table(driver: OracleFamilyDriver, table: str) -> None:
    await _invoke(
        _method(driver, "execute_script"),
        f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; EXCEPTION WHEN OTHERS THEN NULL; END;",
    )


@asynccontextmanager
async def _tenant_oracle_session(
    config: OracleSyncConfig | OracleAsyncConfig,
) -> AsyncGenerator[OracleFamilyDriver, None]:
    if isinstance(config, OracleAsyncConfig):
        async with config.provide_session() as session:
            yield session
    else:
        with config.provide_session() as session:
            yield session


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_tenant_identifier_reuse_clear_hooks_and_tracing(mode: str, oracle_23ai_service: OracleService) -> None:
    hook = OracleSessionContextConfig(nls_parameters={"NLS_DATE_FORMAT": "YYYY-MM-DD"})
    config_cls = OracleSyncConfig if mode == "sync" else OracleAsyncConfig
    strategy_cls = OracleSyncIdentifierStrategy if mode == "sync" else OracleAsyncIdentifierStrategy
    config = config_cls(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 1,
        },
        driver_features={
            "tenant_strategy": strategy_cls(hook),
            "session_context_hook": hook,
            "enable_session_tracing": True,
        },
    )
    try:
        with pytest.raises(TenantContextError):
            async with _tenant_oracle_session(config):
                pass
        observed: list[tuple[str, str]] = []
        for tenant in ("a", "b"):
            with TenantContext.scope(tenant), CorrelationContext.context("hdr"):
                async with _tenant_oracle_session(config) as session:
                    for _ in range(2):
                        assert (
                            await _invoke(
                                _method(session, "select_value"),
                                "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL",
                            )
                            == tenant
                        )
                    sid = str(
                        await _invoke(
                            _method(session, "select_value"), "SELECT SYS_CONTEXT('USERENV', 'SID') FROM DUAL"
                        )
                    )
                    observed.append((tenant, sid))
                    assert (
                        await _invoke(
                            _method(session, "select_value"),
                            "SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER = 'NLS_DATE_FORMAT'",
                        )
                        == "YYYY-MM-DD"
                    )
            pool = await _invoke(config.provide_pool)
            connection = await _invoke(pool.acquire)
            try:
                with connection.cursor() as cursor:
                    await _invoke(
                        cursor.execute,
                        "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER'), SYS_CONTEXT('USERENV', 'SID') FROM DUAL",
                    )
                    assert await _invoke(cursor.fetchone) == (None, sid)
            finally:
                await _invoke(pool.release, connection)
        assert observed[0][1] == observed[1][1]
        with TenantContext.scope("x" * 65), pytest.raises(TenantContextError):
            async with _tenant_oracle_session(config):
                pass
        with TenantContext.scope("valid"):
            async with _tenant_oracle_session(config) as session:
                assert (
                    await _invoke(
                        _method(session, "select_value"), "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL"
                    )
                    == "valid"
                )
    finally:
        await _invoke(config.close_pool)


class _OracleSyncResetFailureStrategy(OracleSyncIdentifierStrategy):
    __slots__ = ()

    def checkin(self, connection: Any, state: Any) -> None:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN RAISE_APPLICATION_ERROR(-20001, 'tenant reset failure'); END;")


class _OracleAsyncResetFailureStrategy(OracleAsyncIdentifierStrategy):
    __slots__ = ()

    async def checkin(self, connection: Any, state: Any) -> None:
        with connection.cursor() as cursor:
            await cursor.execute("BEGIN RAISE_APPLICATION_ERROR(-20001, 'tenant reset failure'); END;")


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_tenant_failed_reset_replaces_physical_session(mode: str, oracle_23ai_service: OracleService) -> None:
    config_cls = OracleSyncConfig if mode == "sync" else OracleAsyncConfig
    strategy_cls = _OracleSyncResetFailureStrategy if mode == "sync" else _OracleAsyncResetFailureStrategy
    config = config_cls(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": oracle_23ai_service.user,
            "password": oracle_23ai_service.password,
            "min": 1,
            "max": 1,
        },
        driver_features={"tenant_strategy": strategy_cls()},
    )
    query = "SELECT SYS_CONTEXT('USERENV', 'SID') || ':' || SYS_CONTEXT('USERENV', 'SESSIONID') FROM DUAL"
    original: str | None = None
    try:
        with TenantContext.scope("a"), pytest.raises(TenantCleanupError):
            async with _tenant_oracle_session(config) as session:
                original = await _invoke(_method(session, "select_value"), query)
        assert isinstance(original, str)
        with TenantContext.bypass():
            async with _tenant_oracle_session(config) as session:
                replacement = await _invoke(_method(session, "select_value"), query)
                assert replacement != original
                assert (
                    await _invoke(
                        _method(session, "select_value"), "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL"
                    )
                    is None
                )
    finally:
        await _invoke(config.close_pool)


@pytest.fixture
async def oracle_tenant_vpd_schema(oracle_23ai_service: OracleService) -> AsyncGenerator[dict[str, Any], None]:
    import oracledb

    owner = f"TENANT_{uuid4().hex[:8].upper()}"
    namespace = f"CTX_{uuid4().hex[:8].upper()}"
    password = "Tenant_native_42"
    params = {
        "host": oracle_23ai_service.host,
        "port": oracle_23ai_service.port,
        "service_name": oracle_23ai_service.service_name,
    }
    with oracledb.connect(**params, user="system", password=oracle_23ai_service.system_password) as admin:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE USER {owner} IDENTIFIED BY "{password}"')
            try:
                cursor.execute(f"GRANT CREATE SESSION, CREATE TABLE, CREATE PROCEDURE, UNLIMITED TABLESPACE TO {owner}")
                cursor.execute(f"CREATE TABLE {owner}.tenant_rows (tenant VARCHAR2(64), value NUMBER)")
                cursor.execute(f"""CREATE OR REPLACE PACKAGE {owner}.tenant_ctx_pkg AS
                    PROCEDURE set_tenant(p_tenant IN VARCHAR2);
                    PROCEDURE clear_tenant;
                    END;""")
                cursor.execute(f"""CREATE OR REPLACE PACKAGE BODY {owner}.tenant_ctx_pkg AS
                    PROCEDURE set_tenant(p_tenant IN VARCHAR2) IS BEGIN
                        DBMS_SESSION.SET_CONTEXT('{namespace}', 'TENANT_ID', p_tenant);
                    END;
                    PROCEDURE clear_tenant IS BEGIN
                        DBMS_SESSION.CLEAR_CONTEXT('{namespace}', NULL, 'TENANT_ID');
                    END;
                    END;""")
                cursor.execute(f"CREATE CONTEXT {namespace} USING {owner}.tenant_ctx_pkg")
                cursor.execute(f"""CREATE OR REPLACE FUNCTION {owner}.tenant_predicate(
                    p_schema IN VARCHAR2, p_object IN VARCHAR2) RETURN VARCHAR2 AS
                    BEGIN RETURN 'tenant = SYS_CONTEXT(''{namespace}'', ''TENANT_ID'')'; END;""")
                cursor.execute(
                    """BEGIN DBMS_RLS.ADD_POLICY(
                    object_schema => :owner, object_name => 'TENANT_ROWS', policy_name => 'TENANT_POLICY',
                    function_schema => :owner, policy_function => 'TENANT_PREDICATE',
                    statement_types => 'SELECT,INSERT,UPDATE,DELETE', update_check => TRUE); END;""",
                    {"owner": owner},
                )
                yield {**params, "user": owner, "password": password, "namespace": namespace}
            finally:
                cursor.execute(f"DROP CONTEXT {namespace}")
                cursor.execute(f"DROP USER {owner} CASCADE")


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_tenant_vpd_policy_enforces_row_isolation(mode: str, oracle_tenant_vpd_schema: dict[str, Any]) -> None:
    params = dict(oracle_tenant_vpd_schema)
    namespace = params.pop("namespace")
    config_cls = OracleSyncConfig if mode == "sync" else OracleAsyncConfig
    strategy_cls = OracleSyncVPDStrategy if mode == "sync" else OracleAsyncVPDStrategy
    owner = params["user"]
    config = config_cls(
        connection_config={**params, "min": 1, "max": 1},
        driver_features={
            "tenant_strategy": strategy_cls(
                set_procedure=f"{owner}.tenant_ctx_pkg.set_tenant",
                clear_procedure=f"{owner}.tenant_ctx_pkg.clear_tenant",
                namespace=namespace,
                attribute="TENANT_ID",
            )
        },
    )
    try:
        for tenant, value in (("a", 10), ("b", 20)):
            with TenantContext.scope(tenant):
                async with _tenant_oracle_session(config) as session:
                    await _invoke(
                        _method(session, "execute"),
                        "INSERT INTO tenant_rows (tenant, value) VALUES (:tenant, :value)",
                        {"tenant": tenant, "value": value},
                    )
                    await _invoke(_method(session, "commit"))
        for tenant, value in (("a", 10), ("b", 20)):
            with TenantContext.scope(tenant):
                async with _tenant_oracle_session(config) as session:
                    assert await _invoke(_method(session, "select_value"), "SELECT COUNT(*) FROM tenant_rows") == 1
                    assert await _invoke(_method(session, "select_value"), "SELECT value FROM tenant_rows") == value
        with TenantContext.bypass():
            async with _tenant_oracle_session(config) as session:
                assert await _invoke(_method(session, "select_value"), "SELECT COUNT(*) FROM tenant_rows") == 0
    finally:
        await _invoke(config.close_pool)
