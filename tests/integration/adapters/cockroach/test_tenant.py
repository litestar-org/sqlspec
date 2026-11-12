"""CockroachDB policies enforce tenant identity on the real provider boundaries."""

from collections.abc import Generator
from typing import Any, LiteralString

import psycopg
import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import SQLSpecError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContext
from tests.integration.fixtures.cockroach import _cockroach_asyncpg_connection_config, _cockroach_conninfo

pytestmark = [pytest.mark.xdist_group("cockroachdb")]


@pytest.fixture
def tenant_connection_settings(
    cockroachdb_service: CockroachDBService,
) -> Generator[tuple[str, dict[str, Any]], None, None]:
    conninfo = _cockroach_conninfo(cockroachdb_service)
    with psycopg.connect(conninfo, autocommit=True) as admin:
        admin.execute("CREATE USER IF NOT EXISTS sqlspec_tenant_user")
        admin.execute("DROP TABLE IF EXISTS sqlspec_tenant_rows")
        admin.execute("CREATE TABLE sqlspec_tenant_rows (tenant_id TEXT NOT NULL, value INT NOT NULL)")
        admin.execute("ALTER TABLE sqlspec_tenant_rows ENABLE ROW LEVEL SECURITY")
        admin.execute("ALTER TABLE sqlspec_tenant_rows FORCE ROW LEVEL SECURITY")
        admin.execute(
            "CREATE POLICY tenant_scope ON sqlspec_tenant_rows USING (tenant_id = current_setting('app.current_tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true))"
        )
        admin.execute("GRANT SELECT, INSERT ON TABLE sqlspec_tenant_rows TO sqlspec_tenant_user")
    params = _cockroach_asyncpg_connection_config(cockroachdb_service)
    params.update(user="sqlspec_tenant_user", min_size=1, max_size=1)
    yield conninfo.replace("user=root", "user=sqlspec_tenant_user"), params
    with psycopg.connect(conninfo, autocommit=True) as admin:
        admin.execute("DROP TABLE IF EXISTS sqlspec_tenant_rows")


class _InvalidRestore:
    """Fault the native restoration SQL while retaining the real strategy."""

    __slots__ = ("checkin", "checkout")

    def __init__(self, strategy: Any, asynchronous: bool) -> None:
        self.checkout = strategy.checkout

        def fail_sync(connection: Any, state: TenantLeaseState) -> None:
            strategy.checkin(connection, TenantLeaseState(state.payload, {"statement_timeout": "invalid"}))

        async def fail_async(connection: Any, state: TenantLeaseState) -> None:
            await strategy.checkin(connection, TenantLeaseState(state.payload, {"statement_timeout": "invalid"}))

        self.checkin = fail_async if asynchronous else fail_sync


def _native_scalar(connection: psycopg.Connection[Any], query: LiteralString) -> object:
    row = connection.execute(query).fetchone()
    if row is None:
        msg = "Native scalar query returned no row"
        raise AssertionError(msg)
    return row[0]


def _assert_rows(driver: Any, identity: str) -> None:
    assert {row["tenant_id"] for row in driver.select("SELECT tenant_id FROM sqlspec_tenant_rows")} == {identity}
    assert driver.select_value("SELECT current_setting('app.region', true)") == ("eu" if identity == "a" else "")


async def _assert_rows_async(driver: Any, identity: str) -> None:
    assert {row["tenant_id"] for row in await driver.select("SELECT tenant_id FROM sqlspec_tenant_rows")} == {identity}
    assert await driver.select_value("SELECT current_setting('app.region', true)") == ("eu" if identity == "a" else "")


def test_tenant_cockroach_psycopg_sync_isolation(tenant_connection_settings: tuple[str, dict[str, Any]]) -> None:
    conninfo, _ = tenant_connection_settings
    config = CockroachPsycopgSyncConfig(
        connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 1},
        driver_features={"tenant_strategy": "rls"},
    )
    assert config.driver_features["pooler_mode"] == "session"
    config.driver_features["tenant_strategy"] = type(config.driver_features["tenant_strategy"])(
        variable_keys={"region": "app.region"}
    )
    try:
        with pytest.raises(TenantContextError), config.provide_session():
            pass
        for identity in ("a", "b"):
            with (
                TenantContext.scope(identity, variables={"region": "eu"} if identity == "a" else None),
                config.provide_session() as driver,
            ):
                driver.execute(
                    "INSERT INTO sqlspec_tenant_rows VALUES (:tenant, :value)", {"tenant": identity, "value": 1}
                )
                driver.commit()
                _assert_rows(driver, identity)
        with TenantContext.scope("a"), config.provide_session() as driver, pytest.raises(SQLSpecError):
            driver.execute("INSERT INTO sqlspec_tenant_rows VALUES ('b', 2)")
        with TenantContext.scope("a"), config.provide_connection() as conn:
            assert _native_scalar(conn, "SELECT current_setting('app.current_tenant_id', true)") == "a"
        with config.provide_pool().connection() as conn:
            assert _native_scalar(conn, "SELECT current_setting('app.current_tenant_id', true)") in (None, "")
        strategy = config.driver_features["tenant_strategy"]
        config.driver_features["tenant_strategy"] = _InvalidRestore(strategy, False)
        failed_pid: object = None
        with TenantContext.scope("a"), pytest.raises(TenantCleanupError):
            with config.provide_connection() as conn:
                failed_pid = _native_scalar(conn, "SELECT pg_backend_pid()")
        config.driver_features["tenant_strategy"] = strategy
        with TenantContext.scope("a"), config.provide_connection() as conn:
            assert _native_scalar(conn, "SELECT pg_backend_pid()") != failed_pid

    finally:
        config.close_pool()


@pytest.mark.parametrize("adapter", ["asyncpg", "psycopg"])
async def test_tenant_cockroach_async_isolation(
    tenant_connection_settings: tuple[str, dict[str, Any]], adapter: str
) -> None:
    conninfo, params = tenant_connection_settings
    config: Any = (
        CockroachAsyncpgConfig(connection_config=params, driver_features={"tenant_strategy": "rls"})
        if adapter == "asyncpg"
        else CockroachPsycopgAsyncConfig(
            connection_config={"conninfo": conninfo, "min_size": 1, "max_size": 1},
            driver_features={"tenant_strategy": "rls"},
        )
    )
    assert config.driver_features["pooler_mode"] == "session"
    config.driver_features["tenant_strategy"] = type(config.driver_features["tenant_strategy"])(
        variable_keys={"region": "app.region"}
    )
    try:
        with pytest.raises(TenantContextError):
            async with config.provide_session():
                pass
        for identity in ("a", "b"):
            with TenantContext.scope(identity, variables={"region": "eu"} if identity == "a" else None):
                async with config.provide_session() as driver:
                    await driver.execute(
                        "INSERT INTO sqlspec_tenant_rows VALUES (:tenant, :value)", {"tenant": identity, "value": 1}
                    )
                    await driver.commit()
                    await _assert_rows_async(driver, identity)
        with TenantContext.scope("a"), pytest.raises(SQLSpecError):
            async with config.provide_session() as driver:
                await driver.execute("INSERT INTO sqlspec_tenant_rows VALUES ('b', 2)")
        with TenantContext.scope("a"):
            async with config.provide_connection() as conn:
                if adapter == "asyncpg":
                    assert await conn.fetchval("SELECT current_setting('app.current_tenant_id', true)") == "a"
                else:
                    cursor = await conn.execute("SELECT current_setting('app.current_tenant_id', true)")
                    assert (await cursor.fetchone())[0] == "a"
        pool = await config.provide_pool()
        if adapter == "asyncpg":
            async with pool.acquire() as conn:
                assert await conn.fetchval("SELECT current_setting('app.current_tenant_id', true)") in (None, "")
        else:
            async with pool.connection() as conn:
                cursor = await conn.execute("SELECT current_setting('app.current_tenant_id', true)")
                assert (await cursor.fetchone())[0] in (None, "")
        strategy = config.driver_features["tenant_strategy"]
        config.driver_features["tenant_strategy"] = _InvalidRestore(strategy, True)
        failed_pid: object = None
        with TenantContext.scope("a"), pytest.raises(TenantCleanupError):
            async with config.provide_connection() as conn:
                if adapter == "asyncpg":
                    failed_pid = await conn.fetchval("SELECT pg_backend_pid()")
                else:
                    cursor = await conn.execute("SELECT pg_backend_pid()")
                    failed_pid = (await cursor.fetchone())[0]
        config.driver_features["tenant_strategy"] = strategy
        with TenantContext.scope("a"):
            async with config.provide_connection() as conn:
                if adapter == "asyncpg":
                    assert await conn.fetchval("SELECT pg_backend_pid()") != failed_pid
                else:
                    cursor = await conn.execute("SELECT pg_backend_pid()")
                    assert (await cursor.fetchone())[0] != failed_pid
    finally:
        await config.close_pool()


@pytest.mark.parametrize("scenario", ["deadline", "cancel", "reset_error"])
async def test_tenant_cockroach_native_reset_finishes_before_holder_reuse(
    cockroachdb_service: CockroachDBService, scenario: str
) -> None:
    from tests.integration.adapters._asyncpg_tenant_release import assert_owned_tenant_reset

    await assert_owned_tenant_reset(
        CockroachAsyncpgConfig, _cockroach_asyncpg_connection_config(cockroachdb_service), scenario
    )
