"""Db2 RCAC row isolation through verified session global variables."""

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from inspect import isawaitable
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.db2 import (
    Db2AsyncConfig,
    Db2AsyncGlobalVariableStrategy,
    Db2ConnectionParams,
    Db2SyncConfig,
    Db2SyncGlobalVariableStrategy,
)
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import TenantCleanupError
from sqlspec.utils.tenant import TenantContext

pytestmark = [pytest.mark.xdist_group("db2"), pytest.mark.anyio]


async def _invoke(function: Any, *args: Any) -> Any:
    result = function(*args)
    return await result if isawaitable(result) else result


@asynccontextmanager
async def _session(config: Any) -> AsyncIterator[Any]:
    if isinstance(config, Db2AsyncConfig):
        async with config.provide_session() as session:
            yield session
    else:
        with config.provide_session() as session:
            yield session


@pytest.fixture
def rcac_schema(db2_connection_config: Db2ConnectionParams) -> Iterator[str]:
    """Create a tenant global variable and an RCAC-protected table owned by the test user."""
    schema = f"T{uuid4().hex[:8].upper()}"
    admin = Db2SyncConfig(connection_config=dict(db2_connection_config))
    statements = [
        f"CREATE VARIABLE {schema}.TENANT_ID VARCHAR(256) DEFAULT NULL",
        f"CREATE TABLE {schema}.TENANT_ROWS (TENANT_ID VARCHAR(256) NOT NULL, VALUE INTEGER NOT NULL)",
        f"INSERT INTO {schema}.TENANT_ROWS VALUES ('a', 1), ('b', 2)",
        (
            f"CREATE PERMISSION {schema}.TENANT_ROWS_PERMISSION ON {schema}.TENANT_ROWS FOR ROWS "
            f"WHERE TENANT_ID = {schema}.TENANT_ID ENFORCED FOR ALL ACCESS ENABLE"
        ),
        f"ALTER TABLE {schema}.TENANT_ROWS ACTIVATE ROW ACCESS CONTROL",
    ]
    try:
        with admin.provide_session() as session:
            for statement in statements:
                session.execute(statement)
            session.commit()
        yield schema
    finally:
        with admin.provide_session() as session:
            for statement in (
                f"DROP TABLE {schema}.TENANT_ROWS",
                f"DROP VARIABLE {schema}.TENANT_ID",
                f"DROP SCHEMA {schema} RESTRICT",
            ):
                try:
                    session.execute(statement)
                except Exception:
                    session.rollback()
            session.commit()
        admin.close_pool()


def _config(mode: str, connection_config: Db2ConnectionParams, strategy: Any) -> Any:
    config_type = Db2SyncConfig if mode == "sync" else Db2AsyncConfig
    return config_type(connection_config=dict(connection_config), driver_features={"tenant_strategy": strategy})


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_tenant_global_variable_enforces_rcac_and_resets(
    mode: str, rcac_schema: str, db2_connection_config: Db2ConnectionParams
) -> None:
    strategy_type = Db2SyncGlobalVariableStrategy if mode == "sync" else Db2AsyncGlobalVariableStrategy
    config = _config(mode, db2_connection_config, strategy_type(tenant_variable=f"{rcac_schema}.TENANT_ID"))
    query = f"SELECT VALUE FROM {rcac_schema}.TENANT_ROWS ORDER BY VALUE"
    try:
        for tenant, expected in (("a", [1]), ("b", [2])):
            with TenantContext.scope(tenant):
                async with _session(config) as session:
                    rows = await _invoke(session.select, query)
                    assert [row["value"] for row in rows] == expected
        with TenantContext.bypass():
            async with _session(config) as session:
                assert await _invoke(session.select, query) == []
                assert await _invoke(session.select_value, f"VALUES {rcac_schema}.TENANT_ID") is None
        if mode == "async":
            pool = await config.provide_pool()
            connection = await pool.acquire()
            try:
                cursor = await connection.cursor()
                await cursor.execute(f"VALUES {rcac_schema}.TENANT_ID")
                assert (await cursor.fetchone())[0] is None
                await cursor.close()
            finally:
                await pool.release(connection)
    finally:
        await _invoke(config.close_pool)


class _SyncFailingReset(Db2SyncGlobalVariableStrategy):
    __slots__ = ()

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        msg = "reset failure"
        raise RuntimeError(msg)


class _AsyncFailingReset(Db2AsyncGlobalVariableStrategy):
    __slots__ = ()

    async def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        msg = "reset failure"
        raise RuntimeError(msg)


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_tenant_unverified_reset_disposes_connection(
    mode: str, rcac_schema: str, db2_connection_config: Db2ConnectionParams
) -> None:
    strategy_type = _SyncFailingReset if mode == "sync" else _AsyncFailingReset
    config = _config(mode, db2_connection_config, strategy_type(tenant_variable=f"{rcac_schema}.TENANT_ID"))
    try:
        with TenantContext.scope("a"), pytest.raises(TenantCleanupError):
            async with _session(config) as session:
                await _invoke(session.select_value, "VALUES 1")
        if mode == "async":
            pool = await config.provide_pool()
            assert (pool.size(), pool.checked_out()) == (0, 0)
    finally:
        await _invoke(config.close_pool)
