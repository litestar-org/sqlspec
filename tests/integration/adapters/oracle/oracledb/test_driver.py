"""OracleDB sync and async driver-specific integration tests."""

import inspect
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec import sql
from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OraclePoolParams,
    OracleSyncConfig,
    OracleSyncDriver,
)
from sqlspec.exceptions import SQLSpecError

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
        with pytest.raises(SQLSpecError, match=r"ORA-02000.*missing COMPRESS or UPDATE keyword"):
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
