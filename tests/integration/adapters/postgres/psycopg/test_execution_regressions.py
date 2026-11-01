"""PostgreSQL execution contracts across cached and native COPY paths."""

import inspect
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.exceptions import UniqueViolationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlspec.adapters.psycopg import PsycopgAsyncDriver, PsycopgSyncDriver
    from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture(params=["sync", "async"])
async def psycopg_regression_driver(
    request: pytest.FixtureRequest, psycopg_sync_config: PsycopgSyncConfig, psycopg_async_config: PsycopgAsyncConfig
) -> "AsyncIterator[PsycopgSyncDriver | PsycopgAsyncDriver]":
    if request.param == "sync":
        with psycopg_sync_config.provide_session() as driver:
            yield driver
    else:
        async with psycopg_async_config.provide_session() as async_driver:
            yield async_driver


async def _execute(driver: "PsycopgSyncDriver | PsycopgAsyncDriver", query: str, *parameters: Any) -> "SQLResult":
    result = driver.execute(query, *parameters)
    return await result if inspect.isawaitable(result) else result


@pytest.mark.parametrize(
    ("query", "parameters", "expected"),
    [
        ("SELECT '100%' AS value", (), "100%"),
        ("SELECT 10 % 3 AS value", (), 1),
        ("SELECT ?::integer % 3 AS value", (10,), 1),
    ],
)
async def test_psycopg_cached_percent_expressions(
    psycopg_regression_driver: "PsycopgSyncDriver | PsycopgAsyncDriver",
    query: str,
    parameters: tuple[int, ...],
    expected: str | int,
) -> None:
    """Native parameter parsing gives equal values before and after a cache hit."""
    for _ in range(2):
        result = await _execute(psycopg_regression_driver, query, *parameters)
        assert result.get_data() == [{"value": expected}]


async def test_psycopg_copy_constraint_error_is_mapped(
    psycopg_regression_driver: "PsycopgSyncDriver | PsycopgAsyncDriver",
) -> None:
    """The constraint error received when COPY finishes is translated consistently."""
    import psycopg

    pa = pytest.importorskip("pyarrow")
    driver = psycopg_regression_driver
    await _execute(driver, "CREATE TEMP TABLE copy_constraint_probe (id INTEGER PRIMARY KEY) ON COMMIT DROP")
    with pytest.raises(UniqueViolationError) as error:
        job = driver.load_from_arrow("copy_constraint_probe", pa.table({"id": [1, 1]}))
        if inspect.isawaitable(job):
            await job
    assert isinstance(error.value.__cause__, psycopg.errors.UniqueViolation)


@pytest.mark.parametrize("is_async", [False, True])
async def test_psycopg_sql_connection_hook_leaves_pool_usable(
    is_async: bool, psycopg_sync_config: PsycopgSyncConfig
) -> None:
    """SQL issued by a connection hook cannot leave the pool's configure callback in INTRANS."""
    connection_config = {**psycopg_sync_config.connection_config, "min_size": 1, "max_size": 1, "timeout": 2}
    if is_async:

        async def async_hook(connection: Any) -> None:
            await connection.execute("SET timezone TO 'Pacific/Honolulu'")

        config = PsycopgAsyncConfig(
            connection_config=connection_config, driver_features={"on_connection_create": async_hook}
        )
        try:
            async with config.provide_session() as driver:
                assert await driver.select_value("SELECT current_setting('TimeZone')") == "Pacific/Honolulu"
        finally:
            await config.close_pool()
    else:
        sync_config = PsycopgSyncConfig(
            connection_config=connection_config,
            driver_features={
                "on_connection_create": lambda connection: connection.execute("SET timezone TO 'Pacific/Honolulu'")
            },
        )
        try:
            with sync_config.provide_session() as sync_driver:
                assert sync_driver.select_value("SELECT current_setting('TimeZone')") == "Pacific/Honolulu"
        finally:
            sync_config.close_pool()
