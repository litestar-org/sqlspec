"""Fixtures for shared adapter contract tests."""

from collections.abc import AsyncGenerator, Generator

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from tests.integration.adapters.contracts._cases import (
    ASYNC_DRIVER_PARAMS,
    DRIVER_PARAMS,
    SYNC_DRIVER_PARAMS,
    DriverCase,
    DriverCaseContext,
)
from tests.integration.adapters.contracts._schema import DEFAULT_CONTRACT_TABLE


@pytest.fixture
def contract_sqlite_driver() -> Generator[SqliteDriver, None, None]:
    """Provide a fresh SQLite driver for contract tests."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute("PRAGMA foreign_keys = ON")
            driver.execute_script(DEFAULT_CONTRACT_TABLE.create_sql)
            driver.commit()
            yield driver
    finally:
        config.close_pool()


@pytest.fixture
async def contract_aiosqlite_driver() -> AsyncGenerator[AiosqliteDriver, None]:
    """Provide a fresh aiosqlite driver for contract tests."""
    config = AiosqliteConfig()
    try:
        async with config.provide_session() as driver:
            await driver.execute("PRAGMA foreign_keys = ON")
            await driver.execute_script(DEFAULT_CONTRACT_TABLE.create_sql)
            await driver.commit()
            yield driver
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


def _resolve_driver_case(request: pytest.FixtureRequest, case: DriverCase) -> DriverCaseContext:
    return DriverCaseContext(case=case, driver=request.getfixturevalue(case.fixture_name))


@pytest.fixture(params=SYNC_DRIVER_PARAMS)
def sync_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve a sync driver contract case by fixture name."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture(params=ASYNC_DRIVER_PARAMS)
def async_driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve an async driver contract case by fixture name."""
    case = request.param
    return _resolve_driver_case(request, case)


@pytest.fixture(params=DRIVER_PARAMS)
def driver_case(request: pytest.FixtureRequest) -> DriverCaseContext:
    """Resolve any driver contract case by fixture name for metadata-only contracts."""
    case = request.param
    return _resolve_driver_case(request, case)
