"""SAP SQL Anywhere integration test fixtures."""

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases.docker.sqlanywhere import SQLAnywhereService, sqlanywhere_client_libs

from sqlspec.adapters.sqlanywhere import SqlanywhereConfig

if TYPE_CHECKING:
    from sqlspec.adapters.sqlanywhere import SqlanywhereDriver

__all__ = (
    "sqlanywhere_client_libs",
    "sqlanywhere_config",
    "sqlanywhere_connection_config",
    "sqlanywhere_session",
    "xdist_sqlanywhere_isolation_level",
)


@pytest.fixture(scope="session")
def xdist_sqlanywhere_isolation_level() -> str:
    """Run one SQL Anywhere server per worker because the demo image cannot create databases on demand."""
    return "server"


@pytest.fixture(scope="session")
def sqlanywhere_connection_config(
    sqlanywhere_service: "SQLAnywhereService", sqlanywhere_client_libs: Path
) -> dict[str, Any]:
    """Return ``sqlanydb`` connection parameters for the SQL Anywhere service."""
    return {
        "uid": sqlanywhere_service.user,
        "pwd": sqlanywhere_service.password,
        "dbn": sqlanywhere_service.database,
        "host": f"{sqlanywhere_service.host}:{sqlanywhere_service.port}",
    }


@pytest.fixture
def sqlanywhere_config(sqlanywhere_connection_config: dict[str, Any]) -> Generator[SqlanywhereConfig, None, None]:
    """Yield a SQL Anywhere config whose pool is closed after the test."""
    config = SqlanywhereConfig(connection_config=dict(sqlanywhere_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def sqlanywhere_session(sqlanywhere_config: SqlanywhereConfig) -> Generator["SqlanywhereDriver", None, None]:
    """Yield a SQL Anywhere driver session."""
    with sqlanywhere_config.provide_session() as session:
        yield session
