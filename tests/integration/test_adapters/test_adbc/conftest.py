"""Test fixtures and configuration for ADBC integration tests."""

import functools
from collections.abc import Generator
from typing import Any, Callable, TypeVar, cast

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver

F = TypeVar("F", bound=Callable[..., Any])


def xfail_if_driver_missing(func: F) -> F:
    """Decorator to xfail a test if the ADBC driver shared object is missing."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if (
                "cannot open shared object file" in str(e)
                or "No module named" in str(e)
                or "Failed to import connect function" in str(e)
                or "Could not configure connection" in str(e)
            ):
                pytest.xfail(f"ADBC driver not available: {e}")
            raise e

    return cast("F", wrapper)


@pytest.fixture(scope="session")
def adbc_session(postgres_service: PostgresService) -> AdbcConfig:
    """Create an ADBC session for PostgreSQL."""
    return AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )


@pytest.fixture(scope="function")
def adbc_sync_driver(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC driver for data dictionary testing."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    with config.provide_session() as session:
        yield session
