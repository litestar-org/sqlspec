"""Shared Spanner integration fixtures."""

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Any, cast

import pytest
from google.api_core import exceptions as api_exceptions
from google.auth.credentials import AnonymousCredentials, Credentials
from pytest_databases.docker.spanner import SpannerService

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig, SpannerSyncDriver

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

__all__ = (
    "build_spanner_connection_config",
    "drop_table_if_exists",
    "run_ddl",
    "spanner_async_config",
    "spanner_config",
    "spanner_database",
    "spanner_read_session",
    "spanner_session",
    "spanner_write_session",
)


def build_spanner_connection_config(spanner_service: "SpannerService") -> dict[str, Any]:
    """Build SQLSpec Spanner connection parameters targeting the Spanner emulator."""
    return {
        "project": spanner_service.project,
        "instance_id": spanner_service.instance_name,
        "database_id": spanner_service.database_name,
        "credentials": _anonymous_credentials(),
        "client_options": {"api_endpoint": f"{spanner_service.host}:{spanner_service.port}"},
        "size": 5,
    }


@pytest.fixture(scope="session")
def spanner_config(spanner_service: "SpannerService") -> Generator[SpannerSyncConfig, None, None]:
    """Create a session-scoped SpannerSyncConfig bound to the pre-bootstrapped emulator database."""
    config = SpannerSyncConfig(connection_config=build_spanner_connection_config(spanner_service))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
def spanner_database(spanner_config: SpannerSyncConfig) -> "Database":
    """Return the pre-bootstrapped Spanner emulator Database handle from SpannerSyncConfig."""
    return spanner_config.get_database()


@pytest.fixture
def spanner_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Provide a Spanner session registered through SQLSpec."""
    sql = SQLSpec()
    registered_config = sql.add_config(spanner_config)
    with sql.provide_session(registered_config) as session:
        yield session


@pytest.fixture
def spanner_write_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Provide a write-capable Spanner session."""
    with spanner_config.provide_write_session() as session:
        yield session


@pytest.fixture
def spanner_read_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Provide a read-only Spanner session."""
    with spanner_config.provide_read_session() as session:
        yield session


@pytest.fixture(scope="session")
async def spanner_async_config(spanner_service: "SpannerService") -> AsyncGenerator[SpannerAsyncConfig, None]:
    """Create an async Spanner configuration bound to the pre-bootstrapped emulator database."""
    config = SpannerAsyncConfig(connection_config=build_spanner_connection_config(spanner_service))
    try:
        yield config
    finally:
        await config.close_pool()


def run_ddl(database: "Database", statements: list[str], timeout: int = 300) -> None:
    """Execute DDL statements on a Spanner database."""
    operation = cast("Any", database).update_ddl(statements)
    operation.result(timeout)


def drop_table_if_exists(database: "Database", table_name: str) -> None:
    """Drop a table if it exists, ignoring API errors."""
    try:
        run_ddl(database, [f"DROP TABLE {table_name}"])
    except api_exceptions.GoogleAPICallError:
        pass


def _anonymous_credentials() -> Credentials:
    """Instantiate Google AnonymousCredentials for local Spanner emulator connections."""
    factory = cast("Any", AnonymousCredentials)
    return cast("Credentials", factory())
