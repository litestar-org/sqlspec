"""Shared Spanner integration fixtures."""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import spanner
from pytest_databases.docker.spanner import SpannerService

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database

__all__ = ("spanner_config", "spanner_database", "spanner_read_session", "spanner_session", "spanner_write_session")


def _spanner_connection_config(spanner_service: "SpannerService") -> "dict[str, Any]":
    return {
        "project": spanner_service.project,
        "instance_id": spanner_service.instance_name,
        "database_id": spanner_service.database_name,
        "credentials": spanner_service.credentials,
        "client_options": {"api_endpoint": f"{spanner_service.host}:{spanner_service.port}"},
        "size": 5,
    }


@pytest.fixture(scope="session")
def spanner_database(
    spanner_service: "SpannerService", spanner_connection: "spanner.Client"
) -> "Generator[Database, None, None]":
    """Ensure the emulator instance and database exist."""
    instance = spanner_connection.instance(spanner_service.instance_name)  # type: ignore[no-untyped-call]
    if not instance.exists():
        config_name = f"{spanner_connection.project_name}/instanceConfigs/emulator-config"
        instance = spanner_connection.instance(  # type: ignore[no-untyped-call]
            spanner_service.instance_name, configuration_name=config_name
        )
        instance.create().result(300)

    database = instance.database(spanner_service.database_name)
    if not database.exists():
        database.create().result(300)
    yield database


@pytest.fixture(scope="session")
def spanner_config(
    spanner_service: "SpannerService", spanner_connection: "spanner.Client", spanner_database: "Database"
) -> "Generator[SpannerSyncConfig, None, None]":
    """Create a Spanner configuration after ensuring the database exists."""
    config = SpannerSyncConfig(connection_config=_spanner_connection_config(spanner_service))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def spanner_session(spanner_config: "SpannerSyncConfig") -> "Generator[SpannerSyncDriver, None, None]":
    """Provide a read-only Spanner session."""
    sql = SQLSpec()
    registered_config = sql.add_config(spanner_config)
    with sql.provide_session(registered_config) as session:
        yield session


@pytest.fixture
def spanner_write_session(spanner_config: "SpannerSyncConfig") -> "Generator[SpannerSyncDriver, None, None]":
    """Provide a write-capable Spanner session."""
    with spanner_config.provide_write_session() as session:
        yield session


@pytest.fixture
def spanner_read_session(spanner_config: "SpannerSyncConfig") -> "Generator[SpannerSyncDriver, None, None]":
    """Provide a read-only Spanner session."""
    with spanner_config.provide_read_session() as session:
        yield session


def run_ddl(database: "Database", statements: "list[str]", timeout: int = 300) -> None:
    """Execute DDL statements on a Spanner database."""
    operation = cast("Any", database).update_ddl(statements)
    operation.result(timeout)


def drop_table_if_exists(database: "Database", table_name: str) -> None:
    """Drop a table if it exists, ignoring API errors."""
    try:
        run_ddl(database, [f"DROP TABLE {table_name}"])
    except api_exceptions.GoogleAPICallError:
        pass
