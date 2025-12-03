from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import spanner
from pytest_databases.docker.spanner import SpannerService

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database


pytestmark = pytest.mark.xdist_group("spanner")


@pytest.fixture(scope="session")
def spanner_database(
    spanner_service: SpannerService, spanner_connection: spanner.Client
) -> Generator["Database", None, None]:
    """Ensure emulator instance and database exist, yield Database."""
    instance = spanner_connection.instance(spanner_service.instance_name)
    if not instance.exists():
        config_name = f"{spanner_connection.project_name}/instanceConfigs/emulator-config"
        instance = spanner_connection.instance(spanner_service.instance_name, configuration_name=config_name)
        instance.create().result(300)

    database = instance.database(spanner_service.database_name)
    if not database.exists():
        database.create().result(300)

    yield database


@pytest.fixture
def spanner_config(
    spanner_service: SpannerService, spanner_connection: spanner.Client, spanner_database: "Database"
) -> SpannerSyncConfig:
    """Create SpannerSyncConfig after ensuring database exists."""
    _ = spanner_database  # Ensure database is created before config
    api_endpoint = f"{spanner_service.host}:{spanner_service.port}"

    return SpannerSyncConfig(
        pool_config={
            "project": spanner_service.project,
            "instance_id": spanner_service.instance_name,
            "database_id": spanner_service.database_name,
            "credentials": spanner_service.credentials,
            "client_options": {"api_endpoint": api_endpoint},
            "min_sessions": 1,
            "max_sessions": 5,
        }
    )


@pytest.fixture
def spanner_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Read-only session for SELECT operations."""
    sql = SQLSpec()
    c = sql.add_config(spanner_config)
    with sql.provide_session(c) as session:
        yield session


@pytest.fixture
def spanner_write_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Write-capable session for DML operations (INSERT/UPDATE/DELETE)."""
    with spanner_config.provide_write_session() as session:
        yield session


@pytest.fixture
def spanner_read_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    """Read-only session for SELECT operations."""
    with spanner_config.provide_session() as session:
        yield session


def run_ddl(database: "Database", statements: "list[str]", timeout: int = 300) -> None:
    """Execute DDL statements on Spanner database."""
    operation = database.update_ddl(statements)  # type: ignore[no-untyped-call]
    operation.result(timeout)


def drop_table_if_exists(database: "Database", table_name: str) -> None:
    """Drop a table if it exists, ignoring errors."""
    try:
        run_ddl(database, [f"DROP TABLE {table_name}"])
    except api_exceptions.GoogleAPICallError:
        pass


@pytest.fixture
def test_users_table(spanner_database: "Database") -> Generator[str, None, None]:
    """Create test_users table for CRUD tests."""
    table_name = "test_users"
    drop_table_if_exists(spanner_database, table_name)

    ddl = f"""
    CREATE TABLE {table_name} (
        id STRING(36) NOT NULL,
        name STRING(100),
        email STRING(255),
        age INT64
    ) PRIMARY KEY (id)
    """
    run_ddl(spanner_database, [ddl])

    yield table_name

    drop_table_if_exists(spanner_database, table_name)


@pytest.fixture
def test_arrow_table(spanner_database: "Database") -> Generator[str, None, None]:
    """Create test table for Arrow tests."""
    table_name = "test_arrow_data"
    drop_table_if_exists(spanner_database, table_name)

    ddl = f"""
    CREATE TABLE {table_name} (
        id INT64 NOT NULL,
        name STRING(100),
        value INT64
    ) PRIMARY KEY (id)
    """
    run_ddl(spanner_database, [ddl])

    yield table_name

    drop_table_if_exists(spanner_database, table_name)
