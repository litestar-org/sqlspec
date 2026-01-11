"""BigQuery integration test fixtures."""

import contextlib
from collections.abc import Generator
from typing import TYPE_CHECKING, Any, cast

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials, Credentials

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.driver import BigQueryDriver

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService


@pytest.fixture(scope="session")
def table_schema_prefix(bigquery_service: "BigQueryService") -> str:
    """Create a table schema prefix."""
    return f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"


def _anonymous_credentials() -> "Credentials":
    """Create anonymous credentials for the emulator."""
    factory = cast("Any", AnonymousCredentials)
    return cast("Credentials", factory())


@pytest.fixture(scope="session")
def bigquery_config(bigquery_service: "BigQueryService", table_schema_prefix: str) -> "BigQueryConfig":
    """Create a BigQuery config object."""
    return BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": table_schema_prefix,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": _anonymous_credentials(),
        }
    )


@pytest.fixture
def bigquery_session(bigquery_config: "BigQueryConfig") -> "Generator[BigQueryDriver, Any, None]":
    """Create a BigQuery sync session."""

    with bigquery_config.provide_session() as session:
        yield session


@pytest.fixture
def bigquery_test_table(
    bigquery_session: "BigQueryDriver", bigquery_service: "BigQueryService"
) -> "Generator[str, None, None]":
    """Create and cleanup test table."""
    table_name = f"`{bigquery_service.project}.{bigquery_service.dataset}.test_table`"

    with contextlib.suppress(Exception):
        bigquery_session.execute_script(f"DROP TABLE {table_name}")

    try:
        bigquery_session.execute_script(f"""
            CREATE TABLE {table_name} (
                id INT64,
                name STRING NOT NULL,
                value INT64,
                created_at TIMESTAMP
            )
        """)

        yield table_name

    finally:
        with contextlib.suppress(Exception):
            bigquery_session.execute_script(f"DROP TABLE {table_name}")
