"""Exception handling integration tests for bigquery adapter."""

from collections.abc import Generator

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery import BigQueryConfig, BigQueryDriver
from sqlspec.exceptions import NotFoundError, SQLParsingError, UniqueViolationError

pytestmark = [pytest.mark.xdist_group("bigquery"), pytest.mark.skip(reason="BigQuery emulator config missing")]


@pytest.fixture
def bigquery_exception_session(bigquery_service: BigQueryService) -> Generator[BigQueryDriver, None]:
    """Create a BigQuery session for exception testing."""
    table_prefix = f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"

    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": table_prefix,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )

    try:
        with config.provide_session() as session:
            yield session
    finally:
        config.close_pool()


def test_not_found_error(bigquery_exception_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test table not found raises NotFoundError."""
    table_prefix = f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"

    with pytest.raises(NotFoundError) as exc_info:
        bigquery_exception_session.execute(f"SELECT * FROM {table_prefix}.nonexistent_table_xyz_123")

    assert "not found" in str(exc_info.value).lower() or "404" in str(exc_info.value)


def test_sql_parsing_error(bigquery_exception_session: BigQueryDriver, bigquery_service: BigQueryService) -> None:
    """Test syntax error raises SQLParsingError."""
    table_prefix = f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"

    bigquery_exception_session.execute_script(f"""
        CREATE OR REPLACE TABLE {table_prefix}.test_syntax_table (
            id INT64,
            name STRING
        );
    """)

    with pytest.raises(SQLParsingError) as exc_info:
        bigquery_exception_session.execute(f"SELCT * FROM {table_prefix}.test_syntax_table")

    assert (
        "syntax" in str(exc_info.value).lower()
        or "invalid" in str(exc_info.value).lower()
        or "400" in str(exc_info.value)
    )

    bigquery_exception_session.execute(f"DROP TABLE IF EXISTS {table_prefix}.test_syntax_table")


def test_unique_violation_table_exists(
    bigquery_exception_session: BigQueryDriver, bigquery_service: BigQueryService
) -> None:
    """Test creating duplicate table raises UniqueViolationError."""
    table_prefix = f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"

    bigquery_exception_session.execute(f"""
        CREATE OR REPLACE TABLE {table_prefix}.test_duplicate_table (
            id INT64,
            name STRING
        )
    """)

    with pytest.raises(UniqueViolationError) as exc_info:
        bigquery_exception_session.execute(f"""
            CREATE TABLE {table_prefix}.test_duplicate_table (
                id INT64,
                value INT64
            )
        """)

    assert "already exists" in str(exc_info.value).lower() or "409" in str(exc_info.value)

    bigquery_exception_session.execute(f"DROP TABLE IF EXISTS {table_prefix}.test_duplicate_table")
