from collections.abc import Generator
from typing import Any

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.adapters.bigquery.driver import BigQueryDriver

pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.bigquery",
]


@pytest.fixture
def table_schema_prefix(bigquery_service: "BigQueryService") -> str:
    """Create a table schema prefix."""
    return f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"


@pytest.fixture
def bigquery_config(bigquery_service: "BigQueryService", table_schema_prefix: str) -> BigQueryConfig:
    """Create a BigQuery config object."""
    return BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": table_schema_prefix,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )


@pytest.fixture
def bigquery_session(bigquery_config: BigQueryConfig) -> Generator[BigQueryDriver, Any, None]:
    """Create a BigQuery sync session."""

    with bigquery_config.provide_session() as session:
        yield session

@pytest.fixture
async def postgres_session_async(postgres_service: "PostgresService") -> Generator[Any, Any, None]:
    """Create a Postgres async session."""
    from sqlspec.adapters.asyncpg import AsyncpgConfig

    config = AsyncpgConfig(
        pool_config={
            "dsn": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 1,
            "max_size": 5,
        }
    )
    async with config.provide_session() as session:
        yield session
