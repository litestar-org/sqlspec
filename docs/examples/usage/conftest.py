from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

pytest_plugins = [
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.bigquery",
]


@pytest.fixture(scope="session", autouse=True)
def usage_postgres_env(postgres_service: PostgresService) -> Generator[None, None, None]:
    """Expose Postgres connection settings via env vars for docs examples."""

    os.environ
    patcher = pytest.MonkeyPatch()
    dsn = (
        f"postgresql://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    patcher.setenv("SQLSPEC_USAGE_PG_DSN", dsn)
    patcher.setenv("SQLSPEC_USAGE_PG_HOST", postgres_service.host)
    patcher.setenv("SQLSPEC_USAGE_PG_PORT", str(postgres_service.port))
    patcher.setenv("SQLSPEC_USAGE_PG_USER", postgres_service.user)
    patcher.setenv("SQLSPEC_USAGE_PG_PASSWORD", postgres_service.password)
    patcher.setenv("SQLSPEC_USAGE_PG_DATABASE", postgres_service.database)
    yield
    patcher.undo()
