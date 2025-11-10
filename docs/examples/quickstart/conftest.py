from __future__ import annotations

from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

pytest_plugins = ["pytest_databases.docker.postgres"]


@pytest.fixture(scope="session", autouse=True)
def quickstart_postgres_env(postgres_service: PostgresService) -> Generator[None, None, None]:
    """Expose pytest-databases Postgres settings to docs quickstart examples."""

    patcher = pytest.MonkeyPatch()
    patcher.setenv("SQLSPEC_QUICKSTART_PG_HOST", postgres_service.host)
    patcher.setenv("SQLSPEC_QUICKSTART_PG_PORT", str(postgres_service.port))
    patcher.setenv("SQLSPEC_QUICKSTART_PG_USER", postgres_service.user)
    patcher.setenv("SQLSPEC_QUICKSTART_PG_PASSWORD", postgres_service.password)
    patcher.setenv("SQLSPEC_QUICKSTART_PG_DATABASE", postgres_service.database)
    yield
    patcher.undo()
