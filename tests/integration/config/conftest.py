"""Fixtures for connection_config integration tests."""

from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService


@pytest.fixture(scope="function")
def asyncpg_connection_config(postgres_service: "PostgresService") -> "dict[str, Any]":
    """Base pool configuration for AsyncPG tests."""
    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }
