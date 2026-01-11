"""Shared fixtures for DuckDB integration tests."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver


@pytest.fixture
def duckdb_basic_config() -> Generator[DuckDBConfig, None, None]:
    """Provide an in-memory DuckDB configuration."""

    config = DuckDBConfig(connection_config={"database": ":memory:"})
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def duckdb_basic_session(duckdb_basic_config: DuckDBConfig) -> Generator[DuckDBDriver, None, None]:
    """Yield a basic DuckDB session for tests requiring a clean database."""

    with duckdb_basic_config.provide_session() as session:
        yield session
