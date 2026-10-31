"""Shared CockroachDB integration fixtures."""

from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from pytest_databases.docker.cockroachdb import CockroachDBService

    from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig
    from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
else:
    CockroachAsyncpgConfig = import_optional_attr("sqlspec.adapters.cockroach_asyncpg", "CockroachAsyncpgConfig")
    CockroachPsycopgAsyncConfig = import_optional_attr(
        "sqlspec.adapters.cockroach_psycopg", "CockroachPsycopgAsyncConfig"
    )
    CockroachPsycopgSyncConfig = import_optional_attr(
        "sqlspec.adapters.cockroach_psycopg", "CockroachPsycopgSyncConfig"
    )

__all__ = ("cockroach_asyncpg_config", "cockroach_psycopg_async_config", "cockroach_psycopg_sync_config")


@pytest.fixture(scope="session")
async def cockroach_asyncpg_config(
    cockroachdb_service: "CockroachDBService",
) -> "AsyncGenerator[CockroachAsyncpgConfig, None]":
    """Provide a session-scoped CockroachDB asyncpg configuration."""
    pytest.importorskip("asyncpg")
    connection_config = _cockroach_asyncpg_connection_config(cockroachdb_service)
    connection_config.update({"min_size": 1, "max_size": 5})
    config = CockroachAsyncpgConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture(scope="session")
async def cockroach_psycopg_async_config(
    cockroachdb_service: "CockroachDBService",
) -> "AsyncGenerator[CockroachPsycopgAsyncConfig, None]":
    """Provide a session-scoped CockroachDB psycopg async configuration."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = CockroachPsycopgAsyncConfig(
        connection_config={"conninfo": _cockroach_conninfo(cockroachdb_service), "min_size": 1, "max_size": 5}
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture(scope="session")
def cockroach_psycopg_sync_config(
    cockroachdb_service: "CockroachDBService",
) -> "Generator[CockroachPsycopgSyncConfig, None, None]":
    """Provide a session-scoped CockroachDB psycopg sync configuration."""
    pytest.importorskip("psycopg")
    pytest.importorskip("psycopg_pool")
    config = CockroachPsycopgSyncConfig(
        connection_config={"conninfo": _cockroach_conninfo(cockroachdb_service), "min_size": 1, "max_size": 5}
    )
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()


def _cockroach_conninfo(cockroachdb_service: "CockroachDBService") -> str:
    """Build a libpq conninfo string for a CockroachDB service container."""
    sslmode = cockroachdb_service.driver_opts.get("sslmode", "disable")
    return (
        f"host={cockroachdb_service.host} port={cockroachdb_service.port} "
        f"user=root dbname={cockroachdb_service.database} sslmode={sslmode}"
    )


def _cockroach_asyncpg_connection_config(cockroachdb_service: "CockroachDBService") -> dict[str, Any]:
    """Build asyncpg connection parameters for a CockroachDB service container."""
    sslmode = cockroachdb_service.driver_opts.get("sslmode", "disable")
    return {
        "host": cockroachdb_service.host,
        "port": cockroachdb_service.port,
        "user": "root",
        "password": "",
        "database": cockroachdb_service.database,
        "ssl": None if sslmode == "disable" else sslmode,
    }
