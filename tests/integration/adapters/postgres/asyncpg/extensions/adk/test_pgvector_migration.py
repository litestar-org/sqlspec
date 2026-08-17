"""Fresh-database regression for the packaged ADK pgvector prerequisite."""

import importlib
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.migrations.commands import AsyncMigrationCommands
from sqlspec.migrations.context import MigrationContext

pytestmark = [pytest.mark.xdist_group("pgvector"), pytest.mark.asyncpg, pytest.mark.integration]

migration = importlib.import_module("sqlspec.extensions.adk.migrations.0001_create_adk_tables")

VECTOR_EXTENSION_COUNT_SQL = "SELECT count(*) FROM pg_extension WHERE extname = 'vector'"
EMBEDDING_TYPE_SQL = (
    "SELECT udt_name FROM information_schema.columns WHERE table_name = 'adk_memory' AND column_name = 'embedding'"
)
TERMINATE_SESSIONS_SQL = (
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()"
)


async def _connect(service: "PostgresService", database: str) -> "asyncpg.Connection[Any]":
    return await asyncpg.connect(
        host=service.host, port=service.port, user=service.user, password=service.password, database=database
    )


def _make_config(service: "PostgresService", database: str, migrations_path: "Path | None" = None) -> AsyncpgConfig:
    migration_config: dict[str, Any] = {}
    if migrations_path is not None:
        migration_config = {
            "script_location": str(migrations_path),
            "include_extensions": ["adk"],
            "version_table_name": "sqlspec_migrations_adk_pgvector",
        }
    return AsyncpgConfig(
        connection_config={
            "host": service.host,
            "port": service.port,
            "user": service.user,
            "password": service.password,
            "database": database,
            "min_size": 1,
            "max_size": 2,
        },
        migration_config=migration_config,
        extension_config={"adk": {"enable_memory": True}},
    )


@pytest.fixture
async def fresh_database(pgvector_service: "PostgresService") -> "AsyncGenerator[str, None]":
    """Create and drop a uniquely named database without the vector extension."""
    database = f"adk_pgvector_{uuid.uuid4().hex}"
    admin = await _connect(pgvector_service, pgvector_service.database)
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()
    try:
        yield database
    finally:
        admin = await _connect(pgvector_service, pgvector_service.database)
        try:
            await admin.execute(TERMINATE_SESSIONS_SQL, database)
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}"')
        finally:
            await admin.close()


async def test_memory_ddl_without_prerequisite_is_rejected(
    pgvector_service: "PostgresService", fresh_database: str
) -> None:
    """Dropping the leading extension statement makes the first vector column fail."""
    context = MigrationContext(config=_make_config(pgvector_service, fresh_database), dialect="postgres")
    statements = await migration.up(context)
    vector_statement = next(sql for sql in statements if "embedding VECTOR(" in sql)

    connection = await _connect(pgvector_service, fresh_database)
    try:
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == 0
        with pytest.raises(asyncpg.PostgresError) as failure:
            await connection.execute(vector_statement)
        assert 'type "vector" does not exist' in str(failure.value)
        assert await connection.fetchval("SELECT 1") == 1
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == 0
    finally:
        await connection.close()


async def test_packaged_migration_installs_vector_on_fresh_database(
    pgvector_service: "PostgresService", fresh_database: str, tmp_path: Path
) -> None:
    """Packaged migration 0001 provisions vector and creates a usable embedding column."""
    connection = await _connect(pgvector_service, fresh_database)
    try:
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == 0
    finally:
        await connection.close()

    migrations_path = tmp_path / "migrations"
    migrations_path.mkdir()
    config = _make_config(pgvector_service, fresh_database, migrations_path)
    try:
        await AsyncMigrationCommands(config).upgrade()
    finally:
        await config.close_pool()

    connection = await _connect(pgvector_service, fresh_database)
    try:
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == 1
        assert await connection.fetchval(EMBEDDING_TYPE_SQL) == "vector"
    finally:
        await connection.close()


async def test_packaged_migration_reruns_when_vector_already_exists(
    pgvector_service: "PostgresService", fresh_database: str, tmp_path: Path
) -> None:
    """Re-running migration 0001 against an existing vector installation succeeds."""
    migrations_path = tmp_path / "migrations"
    migrations_path.mkdir()
    config = _make_config(pgvector_service, fresh_database, migrations_path)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.upgrade()
        await commands.downgrade("base")
        await commands.upgrade()
    finally:
        await config.close_pool()

    connection = await _connect(pgvector_service, fresh_database)
    try:
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == 1
        assert await connection.fetchval(EMBEDDING_TYPE_SQL) == "vector"
    finally:
        await connection.close()
