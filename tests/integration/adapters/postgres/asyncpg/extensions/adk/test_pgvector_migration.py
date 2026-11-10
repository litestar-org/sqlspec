"""Fresh-database regression for the packaged ADK pgvector prerequisite."""

import importlib
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import asyncpg
import pytest
from pytest_databases.docker.postgres import PostgresService, exec_psql

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKMemoryStore
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands
from sqlspec.migrations.context import MigrationContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from sqlspec.extensions.adk import StoredMemory

pytestmark = [pytest.mark.xdist_group("pgvector"), pytest.mark.asyncpg, pytest.mark.integration]

migration = importlib.import_module("sqlspec.extensions.adk.migrations.0001_create_adk_tables")

VECTOR_EXTENSION_COUNT_SQL = "SELECT count(*) FROM pg_extension WHERE extname = 'vector'"
EMBEDDING_TYPE_SQL = (
    "SELECT udt_name FROM information_schema.columns WHERE table_name = 'adk_memory' AND column_name = 'embedding'"
)


@pytest.fixture
async def fresh_database(pgvector_service: PostgresService) -> "AsyncGenerator[str, None]":
    """Create and drop a uniquely named database without the vector extension."""
    database = f"adk_pgvector_{uuid.uuid4().hex}"
    exit_code, output = exec_psql(
        pgvector_service.container,
        user=pgvector_service.user,
        password=pgvector_service.password,
        database="postgres",
        sql=f'CREATE DATABASE "{database}"',
    )
    if exit_code != 0:
        decoded_output = output.decode("utf-8", errors="replace")
        msg = f"Failed to create database {database!r}: {decoded_output}"
        raise RuntimeError(msg)
    try:
        yield database
    finally:
        pgvector_service.drop_database_force(database)


async def test_memory_ddl_without_prerequisite_is_rejected(
    pgvector_service: PostgresService, fresh_database: str
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
    pgvector_service: PostgresService, fresh_database: str, tmp_path: Path
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
    pgvector_service: PostgresService, fresh_database: str, tmp_path: Path
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


async def _connect(service: PostgresService, database: str) -> "asyncpg.Connection[Any]":
    return await asyncpg.connect(
        host=service.host, port=service.port, user=service.user, password=service.password, database=database
    )


def _make_config(
    service: PostgresService,
    database: str,
    migrations_path: Path | None = None,
    adk_settings: dict[str, Any] | None = None,
    migration_settings: dict[str, Any] | None = None,
) -> AsyncpgConfig:
    migration_config: dict[str, Any] = {}
    if migrations_path is not None:
        migration_config = {
            "script_location": str(migrations_path),
            "include_extensions": ["adk"],
            "version_table_name": "sqlspec_migrations_adk_pgvector",
        }
    if migration_settings:
        migration_config.update(migration_settings)
    adk_config: dict[str, Any] = {"enable_memory": True}
    if adk_settings:
        adk_config.update(adk_settings)
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
        migration_config=cast("Any", migration_config),
        extension_config=cast("Any", {"adk": adk_config}),
    )


def _memory_entry(entry_id: str, embedding: list[float]) -> "StoredMemory":
    now = datetime.now(UTC)
    return cast(
        "StoredMemory",
        {
            "id": entry_id,
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "u1",
            "scope": "user",
            "event_id": f"evt-{entry_id}",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "alpha memory"},
            "content_text": "alpha memory",
            "metadata_json": None,
            "embedding": embedding,
            "inserted_at": now,
        },
    )


async def test_reconcile_upgrade_installs_vector_searches_on_same_config_and_keeps_rows(
    pgvector_service: PostgresService, fresh_database: str, tmp_path: Path
) -> None:
    """Reconcile upgrade installs vector, enables immediate vector search on the same config, and writes no ledger rows."""
    migrations_path = tmp_path / "migrations"
    migrations_path.mkdir()
    entry_id = f"mem-{uuid.uuid4().hex}"
    config = _make_config(
        pgvector_service,
        fresh_database,
        migrations_path,
        adk_settings={"migration_mode": "reconcile", "vector_dimensions": 3},
    )
    try:
        commands = AsyncMigrationCommands(config)
        await commands.upgrade()

        store = AsyncpgADKMemoryStore(config)
        inserted = await store.insert_memory_entries([_memory_entry(entry_id, [1.0, 0.0, 0.0])])
        assert inserted == 1

        matches = await store.search_entries(query="alpha", app_name="app", user_id="u1", embedding=[1.0, 0.0, 0.0])
        assert len(matches) == 1
        assert matches[0]["id"] == entry_id

        await commands.upgrade()
    finally:
        await config.close_pool()

    connection = await _connect(pgvector_service, fresh_database)
    try:
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == 1
        assert await connection.fetchval(EMBEDDING_TYPE_SQL) == "vector"
        assert await connection.fetchval("SELECT count(*) FROM adk_memory") == 1
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM sqlspec_migrations_adk_pgvector WHERE version_num LIKE 'ext_adk_%'"
            )
            == 0
        )
    finally:
        await connection.close()


async def test_reconcile_upgrade_recreates_missing_indexes_once(
    pgvector_service: PostgresService, fresh_database: str, tmp_path: Path
) -> None:
    """Reconcile upgrade recreates dropped companion indexes once and preserves OIDs on repeat runs."""
    migrations_path = tmp_path / "migrations"
    migrations_path.mkdir()
    config = _make_config(
        pgvector_service,
        fresh_database,
        migrations_path,
        adk_settings={"migration_mode": "reconcile", "vector_dimensions": 3},
    )
    try:
        commands = AsyncMigrationCommands(config)
        await commands.upgrade()

        store = AsyncpgADKMemoryStore(config)
        await store.insert_memory_entries([_memory_entry("mem-repair-1", [0.5, 0.5, 0.0])])

        connection = await _connect(pgvector_service, fresh_database)
        try:
            await connection.execute("DROP INDEX idx_adk_memory_session")
            await connection.execute("DROP INDEX idx_adk_session_state")
        finally:
            await connection.close()

        await commands.upgrade()

        connection = await _connect(pgvector_service, fresh_database)
        try:
            present = await connection.fetch(
                "SELECT indexname FROM pg_indexes WHERE indexname IN ('idx_adk_memory_session', 'idx_adk_session_state')"
            )
            assert {row["indexname"] for row in present} == {"idx_adk_memory_session", "idx_adk_session_state"}
            repaired_oid = await connection.fetchval(
                "SELECT oid FROM pg_class WHERE relname = 'idx_adk_memory_session'"
            )
            assert repaired_oid is not None
        finally:
            await connection.close()

        await commands.upgrade()

        connection = await _connect(pgvector_service, fresh_database)
        try:
            repeat_oid = await connection.fetchval("SELECT oid FROM pg_class WHERE relname = 'idx_adk_memory_session'")
            assert repeat_oid == repaired_oid
            assert await connection.fetchval("SELECT count(*) FROM adk_memory") == 1
        finally:
            await connection.close()
    finally:
        await config.close_pool()


@pytest.mark.parametrize(
    ("migration_settings", "expected_vector_count"),
    [
        pytest.param({}, 0, id="transactional-default"),
        pytest.param({"transactional": False}, 1, id="non-transactional"),
    ],
)
async def test_reconcile_strict_refusal_after_prerequisite(
    pgvector_service: PostgresService,
    fresh_database: str,
    tmp_path: Path,
    migration_settings: dict[str, Any],
    expected_vector_count: int,
) -> None:
    """Strict target refusal rolls back vector in transactional mode and never refreshes config extension state."""
    connection = await _connect(pgvector_service, fresh_database)
    try:
        await connection.execute("CREATE TABLE adk_memory (id VARCHAR(128) PRIMARY KEY)")
    finally:
        await connection.close()

    migrations_path = tmp_path / "migrations"
    migrations_path.mkdir()
    config = _make_config(
        pgvector_service,
        fresh_database,
        migrations_path,
        adk_settings={"migration_mode": "reconcile", "enable_sessions": False, "vector_dimensions": 3},
        migration_settings=migration_settings,
    )
    try:
        with pytest.raises(MigrationError):
            await AsyncMigrationCommands(config).upgrade()
        assert config._pgvector_available is False
        assert config.statement_config.dialect == "postgres"
        assert "vector" not in config.driver_features.get("active_extensions", set())
    finally:
        await config.close_pool()

    connection = await _connect(pgvector_service, fresh_database)
    try:
        assert await connection.fetchval(VECTOR_EXTENSION_COUNT_SQL) == expected_vector_count
        columns = await connection.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'adk_memory' ORDER BY ordinal_position"
        )
        assert [row["column_name"] for row in columns] == ["id"]
    finally:
        await connection.close()
