"""Live PostgreSQL checks that migration processes contend on one advisory lock."""

import hashlib
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.psycopg import PsycopgSyncConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = [pytest.mark.xdist_group("postgres"), pytest.mark.anyio]


def _connection_config(service: PostgresService) -> dict[str, Any]:
    return {
        "host": service.host,
        "port": service.port,
        "user": service.user,
        "password": service.password,
        "database": service.database,
    }


def _expected_lock_id(table: str) -> int:
    digest = hashlib.sha256(f"sqlspec_mig_{table}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _lock_is_free(service: PostgresService, lock_id: int) -> bool:
    config = PsycopgSyncConfig(connection_config=_connection_config(service))
    try:
        with config.provide_session() as session:
            acquired = bool(session.select_value("SELECT pg_try_advisory_lock(:lock_id)", {"lock_id": lock_id}))
            if acquired:
                session.execute("SELECT pg_advisory_unlock(:lock_id)", {"lock_id": lock_id})
            return acquired
    finally:
        config.close_pool()


def _drop_table(service: PostgresService, table: str) -> None:
    config = PsycopgSyncConfig(connection_config=_connection_config(service))
    try:
        with config.provide_session() as session:
            session.execute_script(f"DROP TABLE IF EXISTS {table}")
            session.commit()
    finally:
        config.close_pool()


def test_sync_processes_contend_on_one_advisory_lock(tmp_path: Path, postgres_service: PostgresService) -> None:
    table = f"ddl_lock_{uuid4().hex[:8]}"
    migration_config = {"script_location": str(tmp_path), "version_table_name": table, "enabled": True}
    holder_config = PsycopgSyncConfig(
        connection_config=_connection_config(postgres_service), migration_config=migration_config
    )
    waiter_config = PsycopgSyncConfig(
        connection_config=_connection_config(postgres_service), migration_config=migration_config
    )
    holder = SyncMigrationCommands(holder_config)
    waiter = SyncMigrationCommands(waiter_config)
    expected_id = _expected_lock_id(table)
    try:
        holder.upgrade()
        with holder_config.provide_session() as holder_session, holder.tracker.acquire_lock(holder_session):
            assert not _lock_is_free(postgres_service, expected_id)
            with waiter_config.provide_session() as waiter_session, pytest.raises(MigrationError):
                with waiter.tracker.acquire_lock(waiter_session, timeout_seconds=1):
                    pass
        with waiter_config.provide_session() as waiter_session, waiter.tracker.acquire_lock(waiter_session):
            assert not _lock_is_free(postgres_service, expected_id)
        assert _lock_is_free(postgres_service, expected_id)
    finally:
        holder_config.close_pool()
        waiter_config.close_pool()
        _drop_table(postgres_service, table)


async def test_async_processes_contend_on_one_advisory_lock(tmp_path: Path, postgres_service: PostgresService) -> None:
    table = f"ddl_lock_{uuid4().hex[:8]}"
    migration_config = {"script_location": str(tmp_path), "version_table_name": table, "enabled": True}
    holder_config = AsyncpgConfig(
        connection_config=_connection_config(postgres_service), migration_config=migration_config
    )
    waiter_config = AsyncpgConfig(
        connection_config=_connection_config(postgres_service), migration_config=migration_config
    )
    holder = AsyncMigrationCommands(holder_config)
    waiter = AsyncMigrationCommands(waiter_config)
    expected_id = _expected_lock_id(table)
    try:
        await holder.upgrade()
        async with holder_config.provide_session() as holder_session, holder.tracker.acquire_lock(holder_session):
            assert not _lock_is_free(postgres_service, expected_id)
            async with waiter_config.provide_session() as waiter_session:
                with pytest.raises(MigrationError):
                    async with waiter.tracker.acquire_lock(waiter_session, timeout_seconds=1):
                        pass
        async with waiter_config.provide_session() as waiter_session, waiter.tracker.acquire_lock(waiter_session):
            assert not _lock_is_free(postgres_service, expected_id)
        assert _lock_is_free(postgres_service, expected_id)
    finally:
        await holder_config.close_pool()
        await waiter_config.close_pool()
        _drop_table(postgres_service, table)
