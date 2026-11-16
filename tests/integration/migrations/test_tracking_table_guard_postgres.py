"""Live PostgreSQL checks for the tracking-table ownership guard and schema pinning."""

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

TEMP_SHADOW_MIGRATION = '''"""Create a temp table that shadows the tracking table."""


def up(context=None):
    return ["CREATE TEMP TABLE ddl_guard_shadow (id int)"]


def down(context=None):
    return ["DROP TABLE IF EXISTS ddl_guard_shadow"]
'''


def _connection_config(service: PostgresService, user: str, password: str) -> dict[str, Any]:
    return {
        "host": service.host,
        "port": service.port,
        "user": user,
        "password": password,
        "database": service.database,
    }


def _admin_statements(service: PostgresService, statements: list[str]) -> None:
    config = PsycopgSyncConfig(connection_config=_connection_config(service, service.user, service.password))
    try:
        with config.provide_session() as session:
            for statement in statements:
                session.execute_script(statement)
            session.commit()
    finally:
        config.close_pool()


def _build(
    mode: str, service: PostgresService, user: str, password: str, migrations_dir: Path, table: str
) -> tuple[Any, Any]:
    migration_config = {"script_location": str(migrations_dir), "version_table_name": table, "enabled": True}
    connection_config = _connection_config(service, user, password)
    if mode == "asyncpg":
        config = AsyncpgConfig(connection_config=connection_config, migration_config=migration_config)
        return config, AsyncMigrationCommands(config)
    config = PsycopgSyncConfig(connection_config=connection_config, migration_config=migration_config)
    return config, SyncMigrationCommands(config)


async def _upgrade(commands: Any) -> None:
    result = commands.upgrade(echo=False)
    if hasattr(result, "__await__"):
        await result


async def _close(config: Any) -> None:
    result = config.close_pool()
    if hasattr(result, "__await__"):
        await result


@pytest.mark.parametrize("mode", ["asyncpg", "psycopg"])
async def test_upgrade_refuses_tracking_table_owned_by_foreign_role(
    tmp_path: Path, postgres_service: PostgresService, mode: str
) -> None:
    suffix = uuid4().hex[:8]
    migrator, owner, table = f"guard_migrator_{suffix}", f"guard_owner_{suffix}", f"ddl_guard_{suffix}"
    _admin_statements(
        postgres_service,
        [
            f"CREATE ROLE {migrator} LOGIN PASSWORD 'pw' NOSUPERUSER",
            f"CREATE ROLE {owner} NOLOGIN NOSUPERUSER",
            f"GRANT CREATE, USAGE ON SCHEMA public TO {migrator}, {owner}",
            f"CREATE TABLE public.{table} (version_num VARCHAR(32) PRIMARY KEY)",
            f"ALTER TABLE public.{table} OWNER TO {owner}",
            f"GRANT ALL ON public.{table} TO {migrator}",
        ],
    )
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_noop.py").write_text(TEMP_SHADOW_MIGRATION, encoding="utf-8")
    config, commands = _build(mode, postgres_service, migrator, "pw", migrations_dir, table)
    try:
        with pytest.raises(MigrationError, match="owned by the current role"):
            await _upgrade(commands)
    finally:
        await _close(config)
        _admin_statements(
            postgres_service,
            [
                f"DROP TABLE IF EXISTS public.{table}",
                f"DROP OWNED BY {migrator}",
                f"DROP OWNED BY {owner}",
                f"DROP ROLE {migrator}",
                f"DROP ROLE {owner}",
            ],
        )


@pytest.mark.parametrize("mode", ["asyncpg", "psycopg"])
async def test_temp_table_does_not_redirect_tracking_writes(
    tmp_path: Path, postgres_service: PostgresService, mode: str
) -> None:
    table = f"ddl_guard_{uuid4().hex[:8]}"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_shadow.py").write_text(
        TEMP_SHADOW_MIGRATION.replace("ddl_guard_shadow", table), encoding="utf-8"
    )
    config, commands = _build(
        mode, postgres_service, postgres_service.user, postgres_service.password, migrations_dir, table
    )
    try:
        await _upgrade(commands)
        verifier = PsycopgSyncConfig(
            connection_config=_connection_config(postgres_service, postgres_service.user, postgres_service.password)
        )
        try:
            with verifier.provide_session() as session:
                rows = session.select(f"SELECT version_num FROM public.{table}")
        finally:
            verifier.close_pool()
        assert [row["version_num"] for row in rows] == ["0001"]
    finally:
        await _close(config)
        _admin_statements(postgres_service, [f"DROP TABLE IF EXISTS public.{table}"])
