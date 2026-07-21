"""Behavior helpers for shared migration-lifecycle contract tests."""

import contextlib
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.utils.text import quote_identifier
from tests.integration.adapters._shared._migration_cases import MigrationCase


def _migration_paths(case: MigrationCase, behavior: str, tmp_path: Path) -> "tuple[str, str, str, str]":
    if case.uses_oracle_ddl:
        token = f"ora_{case.mode[0]}_{behavior[:5]}_{uuid4().hex[:4]}"
        script_location = str(tmp_path / f"mig_{token}")
        version_table = f"dm_{token}".upper()
        table = f"mu_{token}".upper()
        return token, script_location, version_table, table
    token = f"{case.id}_{behavior}".replace("-", "_")
    script_location = str(tmp_path / f"mig_{token}")
    version_table = f"ddl_mig_{token}"
    table = f"mig_users_{token}"
    return token, script_location, version_table, table


def _create_table_migration(case: MigrationCase, table: str) -> str:
    if case.uses_oracle_ddl:
        create_sql = f"CREATE TABLE {table} (id NUMBER, name VARCHAR2(255) NOT NULL)"
        drop_sql = f"DROP TABLE {table}"
    else:
        create_sql = f"CREATE TABLE {table} (id INTEGER, name VARCHAR(255) NOT NULL)"
        drop_sql = f"DROP TABLE IF EXISTS {table}"
    return f'''"""Create {table}."""


def up():
    """Create the table."""
    return ["{create_sql}"]


def down():
    """Drop the table."""
    return ["{drop_sql}"]
'''


def _seeded_table_migration(case: MigrationCase, table: str) -> str:
    if case.uses_oracle_ddl:
        create_sql = f"CREATE TABLE {table} (id NUMBER, name VARCHAR2(255) NOT NULL)"
        drop_sql = f"DROP TABLE {table}"
    else:
        create_sql = f"CREATE TABLE {table} (id INTEGER, name VARCHAR(255) NOT NULL)"
        drop_sql = f"DROP TABLE IF EXISTS {table}"
    return f'''"""Create {table} with seed rows."""


def up():
    """Create the table and seed two rows."""
    return [
        "{create_sql}",
        "INSERT INTO {table} (id, name) VALUES (1, 'first')",
        "INSERT INTO {table} (id, name) VALUES (2, 'second')",
    ]


def down():
    """Drop the table."""
    return ["{drop_sql}"]
'''


def _bad_migration() -> str:
    return '''"""Migration with invalid SQL."""


def up():
    """Invalid SQL that fails to apply."""
    return ["CREATE INVALID SQL STATEMENT"]


def down():
    """No-op downgrade."""
    return []
'''


def _build(make_config: Any, script_location: str, version_table: str, suffix: str) -> Any:
    return make_config(script_location=script_location, version_table_name=version_table, suffix=suffix)


def _build_with_schema(
    make_config: Any,
    script_location: str,
    version_table: str,
    suffix: str,
    *,
    default_schema: str | None = None,
    version_table_schema: str | None = None,
) -> Any:
    return make_config(
        script_location=script_location,
        version_table_name=version_table,
        suffix=suffix,
        default_schema=default_schema,
        version_table_schema=version_table_schema,
    )


def _write_migration(script_location: str, filename: str, content: str) -> None:
    (Path(script_location) / filename).write_text(content)


def _write_non_transactional_sql_migration(script_location: str, table: str) -> None:
    (Path(script_location) / "0001_create_unqualified_table.sql").write_text(f"""-- transactional: false
-- name: migrate-0001-up
CREATE TABLE {table} (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE IF EXISTS {table};""")


def _schema_identifier(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:10]}"


def _create_schema_sql(schema: str) -> str:
    return f"CREATE SCHEMA {quote_identifier(schema)}"


def _drop_schema_sql(schema: str) -> str:
    return f"DROP SCHEMA IF EXISTS {quote_identifier(schema)} CASCADE"


def _default_schema_name(case: MigrationCase) -> str:
    return "main" if case.schema_dialect == "duckdb" else "public"


def _table_exists_in_schema_sync(driver: Any, schema: str, table: str) -> bool:
    result = driver.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table",
        schema=schema,
        table=table,
    )
    return bool(result.data)


async def _table_exists_in_schema_async(driver: Any, schema: str, table: str) -> bool:
    result = await driver.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table",
        schema=schema,
        table=table,
    )
    return bool(result.data)


def _table_exists_sync(config: Any, table: str) -> bool:
    with config.provide_session() as driver:
        try:
            driver.execute(f"SELECT 1 FROM {table} WHERE 1 = 0")
        except Exception:
            return False
        return True


async def _table_exists_async(config: Any, table: str) -> bool:
    async with config.provide_session() as driver:
        try:
            await driver.execute(f"SELECT 1 FROM {table} WHERE 1 = 0")
        except Exception:
            return False
        return True


def _recorded_count_sync(config: Any, version_table: str) -> int:
    with config.provide_session() as driver:
        try:
            return int(driver.select_value(f"SELECT COUNT(*) FROM {version_table}"))
        except Exception:
            return 0


async def _recorded_count_async(config: Any, version_table: str) -> int:
    async with config.provide_session() as driver:
        try:
            return int(await driver.select_value(f"SELECT COUNT(*) FROM {version_table}"))
        except Exception:
            return 0


def assert_sync_migration_full_workflow_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations init, upgrade to create a table, then downgrade to remove it."""
    token, script_location, version_table, table = _migration_paths(case, "full", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))

        commands.upgrade()
        assert _table_exists_sync(config, table)
        with config.provide_session() as driver:
            driver.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'alpha')")
            rows = driver.execute(f"SELECT name FROM {table}")
            assert rows.get_data()[0]["name"] == "alpha"

        commands.downgrade("base")
        assert not _table_exists_sync(config, table)
    finally:
        config.close_pool()


async def assert_async_migration_full_workflow_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async migrations init, upgrade to create a table, then downgrade to remove it."""
    token, script_location, version_table, table = _migration_paths(case, "full", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))

        await commands.upgrade()
        assert await _table_exists_async(config, table)
        async with config.provide_session() as driver:
            await driver.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'alpha')")
            rows = await driver.execute(f"SELECT name FROM {table}")
            assert rows.get_data()[0]["name"] == "alpha"

        await commands.downgrade("base")
        assert not await _table_exists_async(config, table)
    finally:
        await config.close_pool()


def assert_sync_migration_multiple_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations apply two revisions, downgrade one, then downgrade all."""
    token, script_location, version_table, table = _migration_paths(case, "multi", tmp_path)
    second_table = f"{table}_b"
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_first.py", _create_table_migration(case, table))
        _write_migration(script_location, "0002_second.py", _create_table_migration(case, second_table))

        commands.upgrade()
        assert _table_exists_sync(config, table)
        assert _table_exists_sync(config, second_table)

        commands.downgrade("0001")
        assert _table_exists_sync(config, table)
        assert not _table_exists_sync(config, second_table)

        commands.downgrade("base")
        assert not _table_exists_sync(config, table)
        assert not _table_exists_sync(config, second_table)
    finally:
        config.close_pool()


async def assert_async_migration_multiple_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async migrations apply two revisions, downgrade one, then downgrade all."""
    token, script_location, version_table, table = _migration_paths(case, "multi", tmp_path)
    second_table = f"{table}_b"
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_first.py", _create_table_migration(case, table))
        _write_migration(script_location, "0002_second.py", _create_table_migration(case, second_table))

        await commands.upgrade()
        assert await _table_exists_async(config, table)
        assert await _table_exists_async(config, second_table)

        await commands.downgrade("0001")
        assert await _table_exists_async(config, table)
        assert not await _table_exists_async(config, second_table)

        await commands.downgrade("base")
        assert not await _table_exists_async(config, table)
        assert not await _table_exists_async(config, second_table)
    finally:
        await config.close_pool()


def assert_sync_migration_current_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync current command reports base, then the applied revision, then base again."""
    token, script_location, version_table, table = _migration_paths(case, "current", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        assert commands.current() in (None, "base")

        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        commands.upgrade()
        assert commands.current() == "0001"

        commands.downgrade("base")
        assert commands.current() in (None, "base")
    finally:
        config.close_pool()


async def assert_async_migration_current_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async current command reports base, then the applied revision, then base again."""
    token, script_location, version_table, table = _migration_paths(case, "current", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        assert await commands.current() in (None, "base")

        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        await commands.upgrade()
        assert await commands.current() == "0001"

        await commands.downgrade("base")
        assert await commands.current() in (None, "base")
    finally:
        await config.close_pool()


def assert_sync_migration_error_handling_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync upgrade of an invalid migration records no applied version."""
    token, script_location, version_table, _table = _migration_paths(case, "error", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_bad.py", _bad_migration())

        with contextlib.suppress(Exception):
            commands.upgrade()
        assert _recorded_count_sync(config, version_table) == 0
    finally:
        config.close_pool()


async def assert_async_migration_error_handling_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async upgrade of an invalid migration records no applied version."""
    token, script_location, version_table, _table = _migration_paths(case, "error", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_bad.py", _bad_migration())

        with contextlib.suppress(Exception):
            await commands.upgrade()
        assert await _recorded_count_async(config, version_table) == 0
    finally:
        await config.close_pool()


def assert_sync_migration_multi_statement_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations apply every statement in a multi-statement revision atomically."""
    token, script_location, version_table, table = _migration_paths(case, "multistmt", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_seed.py", _seeded_table_migration(case, table))

        commands.upgrade()
        with config.provide_session() as driver:
            count = driver.select_value(f"SELECT COUNT(*) FROM {table}")
            assert int(count) == 2

        commands.downgrade("base")
        assert not _table_exists_sync(config, table)
    finally:
        config.close_pool()


async def assert_async_migration_multi_statement_contract(
    make_config: Any, case: MigrationCase, tmp_path: Path
) -> None:
    """Async migrations apply every statement in a multi-statement revision atomically."""
    token, script_location, version_table, table = _migration_paths(case, "multistmt", tmp_path)
    config = _build(make_config, script_location, version_table, token)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_seed.py", _seeded_table_migration(case, table))

        await commands.upgrade()
        async with config.provide_session() as driver:
            count = await driver.select_value(f"SELECT COUNT(*) FROM {table}")
            assert int(count) == 2

        await commands.downgrade("base")
        assert not await _table_exists_async(config, table)
    finally:
        await config.close_pool()


def assert_sync_migration_default_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations run unqualified DDL in the configured default schema."""
    if not case.supports_default_schema:
        pytest.skip(f"{case.adapter} has no default-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "schema", tmp_path)
    schema = _schema_identifier(f"{case.adapter}_default")
    config = _build_with_schema(make_config, script_location, version_table, token, default_schema=schema)
    try:
        with config.provide_session() as driver:
            driver.execute_script(_create_schema_sql(schema))
            driver.commit()

        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        commands.upgrade()

        with config.provide_session() as driver:
            assert _table_exists_in_schema_sync(driver, schema, table)
            assert _table_exists_in_schema_sync(driver, schema, version_table)
            assert not _table_exists_in_schema_sync(driver, "public", table)
    finally:
        with contextlib.suppress(Exception):
            with config.provide_session() as driver:
                driver.execute_script(_drop_schema_sql(schema))
                driver.commit()
        config.close_pool()


async def assert_async_migration_default_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async migrations run unqualified DDL in the configured default schema."""
    if not case.supports_default_schema:
        pytest.skip(f"{case.adapter} has no default-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "schema", tmp_path)
    schema = _schema_identifier(f"{case.adapter}_default")
    config = _build_with_schema(make_config, script_location, version_table, token, default_schema=schema)
    try:
        async with config.provide_session() as driver:
            await driver.execute_script(_create_schema_sql(schema))
            await driver.commit()

        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await _table_exists_in_schema_async(driver, schema, table)
            assert await _table_exists_in_schema_async(driver, schema, version_table)
            assert not await _table_exists_in_schema_async(driver, "public", table)
    finally:
        with contextlib.suppress(Exception):
            async with config.provide_session() as driver:
                await driver.execute_script(_drop_schema_sql(schema))
                await driver.commit()
        await config.close_pool()


def assert_sync_migration_multi_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations can separate migrated DDL and tracker schemas."""
    if not case.supports_multi_schema_migrations:
        pytest.skip(f"{case.adapter} has no multi-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "multischema", tmp_path)
    default_schema = _schema_identifier(f"{case.adapter}_default")
    tracker_schema = _schema_identifier(f"{case.adapter}_tracker")
    config = _build_with_schema(
        make_config,
        script_location,
        version_table,
        token,
        default_schema=default_schema,
        version_table_schema=tracker_schema,
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script(_create_schema_sql(default_schema))
            driver.execute_script(_create_schema_sql(tracker_schema))
            driver.commit()

        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        commands.upgrade()

        with config.provide_session() as driver:
            assert _table_exists_in_schema_sync(driver, default_schema, table)
            assert _table_exists_in_schema_sync(driver, tracker_schema, version_table)
            assert not _table_exists_in_schema_sync(driver, default_schema, version_table)
    finally:
        with contextlib.suppress(Exception):
            with config.provide_session() as driver:
                driver.execute_script(_drop_schema_sql(default_schema))
                driver.execute_script(_drop_schema_sql(tracker_schema))
                driver.commit()
        config.close_pool()


async def assert_async_migration_multi_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async migrations can separate migrated DDL and tracker schemas."""
    if not case.supports_multi_schema_migrations:
        pytest.skip(f"{case.adapter} has no multi-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "multischema", tmp_path)
    default_schema = _schema_identifier(f"{case.adapter}_default")
    tracker_schema = _schema_identifier(f"{case.adapter}_tracker")
    config = _build_with_schema(
        make_config,
        script_location,
        version_table,
        token,
        default_schema=default_schema,
        version_table_schema=tracker_schema,
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute_script(_create_schema_sql(default_schema))
            await driver.execute_script(_create_schema_sql(tracker_schema))
            await driver.commit()

        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await _table_exists_in_schema_async(driver, default_schema, table)
            assert await _table_exists_in_schema_async(driver, tracker_schema, version_table)
            assert not await _table_exists_in_schema_async(driver, default_schema, version_table)
    finally:
        with contextlib.suppress(Exception):
            async with config.provide_session() as driver:
                await driver.execute_script(_drop_schema_sql(default_schema))
                await driver.execute_script(_drop_schema_sql(tracker_schema))
                await driver.commit()
        await config.close_pool()


def assert_sync_migration_version_table_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations can place the tracker table in a configured schema without changing DDL schema."""
    if not case.supports_multi_schema_migrations:
        pytest.skip(f"{case.adapter} has no version-table-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "trackschema", tmp_path)
    tracker_schema = _schema_identifier(f"{case.adapter}_tracker")
    config = _build_with_schema(make_config, script_location, version_table, token, version_table_schema=tracker_schema)
    try:
        with config.provide_session() as driver:
            driver.execute_script(_create_schema_sql(tracker_schema))
            driver.commit()

        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        commands.upgrade()

        with config.provide_session() as driver:
            assert _table_exists_in_schema_sync(driver, _default_schema_name(case), table)
            assert _table_exists_in_schema_sync(driver, tracker_schema, version_table)
            assert not _table_exists_in_schema_sync(driver, _default_schema_name(case), version_table)
    finally:
        with contextlib.suppress(Exception):
            with config.provide_session() as driver:
                driver.execute_script(_drop_schema_sql(tracker_schema))
                driver.commit()
        config.close_pool()


async def assert_async_migration_version_table_schema_contract(
    make_config: Any, case: MigrationCase, tmp_path: Path
) -> None:
    """Async migrations can place the tracker table in a configured schema without changing DDL schema."""
    if not case.supports_multi_schema_migrations:
        pytest.skip(f"{case.adapter} has no version-table-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "trackschema", tmp_path)
    tracker_schema = _schema_identifier(f"{case.adapter}_tracker")
    config = _build_with_schema(make_config, script_location, version_table, token, version_table_schema=tracker_schema)
    try:
        async with config.provide_session() as driver:
            await driver.execute_script(_create_schema_sql(tracker_schema))
            await driver.commit()

        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await _table_exists_in_schema_async(driver, _default_schema_name(case), table)
            assert await _table_exists_in_schema_async(driver, tracker_schema, version_table)
            assert not await _table_exists_in_schema_async(driver, _default_schema_name(case), version_table)
    finally:
        with contextlib.suppress(Exception):
            async with config.provide_session() as driver:
                await driver.execute_script(_drop_schema_sql(tracker_schema))
                await driver.commit()
        await config.close_pool()


def assert_sync_migration_missing_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Sync migrations fail before touching public schema when the configured schema is absent."""
    if not case.supports_missing_schema_validation:
        pytest.skip(f"{case.adapter} has no missing-schema validation contract")
    token, script_location, version_table, table = _migration_paths(case, "misschema", tmp_path)
    schema = _schema_identifier(f"{case.adapter}_missing")
    config = _build_with_schema(make_config, script_location, version_table, token, default_schema=schema)
    try:
        commands = SyncMigrationCommands(config)
        commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))

        with pytest.raises(MigrationError, match=f"Configured schema '{schema}' does not exist"):
            commands.upgrade()

        with config.provide_session() as driver:
            assert not _table_exists_in_schema_sync(driver, "public", version_table)
            assert not _table_exists_in_schema_sync(driver, "public", table)
    finally:
        config.close_pool()


async def assert_async_migration_missing_schema_contract(make_config: Any, case: MigrationCase, tmp_path: Path) -> None:
    """Async migrations fail before touching public schema when the configured schema is absent."""
    if not case.supports_missing_schema_validation:
        pytest.skip(f"{case.adapter} has no missing-schema validation contract")
    token, script_location, version_table, table = _migration_paths(case, "misschema", tmp_path)
    schema = _schema_identifier(f"{case.adapter}_missing")
    config = _build_with_schema(make_config, script_location, version_table, token, default_schema=schema)
    try:
        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_migration(script_location, "0001_create.py", _create_table_migration(case, table))

        with pytest.raises(MigrationError, match=f"Configured schema '{schema}' does not exist"):
            await commands.upgrade()

        async with config.provide_session() as driver:
            assert not await _table_exists_in_schema_async(driver, "public", version_table)
            assert not await _table_exists_in_schema_async(driver, "public", table)
    finally:
        await config.close_pool()


async def assert_async_migration_non_transactional_default_schema_contract(
    make_config: Any, case: MigrationCase, tmp_path: Path
) -> None:
    """Async non-transactional SQL migrations honor the configured default schema."""
    if not case.supports_non_transactional_default_schema:
        pytest.skip(f"{case.adapter} has no non-transactional default-schema migration support")
    token, script_location, version_table, table = _migration_paths(case, "nontxschema", tmp_path)
    schema = _schema_identifier(f"{case.adapter}_default")
    config = _build_with_schema(make_config, script_location, version_table, token, default_schema=schema)
    try:
        async with config.provide_session() as driver:
            await driver.execute_script(_create_schema_sql(schema))
            await driver.commit()

        commands = AsyncMigrationCommands(config)
        await commands.init(script_location, package=True)
        _write_non_transactional_sql_migration(script_location, table)
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await _table_exists_in_schema_async(driver, schema, table)
            assert not await _table_exists_in_schema_async(driver, "public", table)
    finally:
        with contextlib.suppress(Exception):
            async with config.provide_session() as driver:
                await driver.execute_script(_drop_schema_sql(schema))
                await driver.commit()
        await config.close_pool()
