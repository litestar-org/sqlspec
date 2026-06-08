"""Behavior helpers for shared migration-lifecycle contract tests."""

import contextlib
from pathlib import Path
from typing import Any

from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from tests.integration.adapters.contracts._migration_cases import MigrationCase


def _migration_paths(case: MigrationCase, behavior: str, tmp_path: Path) -> "tuple[str, str, str, str]":
    token = f"{case.id}_{behavior}".replace("-", "_")
    script_location = str(tmp_path / f"mig_{token}")
    version_table = f"ddl_mig_{token}"
    table = f"mig_users_{token}"
    return token, script_location, version_table, table


def _create_table_migration(table: str) -> str:
    return f'''"""Create {table}."""


def up():
    """Create the table."""
    return ["CREATE TABLE {table} (id INTEGER, name VARCHAR(255) NOT NULL)"]


def down():
    """Drop the table."""
    return ["DROP TABLE IF EXISTS {table}"]
'''


def _seeded_table_migration(table: str) -> str:
    return f'''"""Create {table} with seed rows."""


def up():
    """Create the table and seed two rows."""
    return [
        "CREATE TABLE {table} (id INTEGER, name VARCHAR(255) NOT NULL)",
        "INSERT INTO {table} (id, name) VALUES (1, 'first')",
        "INSERT INTO {table} (id, name) VALUES (2, 'second')",
    ]


def down():
    """Drop the table."""
    return ["DROP TABLE IF EXISTS {table}"]
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


def _write_migration(script_location: str, filename: str, content: str) -> None:
    (Path(script_location) / filename).write_text(content)


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
        _write_migration(script_location, "0001_create.py", _create_table_migration(table))

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
        _write_migration(script_location, "0001_create.py", _create_table_migration(table))

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
        _write_migration(script_location, "0001_first.py", _create_table_migration(table))
        _write_migration(script_location, "0002_second.py", _create_table_migration(second_table))

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
        _write_migration(script_location, "0001_first.py", _create_table_migration(table))
        _write_migration(script_location, "0002_second.py", _create_table_migration(second_table))

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

        _write_migration(script_location, "0001_create.py", _create_table_migration(table))
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

        _write_migration(script_location, "0001_create.py", _create_table_migration(table))
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
        _write_migration(script_location, "0001_seed.py", _seeded_table_migration(table))

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
        _write_migration(script_location, "0001_seed.py", _seeded_table_migration(table))

        await commands.upgrade()
        async with config.provide_session() as driver:
            count = await driver.select_value(f"SELECT COUNT(*) FROM {table}")
            assert int(count) == 2

        await commands.downgrade("base")
        assert not await _table_exists_async(config, table)
    finally:
        await config.close_pool()
