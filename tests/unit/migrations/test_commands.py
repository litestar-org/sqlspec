"""Unit tests for migration squash validation, tracker recording, and command orchestration."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.db2.migrations import Db2SyncMigrationTracker
from sqlspec.adapters.mssql_python.migrations import MssqlPythonSyncMigrationTracker
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.adapters.pymssql.migrations import PymssqlSyncMigrationTracker
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SquashValidationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands, create_migration_commands
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.squash import MigrationSquasher
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker


def _write_sql_migration(
    migrations_dir: Path,
    filename: str,
    version: str,
    up_sql: str,
    down_sql: str | None = None,
    *,
    header_lines: list[str] | None = None,
) -> Path:
    prefix = "\n".join(header_lines) + "\n" if header_lines else ""
    content = f"{prefix}-- name: migrate-{version}-up\n{up_sql}\n"
    if down_sql is not None:
        content += f"\n-- name: migrate-{version}-down\n{down_sql}\n"
    path = migrations_dir / filename
    path.write_text(content, encoding="utf-8")
    return path


def test_schema_strategy_rejects_partial_range_in_plan_squash(tmp_path: Path) -> None:
    """The schema squash strategy requires squashing from the first migration through head."""
    _write_sql_migration(tmp_path, "0001_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);")
    _write_sql_migration(tmp_path, "0002_posts.sql", "0002", "CREATE TABLE posts (id INTEGER PRIMARY KEY);")
    _write_sql_migration(tmp_path, "0003_tags.sql", "0003", "CREATE TABLE tags (id INTEGER PRIMARY KEY);")

    runner = SyncMigrationRunner(tmp_path)
    squasher = MigrationSquasher(tmp_path, runner, strategy="schema")

    with pytest.raises(
        SquashValidationError, match=r"must squash from the first migration \(0001\) through head \(0003\)"
    ):
        squasher.plan_squash(
            "0002",
            "0003",
            "partial_tail",
            schema_ddl=(["CREATE TABLE users (id INTEGER PRIMARY KEY);"], ["DROP TABLE IF EXISTS users;"]),
        )

    with pytest.raises(
        SquashValidationError, match=r"must squash from the first migration \(0001\) through head \(0003\)"
    ):
        squasher.plan_squash(
            "0001",
            "0002",
            "partial_head",
            schema_ddl=(["CREATE TABLE users (id INTEGER PRIMARY KEY);"], ["DROP TABLE IF EXISTS users;"]),
        )


def test_schema_strategy_rejects_unapplied_migrations_in_commands(tmp_path: Path) -> None:
    """SyncMigrationCommands.squash(--strategy schema) requires all migrations through head to be applied."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _write_sql_migration(migrations_dir, "0001_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);")
    _write_sql_migration(migrations_dir, "0002_posts.sql", "0002", "CREATE TABLE posts (id INTEGER PRIMARY KEY);")

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    commands: SyncMigrationCommands = create_migration_commands(config)
    assert isinstance(commands, SyncMigrationCommands)

    with pytest.raises(SquashValidationError, match="database has unapplied migrations"):
        commands.squash("0001", "0002", "baseline", strategy="schema", yes=True)

    commands.upgrade("0001")
    with pytest.raises(SquashValidationError, match="database has unapplied migrations"):
        commands.squash("0001", "0002", "baseline", strategy="schema", yes=True)

    commands.upgrade()
    commands.squash("0001", "0002", "baseline", strategy="schema", yes=True)
    squashed = (migrations_dir / "0001_baseline.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE users" in squashed
    assert "CREATE TABLE posts" in squashed


async def test_async_schema_strategy_rejects_unapplied_migrations(tmp_path: Path) -> None:
    """AsyncMigrationCommands.squash(--strategy schema) requires all migrations through head to be applied."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _write_sql_migration(migrations_dir, "0001_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);")
    _write_sql_migration(migrations_dir, "0002_posts.sql", "0002", "CREATE TABLE posts (id INTEGER PRIMARY KEY);")

    config = AiosqliteConfig(
        connection_config={"database": str(tmp_path / "async_test.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    commands: AsyncMigrationCommands = create_migration_commands(config)
    assert isinstance(commands, AsyncMigrationCommands)

    try:
        with pytest.raises(SquashValidationError, match="database has unapplied migrations"):
            await commands.squash("0001", "0002", "baseline", strategy="schema", yes=True)

        await commands.upgrade()
        await commands.squash("0001", "0002", "baseline", strategy="schema", yes=True)
        assert (migrations_dir / "0001_baseline.sql").exists()
    finally:
        await config.close_pool()


def test_single_file_auto_splits_on_sql_directive_changes_and_rejects_forced_single_file(tmp_path: Path) -> None:
    """Default single_file=None splits when SQL directives change while single_file=True raises an error."""
    _write_sql_migration(
        tmp_path, "0001_create_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);", "DROP TABLE users;"
    )
    _write_sql_migration(
        tmp_path,
        "0002_concurrent_idx.sql",
        "0002",
        "CREATE INDEX idx_users_id ON users (id);",
        "DROP INDEX idx_users_id;",
        header_lines=["-- transactional: false"],
    )
    _write_sql_migration(
        tmp_path, "0003_create_posts.sql", "0003", "CREATE TABLE posts (id INTEGER PRIMARY KEY);", "DROP TABLE posts;"
    )

    runner = SyncMigrationRunner(tmp_path)
    squasher = MigrationSquasher(tmp_path, runner)

    auto_plans = squasher.plan_squash("0001", "0003", "release")
    assert len(auto_plans) == 3
    assert [plan.target_version for plan in auto_plans] == ["0001", "0002", "0003"]

    split_plans = squasher.plan_squash("0001", "0003", "release", single_file=False)
    assert len(split_plans) == 3

    with pytest.raises(SquashValidationError, match="conflicting '-- transactional:' or '-- schema:' directives"):
        squasher.plan_squash("0001", "0003", "release", single_file=True)


def test_single_file_rejects_conflicting_schema_directives_and_python_output_with_directives(tmp_path: Path) -> None:
    """Conflicting -- schema: directives or Python output on directive-bearing SQL migrations fail closed."""
    _write_sql_migration(
        tmp_path,
        "0001_tenant_a.sql",
        "0001",
        "CREATE TABLE users (id INTEGER PRIMARY KEY);",
        header_lines=["-- schema: tenant_a"],
    )
    _write_sql_migration(
        tmp_path,
        "0002_tenant_b.sql",
        "0002",
        "CREATE TABLE posts (id INTEGER PRIMARY KEY);",
        header_lines=["-- schema: tenant_b"],
    )

    runner = SyncMigrationRunner(tmp_path)
    squasher = MigrationSquasher(tmp_path, runner)

    plans = squasher.plan_squash("0001", "0002", "release")
    assert len(plans) == 2

    with pytest.raises(SquashValidationError, match="conflicting '-- transactional:' or '-- schema:' directives"):
        squasher.plan_squash("0001", "0002", "release", single_file=True)

    with pytest.raises(SquashValidationError, match="Python migrations do not support SQL header directives"):
        squasher.plan_squash("0001", "0001", "release", output_format="py")


def test_squash_dry_run_extracts_and_prints_sql_without_modifying_files(tmp_path: Path) -> None:
    """squash(dry_run=True) calls extract_plan_sql, prints SQL preview, and validates DDL folding."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _write_sql_migration(
        migrations_dir, "0001_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);", "DROP TABLE users;"
    )
    _write_sql_migration(migrations_dir, "0002_add_email.sql", "0002", "ALTER TABLE users ADD COLUMN email TEXT;")

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    commands: SyncMigrationCommands = create_migration_commands(config)
    assert isinstance(commands, SyncMigrationCommands)

    with patch("sqlspec.migrations.commands.console.print") as mock_print:
        commands.squash("0001", "0002", "release", strategy="ddl", dry_run=True)

    printed = "\n".join(str(call.args[0]) for call in mock_print.call_args_list if call.args)
    assert "[Preview Mode - No changes made]" in printed
    assert "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)" in printed
    assert (migrations_dir / "0001_users.sql").exists()
    assert (migrations_dir / "0002_add_email.sql").exists()
    assert not (migrations_dir / "0001_release.sql").exists()


def test_squash_dry_run_surfaces_ddl_validation_errors(tmp_path: Path) -> None:
    """squash(dry_run=True, strategy='ddl') raises SquashValidationError when DDL cannot be folded."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _write_sql_migration(
        migrations_dir, "0001_alter_missing.sql", "0001", "ALTER TABLE missing_table ADD COLUMN email TEXT;"
    )

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    commands: SyncMigrationCommands = create_migration_commands(config)
    assert isinstance(commands, SyncMigrationCommands)

    with pytest.raises(SquashValidationError, match="not a table created within"):
        commands.squash("0001", "0001", "release", strategy="ddl", dry_run=True)


def test_squash_prechecks_all_plans_before_writing_files(tmp_path: Path) -> None:
    """If any split plan fails database pre-validation, no migration files on disk are modified."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    _write_sql_migration(migrations_dir, "0001_users.sql", "0001", "CREATE TABLE users (id INTEGER PRIMARY KEY);")
    _write_sql_migration(
        migrations_dir,
        "0002_idx.sql",
        "0002",
        "CREATE INDEX idx_users_id ON users (id);",
        header_lines=["-- transactional: false"],
    )
    before = {path.name: path.read_text(encoding="utf-8") for path in migrations_dir.iterdir()}

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    commands: SyncMigrationCommands = create_migration_commands(config)
    assert isinstance(commands, SyncMigrationCommands)

    with (
        patch.object(
            commands.tracker,
            "is_squash_already_applied",
            side_effect=[False, SquashValidationError("partial squash state")],
        ),
        pytest.raises(SquashValidationError, match="partial squash state"),
    ):
        commands.squash("0001", "0002", "release", update_database=True, yes=True)

    after = {path.name: path.read_text(encoding="utf-8") for path in migrations_dir.iterdir()}
    assert after == before


def test_tracker_record_statement_shared_across_sync_async_and_adapter_trackers() -> None:
    """Sync, async, and adapter-specific trackers build record statements via _record_statement."""
    sync_tracker = SyncMigrationTracker()
    async_tracker = AsyncMigrationTracker()

    stmt = sync_tracker._record_statement("0001", "sequential", 1, "initial", 12, "abc123", "tester", ["0001", "0002"])
    async_stmt = async_tracker._record_statement(
        "0001", "sequential", 1, "initial", 12, "abc123", "tester", ["0001", "0002"]
    )
    built_sync = stmt.build()
    built_async = async_stmt.build()
    assert "INSERT INTO" in built_sync.sql
    assert built_sync.sql == built_async.sql

    for tracker_cls in (PymssqlSyncMigrationTracker, MssqlPythonSyncMigrationTracker, OracleSyncMigrationTracker):
        tracker = tracker_cls()
        driver = MagicMock()
        seq_result = MagicMock()
        seq_result.get_data.return_value = [{"next_seq": 1}]
        driver.execute.return_value = seq_result
        with patch.object(tracker, "_record_statement", wraps=tracker._record_statement) as spy_stmt:
            tracker.record_migration(driver, "0001", "init", 5, "chk")
            spy_stmt.assert_called_once()
        assert driver.execute.call_count == 2

    db2_tracker = Db2SyncMigrationTracker()
    db2_stmt = db2_tracker._record_statement("0001", "sequential", 1, "init", 5, "chk", "tester", ["0001"])
    assert "INSERT INTO" in db2_stmt.build().sql


async def test_oracle_async_tracker_uses_record_statement() -> None:
    """OracleAsyncMigrationTracker.record_migration delegates statement construction to _record_statement."""
    tracker = OracleAsyncMigrationTracker()
    driver = MagicMock()
    seq_result = MagicMock()
    seq_result.get_data.return_value = [{"next_seq": 1}]
    driver.execute = AsyncMock(return_value=seq_result)
    driver.commit = AsyncMock()
    with patch.object(tracker, "_record_statement", wraps=tracker._record_statement) as spy_stmt:
        await tracker.record_migration(driver, "0001", "init", 5, "chk")
        spy_stmt.assert_called_once()
    assert driver.execute.await_count == 2
