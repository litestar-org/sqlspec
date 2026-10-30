"""Unit tests for Migration Commands squash functionality.

Tests for:
- SyncMigrationCommands.squash()
- AsyncMigrationCommands.squash()
"""

import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import SquashValidationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.migrations.context import MigrationContext


def _write_users_posts_migrations(migrations_dir: Path) -> None:
    migrations_dir.mkdir(exist_ok=True)
    (migrations_dir / "0001_users.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
        "INSERT INTO users (id, name) VALUES (1, 'seed');\n\n"
        "-- name: migrate-0001-down\nDROP TABLE users;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_email.sql").write_text(
        "-- name: migrate-0002-up\nALTER TABLE users ADD COLUMN email TEXT;\n"
        "UPDATE users SET email = 'seed@example.com';\n\n"
        "-- name: migrate-0002-down\nALTER TABLE users DROP COLUMN email;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0003_posts.sql").write_text(
        "-- name: migrate-0003-up\n"
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), body TEXT);\n\n"
        "-- name: migrate-0003-down\nDROP TABLE posts;\n",
        encoding="utf-8",
    )


def _schema_and_rows(db_path: Path) -> tuple[list[tuple[str, str]], list[tuple[str, str | None]], list[Any]]:
    with sqlite3.connect(db_path) as connection:
        schema = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name != 'ddl_migrations' ORDER BY name"
        ).fetchall()
        tracking = connection.execute(
            "SELECT version_num, replaces FROM ddl_migrations ORDER BY version_num"
        ).fetchall()
        users = connection.execute("SELECT id, name, email FROM users").fetchall()
    return schema, tracking, users


def test_sync_migration_commands_squash_squash_valid_range(tmp_path: Path) -> None:
    """Test squash succeeds with valid version range."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    (tmp_path / "0003_posts.sql").write_text("-- name: migrate-0003-up\nCREATE TABLE t3 (id INT);")
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    commands.squash(start_version="0001", end_version="0003", description="release_1", dry_run=True)


def test_sync_migration_commands_squash_squash_invalid_range_raises_error(tmp_path: Path) -> None:
    """Test squash raises error when end version is before start."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    with pytest.raises(SquashValidationError, match="Invalid range"):
        commands.squash(start_version="0003", end_version="0001", description="release")


def test_sync_migration_commands_squash_squash_dry_run_does_not_modify_files(tmp_path: Path) -> None:
    """Test dry_run mode previews without modifying files."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    commands.squash(start_version="0001", end_version="0002", description="release", dry_run=True)
    assert (tmp_path / "0001_initial.sql").exists()
    assert (tmp_path / "0002_users.sql").exists()


def test_sync_migration_commands_squash_squash_update_database_false_skips_tracker(tmp_path: Path) -> None:
    """Test update_database=False skips tracker operations by verifying no DB session is opened."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    config = Mock()
    config.is_async = False
    tracker_mock = Mock()
    config.migration_tracker_type = Mock(return_value=tracker_mock)
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    commands.squash(
        start_version="0001", end_version="0002", description="release", update_database=False, dry_run=True
    )
    config.provide_session.assert_not_called()


@pytest.mark.anyio
async def test_async_squash_builds_sync_runner_with_migration_context(tmp_path: Path) -> None:
    """Async squash should mirror the sync path and pass a real migration context."""
    (tmp_path / "0001_initial.py").write_text("def up(context): pass\n\ndef down(context): pass\n")
    config = Mock()
    config.is_async = True
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.get_observability_runtime = Mock(return_value=None)
    commands = AsyncMigrationCommands(config)

    with patch("sqlspec.migrations.commands.SyncMigrationRunner") as runner_type:
        runner = runner_type.return_value
        runner.get_migration_files.return_value = [("0001", tmp_path / "0001_initial.py")]
        await commands.squash(start_version="0001", end_version="0001", description="release", dry_run=True)

    context = runner_type.call_args.args[2]
    assert isinstance(context, MigrationContext)


@pytest.mark.anyio
async def test_async_migration_commands_squash_async_squash_valid_range(tmp_path: Path) -> None:
    """Test async squash succeeds with valid version range."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    config = Mock()
    config.is_async = True
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.get_observability_runtime = Mock(return_value=None)
    commands = AsyncMigrationCommands(config)
    await commands.squash(start_version="0001", end_version="0002", description="release", dry_run=True)


@pytest.mark.anyio
async def test_async_migration_commands_squash_async_squash_invalid_range_raises_error(tmp_path: Path) -> None:
    """Test async squash raises error when end version is before start."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    config = Mock()
    config.is_async = True
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.get_observability_runtime = Mock(return_value=None)
    commands = AsyncMigrationCommands(config)
    with pytest.raises(SquashValidationError, match="Invalid range"):
        await commands.squash(start_version="0003", end_version="0001", description="release")


def test_sync_squash_defaults_squash_defaults_to_all_migrations(tmp_path: Path) -> None:
    """Test that omitting start/end squashes all sequential migrations."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    (tmp_path / "0002_users.sql").write_text("-- name: migrate-0002-up\nCREATE TABLE t2 (id INT);")
    (tmp_path / "0003_posts.sql").write_text("-- name: migrate-0003-up\nCREATE TABLE t3 (id INT);")
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    commands.squash(description="all_squashed", dry_run=True)


def test_sync_squash_defaults_squash_prompts_for_description(tmp_path: Path) -> None:
    """Test that missing description triggers interactive prompt."""
    (tmp_path / "0001_initial.sql").write_text("-- name: migrate-0001-up\nCREATE TABLE t1 (id INT);")
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    with patch("rich.prompt.Prompt.ask", return_value="prompted_description") as mock_prompt:
        commands.squash(start_version="0001", end_version="0001", dry_run=True)
        mock_prompt.assert_called_once()


def test_sync_squash_defaults_squash_no_migrations_returns_early(tmp_path: Path) -> None:
    """Test that empty directory returns early when no range given."""
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)
    commands.squash(description="empty", dry_run=True)


def test_sync_squash_auto_discovers_timestamp_migrations_and_defaults_description_with_yes(tmp_path: Path) -> None:
    """Omitting range and description with yes=True squashes timestamped migrations into 0001_initial_schema.sql.

    The default ``concat`` strategy keeps every statement in order, so the squashed UP replays to the same schema.
    """
    (tmp_path / "20251011120000_init.sql").write_text(
        "-- name: migrate-20251011120000-up\nCREATE TABLE widgets (id INT PRIMARY KEY);\n", encoding="utf-8"
    )
    (tmp_path / "20251012153000_alter.sql").write_text(
        "-- name: migrate-20251012153000-up\nALTER TABLE widgets ADD COLUMN label VARCHAR(80);\n", encoding="utf-8"
    )
    config = Mock()
    config.is_async = False
    config.migration_tracker_type = Mock(return_value=Mock())
    config.migration_config = {"script_location": str(tmp_path)}
    config.provide_session = Mock()
    config.get_observability_runtime = Mock(return_value=None)
    commands = SyncMigrationCommands(config)

    with patch("rich.prompt.Prompt.ask") as mock_prompt:
        commands.squash(yes=True, update_database=False)
        mock_prompt.assert_not_called()

    assert sorted(p.name for p in tmp_path.glob("*.sql")) == ["0001_initial_schema.sql"]
    content = (tmp_path / "0001_initial_schema.sql").read_text(encoding="utf-8")
    up_body = content.split("-- name: migrate-0001-up", 1)[1].split("-- name:", 1)[0]
    with sqlite3.connect(":memory:") as replay:
        replay.executescript(up_body)
        assert [row[1] for row in replay.execute("PRAGMA table_info(widgets)")] == ["id", "label"]


def test_sync_default_concat_squash_round_trip(tmp_path: Path) -> None:
    """Default squash keeps DML and DDL order; fresh, fully applied, and partially applied databases behave correctly."""
    migrations_dir = tmp_path / "migrations"
    _write_users_posts_migrations(migrations_dir)

    def commands_for(name: str) -> SyncMigrationCommands:
        return SyncMigrationCommands(
            SqliteConfig(
                connection_config={"database": str(tmp_path / f"{name}.db")},
                migration_config={"script_location": str(migrations_dir), "enabled": True},
            )
        )

    reference = commands_for("reference")
    reference.upgrade(echo=False)
    expected_schema, _, expected_users = _schema_and_rows(tmp_path / "reference.db")
    commands_for("full").upgrade(echo=False)
    commands_for("partial").upgrade(revision="0002", echo=False)

    reference.squash(yes=True)
    assert sorted(p.name for p in migrations_dir.glob("*.sql")) == ["0001_initial_schema.sql"]
    assert _schema_and_rows(tmp_path / "reference.db")[1] == [("0001", "0001,0002,0003")]

    commands_for("fresh").upgrade(echo=False)
    fresh_schema, fresh_tracking, fresh_users = _schema_and_rows(tmp_path / "fresh.db")
    assert fresh_schema == expected_schema
    assert fresh_users == expected_users == [(1, "seed", "seed@example.com")]
    assert fresh_tracking == [("0001", "0001,0002,0003")]

    commands_for("full").upgrade(echo=False)
    full_schema, full_tracking, _ = _schema_and_rows(tmp_path / "full.db")
    assert full_schema == expected_schema
    assert full_tracking == [("0001", "0001,0002,0003")]

    with pytest.raises(SquashValidationError, match="Cannot reconcile squashed migration 0001"):
        commands_for("partial").upgrade(echo=False)
    assert _schema_and_rows(tmp_path / "partial.db")[1] == [("0001", None), ("0002", None)]

    commands_for("fresh").downgrade(revision="base", echo=False)
    with sqlite3.connect(tmp_path / "fresh.db") as connection:
        remaining = connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    assert remaining == [("ddl_migrations",)]


@pytest.mark.anyio
async def test_async_default_concat_squash_round_trip(tmp_path: Path) -> None:
    """Async squash with the default strategy reconciles a fully applied database and rebuilds a fresh one."""
    migrations_dir = tmp_path / "migrations"
    _write_users_posts_migrations(migrations_dir)

    def config_for(name: str) -> AiosqliteConfig:
        return AiosqliteConfig(
            connection_config={"database": str(tmp_path / f"{name}.db")},
            migration_config={"script_location": str(migrations_dir), "enabled": True},
        )

    author_config = config_for("author")
    try:
        author = AsyncMigrationCommands(author_config)
        await author.upgrade(echo=False)
        expected_schema, _, expected_users = _schema_and_rows(tmp_path / "author.db")
        await author.squash(yes=True)
    finally:
        await author_config.close_pool()

    fresh_config = config_for("fresh")
    try:
        await AsyncMigrationCommands(fresh_config).upgrade(echo=False)
    finally:
        await fresh_config.close_pool()

    assert _schema_and_rows(tmp_path / "author.db")[1] == [("0001", "0001,0002,0003")]
    assert _schema_and_rows(tmp_path / "fresh.db") == (expected_schema, [("0001", "0001,0002,0003")], expected_users)
