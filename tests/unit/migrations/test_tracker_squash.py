"""Unit tests for Migration Tracker squash functionality.

Tests for:
- Schema migration to add `replaces` column
- replace_with_squash() method
- is_squash_already_applied() method
"""

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest


def _mock_result(**kwargs: Any) -> Mock:
    """Create a Mock result with get_data() returning .data."""
    m = Mock(**kwargs)
    m.get_data.return_value = m.data
    return m


def test_tracker_schema_squash_create_table_sql_includes_replaces_column() -> None:
    """Test that _tracking_table_ddl includes replaces column."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    create_sql = tracker._tracking_table_ddl()
    column_names = [col.name.lower() for col in create_sql.columns]
    assert "replaces" in column_names


def test_tracker_schema_squash_replaces_column_is_nullable() -> None:
    """Test that replaces column is nullable (not_null=False)."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    create_sql = tracker._tracking_table_ddl()
    replaces_col = next((col for col in create_sql.columns if col.name.lower() == "replaces"), None)
    assert replaces_col is not None
    assert not replaces_col.not_null


def test_sync_tracker_squash_methods_replace_with_squash_deletes_replaced_versions() -> None:
    """Test replace_with_squash deletes all replaced version records."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {"autocommit": False}
    execute_calls: list[Any] = []

    def mock_execute(sql: Any) -> Mock:
        execute_calls.append(str(sql))
        return _mock_result(rows_affected=1, data=[{"next_seq": 1}])

    driver.execute.side_effect = mock_execute
    tracker.replace_with_squash(
        driver=driver,
        squashed_version="0001",
        replaced_versions=["0001", "0002", "0003"],
        description="release",
        checksum="abc123",
    )
    delete_calls = [c for c in execute_calls if "DELETE" in c.upper()]
    assert len(delete_calls) >= 1


def test_sync_tracker_squash_methods_replace_with_squash_inserts_squashed_record() -> None:
    """Test replace_with_squash inserts squashed version with replaces metadata."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {"autocommit": False}
    execute_calls: list[Any] = []

    def mock_execute(sql: Any) -> Mock:
        execute_calls.append(str(sql))
        return _mock_result(rows_affected=1, data=[{"next_seq": 1}])

    driver.execute.side_effect = mock_execute
    tracker.replace_with_squash(
        driver=driver,
        squashed_version="0001",
        replaced_versions=["0001", "0002", "0003"],
        description="release",
        checksum="abc123",
    )
    insert_calls = [c for c in execute_calls if "INSERT" in c.upper()]
    assert len(insert_calls) >= 1


def test_sync_tracker_squash_methods_is_squash_already_applied_returns_true_when_replaced_exists() -> None:
    """Test is_squash_already_applied returns True when replaced version found."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(
        data=[{"version_num": "0001"}, {"version_num": "0002"}, {"version_num": "0003"}]
    )
    result = tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is True


def test_sync_tracker_squash_methods_is_squash_already_applied_returns_false_for_fresh_db() -> None:
    """Test is_squash_already_applied returns False when no replaced versions found."""
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[])
    result = tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is False


@pytest.mark.anyio
async def test_async_tracker_squash_methods_async_replace_with_squash_deletes_replaced_versions() -> None:
    """Test async replace_with_squash deletes all replaced version records."""
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()
    driver.driver_features = {"autocommit": False}
    execute_calls: list[Any] = []

    async def mock_execute(sql: Any) -> Mock:
        execute_calls.append(str(sql))
        return _mock_result(rows_affected=1, data=[{"next_seq": 1}])

    async def mock_commit() -> None:
        pass

    driver.execute = mock_execute
    driver.commit = mock_commit
    await tracker.replace_with_squash(
        driver=driver,
        squashed_version="0001",
        replaced_versions=["0001", "0002", "0003"],
        description="release",
        checksum="abc123",
    )
    delete_calls = [c for c in execute_calls if "DELETE" in c.upper()]
    assert len(delete_calls) >= 1


@pytest.mark.anyio
async def test_async_tracker_squash_methods_async_is_squash_already_applied_returns_true_when_exists() -> None:
    """Test async is_squash_already_applied returns True when replaced version found."""
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def mock_execute(sql: Any) -> Mock:
        return _mock_result(data=[{"version_num": "0001"}, {"version_num": "0002"}, {"version_num": "0003"}])

    driver.execute = mock_execute
    result = await tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is True


@pytest.mark.anyio
async def test_async_tracker_squash_methods_async_is_squash_already_applied_returns_false_for_fresh_db() -> None:
    """Test async is_squash_already_applied returns False when no replaced versions found."""
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def mock_execute(sql: Any) -> Mock:
        return _mock_result(data=[])

    driver.execute = mock_execute
    result = await tracker.is_squash_already_applied(
        driver=driver, squashed_version="0001", replaced_versions=["0001", "0002", "0003"]
    )
    assert result is False


def test_check_versions_exist_sql_uses_where_in_clause() -> None:
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    query = str(tracker._check_versions_query(["0001", "0002"])).upper()
    assert "WHERE" in query
    assert "IN" in query
    assert "SELECT *" not in query


def test_sync_is_squash_already_applied_executes_filtered_where_in_query() -> None:
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[{"version_num": "0001"}, {"version_num": "0002"}])
    result = tracker.is_squash_already_applied(
        driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
    )
    query = str(driver.execute.call_args.args[0]).upper()
    assert result is True
    assert "WHERE" in query
    assert "IN" in query
    assert "SELECT *" not in query


def test_sync_is_squash_already_applied_returns_false_for_empty_filtered_result() -> None:
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[])
    assert (
        tracker.is_squash_already_applied(driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"])
        is False
    )


@pytest.mark.anyio
async def test_async_is_squash_already_applied_executes_filtered_where_in_query() -> None:
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()
    captured: list[Any] = []

    async def execute(query: Any) -> Mock:
        captured.append(query)
        return _mock_result(data=[{"version_num": "0001"}, {"version_num": "0002"}])

    driver.execute = execute
    result = await tracker.is_squash_already_applied(
        driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
    )
    query = str(captured[0]).upper()
    assert result is True
    assert "WHERE" in query
    assert "IN" in query
    assert "SELECT *" not in query


@pytest.mark.anyio
async def test_async_is_squash_already_applied_returns_true_for_filtered_result() -> None:
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def execute(query: Any) -> Mock:
        _ = query
        return _mock_result(data=[{"version_num": "0001"}, {"version_num": "0002"}])

    driver.execute = execute
    assert (
        await tracker.is_squash_already_applied(
            driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
        )
        is True
    )


@pytest.mark.anyio
async def test_async_is_squash_already_applied_returns_false_for_empty_filtered_result() -> None:
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def execute(query: Any) -> Mock:
        _ = query
        return _mock_result(data=[])

    driver.execute = execute
    assert (
        await tracker.is_squash_already_applied(
            driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
        )
        is False
    )


def test_sync_is_squash_already_applied_raises_on_partial_overlap() -> None:
    from sqlspec.exceptions import MigrationError
    from sqlspec.migrations.tracker import SyncMigrationTracker

    tracker = SyncMigrationTracker()
    driver = Mock()
    driver.execute.return_value = _mock_result(data=[{"version_num": "0001"}])
    with pytest.raises(MigrationError, match="Partial squash state detected"):
        tracker.is_squash_already_applied(driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"])


@pytest.mark.anyio
async def test_async_is_squash_already_applied_raises_on_partial_overlap() -> None:
    from sqlspec.exceptions import MigrationError
    from sqlspec.migrations.tracker import AsyncMigrationTracker

    tracker = AsyncMigrationTracker()
    driver = Mock()

    async def execute(query: Any) -> Mock:
        _ = query
        return _mock_result(data=[{"version_num": "0001"}])

    driver.execute = execute
    with pytest.raises(MigrationError, match="Partial squash state detected"):
        await tracker.is_squash_already_applied(
            driver=driver, squashed_version="0009", replaced_versions=["0001", "0002"]
        )


@pytest.mark.anyio
async def test_runner_load_migration_extracts_replaces_from_sql_and_python_files(tmp_path: Path) -> None:
    """Verify SyncMigrationRunner and AsyncMigrationRunner populate replaces from SQL and Python migrations."""
    from sqlspec.migrations.runner import AsyncMigrationRunner, SyncMigrationRunner, extract_replaces_versions

    sql_file = tmp_path / "0001_initial_schema.sql"
    sql_file.write_text(
        "-- SQLSpec Migration\n"
        "-- Version: 0001\n"
        "-- Description: initial_schema\n"
        "-- Squashed from: 0001, 0002, 0003\n\n"
        "-- name: migrate-0001-up\n"
        "CREATE TABLE users (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE IF EXISTS users;\n",
        encoding="utf-8",
    )

    py_doc_file = tmp_path / "0002_py_squash.py"
    py_doc_file.write_text(
        '"""SQLSpec Migration.\n\n'
        "Version: 0002\n"
        "Description: py_squash\n"
        "Squashed from: 20251011120000, 20251012130000\n"
        '"""\n\n'
        "def up() -> list[str]:\n"
        "    return ['CREATE TABLE posts (id INTEGER PRIMARY KEY);']\n\n"
        "def down() -> list[str]:\n"
        "    return ['DROP TABLE IF EXISTS posts;']\n",
        encoding="utf-8",
    )

    py_attr_file = tmp_path / "0003_py_replaces.py"
    py_attr_file.write_text(
        '"""Migration with __replaces__."""\n\n'
        '__replaces__ = ["0003", "0004"]\n\n'
        "def up() -> list[str]:\n"
        "    return ['SELECT 1;']\n",
        encoding="utf-8",
    )

    assert extract_replaces_versions(sql_file.read_text(encoding="utf-8"), ".sql") == ["0001", "0002", "0003"]
    assert extract_replaces_versions(py_doc_file.read_text(encoding="utf-8"), ".py") == [
        "20251011120000",
        "20251012130000",
    ]
    assert extract_replaces_versions(py_attr_file.read_text(encoding="utf-8"), ".py") == ["0003", "0004"]

    sync_runner = SyncMigrationRunner(tmp_path)
    sql_meta = sync_runner.load_migration(sql_file, "0001")
    py_doc_meta = sync_runner.load_migration(py_doc_file, "0002")
    py_attr_meta = sync_runner.load_migration(py_attr_file, "0003")

    assert sql_meta["replaces"] == ["0001", "0002", "0003"]
    assert py_doc_meta["replaces"] == ["20251011120000", "20251012130000"]
    assert py_attr_meta["replaces"] == ["0003", "0004"]

    async_runner = AsyncMigrationRunner(tmp_path)
    async_sql_meta = await async_runner.load_migration(sql_file, "0001")
    assert async_sql_meta["replaces"] == ["0001", "0002", "0003"]


def test_sync_upgrade_fresh_db_records_replaces_metadata(tmp_path: Path) -> None:
    """Verify upgrading a fresh database with a squashed migration populates the replaces column."""
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.migrations.commands import SyncMigrationCommands

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_initial_schema.sql").write_text(
        "-- SQLSpec Migration\n"
        "-- Version: 0001\n"
        "-- Description: initial_schema\n"
        "-- Squashed from: 0001, 0002, 0003\n\n"
        "-- name: migrate-0001-up\n"
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE IF EXISTS users;\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "fresh.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
    )
    commands = SyncMigrationCommands(config)
    commands.upgrade(echo=False)

    with config.provide_session() as driver:
        applied = commands.tracker.get_applied_migrations(driver)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"
    assert applied[0]["replaces"] in {"0001,0002,0003", "0001, 0002, 0003"}

    commands.upgrade(echo=False)
    with config.provide_session() as driver:
        applied_after = commands.tracker.get_applied_migrations(driver)
    assert len(applied_after) == 1


def test_sync_upgrade_reconciles_fully_applied_pre_squash_db_and_allows_downgrade(tmp_path: Path) -> None:
    """Verify upgrade reconciles a pre-squash DB with 0001..0003 applied and downgrade succeeds."""
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.migrations.commands import SyncMigrationCommands

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_users.sql").write_text(
        "-- name: migrate-0001-up\n"
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE users;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_email.sql").write_text(
        "-- name: migrate-0002-up\n"
        "ALTER TABLE users ADD COLUMN email TEXT;\n"
        "-- name: migrate-0002-down\n"
        "ALTER TABLE users DROP COLUMN email;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0003_posts.sql").write_text(
        "-- name: migrate-0003-up\n"
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);\n"
        "-- name: migrate-0003-down\n"
        "DROP TABLE posts;\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "staging.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
    )
    commands = SyncMigrationCommands(config)
    commands.upgrade(echo=False)

    with config.provide_session() as driver:
        pre_applied = commands.tracker.get_applied_migrations(driver)
    assert [row["version_num"] for row in pre_applied] == ["0001", "0002", "0003"]

    commands.squash("0001", "0003", "initial_schema", update_database=False, yes=True)

    reconcile_commands = SyncMigrationCommands(config)
    reconcile_commands.upgrade(echo=False)

    with config.provide_session() as driver:
        post_applied = reconcile_commands.tracker.get_applied_migrations(driver)
    assert len(post_applied) == 1
    assert post_applied[0]["version_num"] == "0001"
    assert post_applied[0]["replaces"] in {"0001,0002,0003", "0001, 0002, 0003"}

    reconcile_commands.downgrade(revision="-1", echo=False)
    with config.provide_session() as driver:
        after_downgrade = reconcile_commands.tracker.get_applied_migrations(driver)
    assert after_downgrade == []


def test_sync_upgrade_raises_on_partially_applied_pre_squash_db(tmp_path: Path) -> None:
    """Verify upgrade raises SquashValidationError when only a subset of replaced versions is applied."""
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.commands import SyncMigrationCommands

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_users.sql").write_text(
        "-- name: migrate-0001-up\n"
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE users;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_email.sql").write_text(
        "-- name: migrate-0002-up\n"
        "ALTER TABLE users ADD COLUMN email TEXT;\n"
        "-- name: migrate-0002-down\n"
        "ALTER TABLE users DROP COLUMN email;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0003_posts.sql").write_text(
        "-- name: migrate-0003-up\n"
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER);\n"
        "-- name: migrate-0003-down\n"
        "DROP TABLE posts;\n",
        encoding="utf-8",
    )

    db_path = tmp_path / "partial.db"
    config = SqliteConfig(
        connection_config={"database": str(db_path)}, migration_config={"script_location": str(migrations_dir)}
    )
    commands = SyncMigrationCommands(config)
    commands.upgrade(revision="0001", echo=False)

    commands.squash("0001", "0003", "initial_schema", update_database=False, yes=True)

    partial_commands = SyncMigrationCommands(config)
    with pytest.raises(SquashValidationError, match="0002"):
        partial_commands.upgrade(echo=False)


@pytest.mark.anyio
async def test_async_upgrade_reconciles_fully_applied_and_rejects_partial_squash(tmp_path: Path) -> None:
    """Verify AsyncMigrationCommands reconciles fully applied squash ranges and rejects partial states."""
    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
    from sqlspec.exceptions import SquashValidationError
    from sqlspec.migrations.commands import AsyncMigrationCommands

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_users.sql").write_text(
        "-- name: migrate-0001-up\n"
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE users;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_posts.sql").write_text(
        "-- name: migrate-0002-up\n"
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT);\n"
        "-- name: migrate-0002-down\n"
        "DROP TABLE posts;\n",
        encoding="utf-8",
    )

    full_db = tmp_path / "async_full.db"
    partial_db = tmp_path / "async_partial.db"

    full_config = AiosqliteConfig(
        connection_config={"database": str(full_db)}, migration_config={"script_location": str(migrations_dir)}
    )
    partial_config = AiosqliteConfig(
        connection_config={"database": str(partial_db)}, migration_config={"script_location": str(migrations_dir)}
    )

    await AsyncMigrationCommands(full_config).upgrade(echo=False)
    await AsyncMigrationCommands(partial_config).upgrade(revision="0001", echo=False)

    await AsyncMigrationCommands(full_config).squash("0001", "0002", "initial_schema", update_database=False, yes=True)

    reconcile_full = AsyncMigrationCommands(full_config)
    await reconcile_full.upgrade(echo=False)
    async with full_config.provide_session() as driver:
        applied = await reconcile_full.tracker.get_applied_migrations(driver)
    assert len(applied) == 1
    assert applied[0]["version_num"] == "0001"
    assert applied[0]["replaces"] in {"0001,0002", "0001, 0002"}

    await reconcile_full.downgrade(revision="-1", echo=False)
    async with full_config.provide_session() as driver:
        after_downgrade = await reconcile_full.tracker.get_applied_migrations(driver)
    assert after_downgrade == []

    reconcile_partial = AsyncMigrationCommands(partial_config)
    with pytest.raises(SquashValidationError, match="0002"):
        await reconcile_partial.upgrade(echo=False)


def test_extract_replaces_versions_reads_only_the_sql_header() -> None:
    """Only the leading SQL comment header declares replaced versions; trailing punctuation is stripped."""
    from sqlspec.migrations.runner import extract_replaces_versions

    header = "-- SQLSpec Migration\n\n-- Squashed from: 0001, 0002;\n-- name: migrate-0003-up\nSELECT 1;\n"
    body_comment = "-- name: migrate-0003-up\n-- Squashed from: 0001\nCREATE TABLE t (id INT);\n"
    prose_header = "-- Replaces: the legacy audit table\n-- name: migrate-0004-up\nSELECT 1;\n"
    after_statement = "CREATE TABLE t (id INT);\n-- Squashed from: 0001\n"

    assert extract_replaces_versions(header, ".sql") == ["0001", "0002"]
    assert extract_replaces_versions(body_comment, ".sql") == []
    assert extract_replaces_versions(prose_header, ".sql") == []
    assert extract_replaces_versions('"""Replaces: the old loader."""\n', ".py") == []
    assert extract_replaces_versions(after_statement, ".sql") == []


def test_extract_replaces_versions_reads_python_docstring_or_module_attribute() -> None:
    """Python migrations use the module docstring or ``__replaces__``, never body comments or strings."""
    from sqlspec.migrations.runner import extract_replaces_versions

    attribute_wins = '"""Squashed from: 0001."""\n\n__replaces__ = ("0004", "0005")\n'
    body_only = '"""Plain migration."""\n\n# Replaces: 0001\ndef up():\n    return ["-- Replaces: 0002"]\n'

    assert extract_replaces_versions(attribute_wins, ".py") == ["0004", "0005"]
    assert extract_replaces_versions(body_only, ".py") == []


def test_extract_replaces_versions_rejects_invalid_versions() -> None:
    """A malformed version in the header fails loudly instead of corrupting tracking rows."""
    from sqlspec.exceptions import MigrationError
    from sqlspec.migrations.runner import extract_replaces_versions

    with pytest.raises(MigrationError, match="'see ticket 12'"):
        extract_replaces_versions(
            "-- Squashed from: 0001, see ticket 12\n-- name: migrate-0002-up\nSELECT 1;\n", ".sql"
        )
    with pytest.raises(MigrationError, match="Invalid version '0001 0002"):
        extract_replaces_versions('"""Squashed from: 0001 0002."""\n', ".py")
    with pytest.raises(MigrationError, match="__replaces__"):
        extract_replaces_versions("__replaces__ = VERSIONS\n", ".py")


def test_sync_upgrade_ignores_replaces_comment_in_migration_body(tmp_path: Path) -> None:
    """Replaces-style comments in a header or body neither delete nor rewrite tracking rows."""
    from sqlspec.adapters.sqlite.config import SqliteConfig
    from sqlspec.migrations.commands import SyncMigrationCommands

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_users.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0001-down\nDROP TABLE users;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_posts.sql").write_text(
        "-- Replaces: the legacy users table\n"
        "-- name: migrate-0002-up\n-- Squashed from: 0001\nCREATE TABLE posts (id INTEGER PRIMARY KEY);\n"
        "-- name: migrate-0002-down\nDROP TABLE posts;\n",
        encoding="utf-8",
    )

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "body_comment.db")},
        migration_config={"script_location": str(migrations_dir)},
    )
    commands = SyncMigrationCommands(config)
    commands.upgrade(echo=False)
    SyncMigrationCommands(config).upgrade(echo=False)

    with config.provide_session() as driver:
        applied = commands.tracker.get_applied_migrations(driver)
        tables = {row["name"] for row in driver.select("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert [(row["version_num"], row["replaces"]) for row in applied] == [("0001", None), ("0002", None)]
    assert {"users", "posts"} <= tables
