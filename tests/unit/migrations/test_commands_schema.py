"""Unit tests for schema dump, verification, and schema/data revision bootstrap in migration commands."""

from pathlib import Path
from unittest.mock import patch

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.utils.serializers import to_json


def test_sync_dump_schema_writes_topological_ddl_and_excludes_migrations_table(tmp_path: Path) -> None:
    """SyncMigrationCommands.dump_schema writes topological DDL and excludes the tracking table by default."""
    db_path = tmp_path / "sync_dump.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    schema_path = tmp_path / "schema.sql"

    (migrations_dir / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\n"
        "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n"
        "CREATE TABLE members (id INTEGER PRIMARY KEY, team_id INTEGER NOT NULL REFERENCES teams(id));\n\n"
        "-- name: migrate-0001-down\n"
        "DROP TABLE members;\n"
        "DROP TABLE teams;\n",
        encoding="utf-8",
    )

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    commands = SyncMigrationCommands(config)
    commands.upgrade(echo=False)

    rendered = commands.dump_schema(output_path=schema_path, echo=False)
    assert schema_path.exists()
    assert schema_path.read_text(encoding="utf-8") == rendered
    assert "-- SQLSpec Schema Dump" in rendered
    assert "CREATE TABLE teams" in rendered
    assert "CREATE TABLE members" in rendered
    assert "ddl_migrations" not in rendered
    assert rendered.index("CREATE TABLE teams") < rendered.index("CREATE TABLE members")

    with_tracking = commands.dump_schema(include_migrations_table=True, echo=False)
    assert "ddl_migrations" in with_tracking


@pytest.mark.anyio
async def test_async_dump_schema_uses_configured_schema_dump_path(tmp_path: Path) -> None:
    """AsyncMigrationCommands.dump_schema writes to migration_config['schema_dump_path'] when output_path is omitted."""
    db_path = tmp_path / "async_dump.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    configured_dump = tmp_path / "configured_schema.sql"

    (migrations_dir / "0001_init.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE projects (id INTEGER PRIMARY KEY, title TEXT NOT NULL);\n",
        encoding="utf-8",
    )

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migrations_dir),
            "enabled": True,
            "schema_dump_path": str(configured_dump),
        },
    )
    commands = AsyncMigrationCommands(config)
    await commands.upgrade(echo=False)

    rendered = await commands.dump_schema(echo=False)
    assert configured_dump.exists()
    assert configured_dump.read_text(encoding="utf-8") == rendered
    assert "CREATE TABLE projects" in rendered
    assert "ddl_migrations" not in rendered
    await config.close_pool()


def test_sync_verify_detects_checksum_drift_and_orphaned_records(tmp_path: Path) -> None:
    """SyncMigrationCommands.verify reports clean, pending, checksum drift, and orphaned migration states."""
    db_path = tmp_path / "sync_verify.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    mig1 = migrations_dir / "0001_users.sql"
    mig2 = migrations_dir / "0002_posts.sql"
    mig1.write_text("-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n", encoding="utf-8")
    mig2.write_text("-- name: migrate-0002-up\nCREATE TABLE posts (id INTEGER PRIMARY KEY);\n", encoding="utf-8")

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    commands = SyncMigrationCommands(config)

    pending_report = commands.verify(echo=False)
    assert pending_report["valid"] is True
    assert pending_report["pending_versions"] == ["0001", "0002"]

    commands.upgrade(echo=False)
    clean_report = commands.verify(strict=True, echo=False)
    assert clean_report["valid"] is True
    assert clean_report["checksum_mismatches"] == []
    assert clean_report["orphaned_records"] == []
    assert clean_report["pending_versions"] == []
    assert clean_report["partial_squash_errors"] == []

    mig1.write_text(
        "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n", encoding="utf-8"
    )
    drift_report = commands.verify(echo=False)
    assert drift_report["valid"] is False
    assert len(drift_report["checksum_mismatches"]) == 1
    assert drift_report["checksum_mismatches"][0]["version"] == "0001"

    with pytest.raises(MigrationError, match="Verification failed"):
        commands.verify(strict=True, echo=False)

    mig1.write_text("-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n", encoding="utf-8")
    mig2.unlink()
    orphan_report = commands.verify(echo=False)
    assert orphan_report["valid"] is False
    assert orphan_report["orphaned_records"] == ["0002"]

    with pytest.raises(MigrationError, match="Verification failed"):
        commands.verify(strict=True, echo=False)


@pytest.mark.anyio
async def test_async_verify_handles_squashed_migrations_and_strict_mode(tmp_path: Path) -> None:
    """AsyncMigrationCommands.verify succeeds after squash and raises MigrationError on checksum tamper."""
    db_path = tmp_path / "async_verify.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    (migrations_dir / "0001_users.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n\n"
        "-- name: migrate-0001-down\nDROP TABLE users;\n",
        encoding="utf-8",
    )
    (migrations_dir / "0002_alter.sql").write_text(
        "-- name: migrate-0002-up\nALTER TABLE users ADD COLUMN name TEXT;\n\n"
        "-- name: migrate-0002-down\nALTER TABLE users DROP COLUMN name;\n",
        encoding="utf-8",
    )

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    commands = AsyncMigrationCommands(config)
    await commands.upgrade(echo=False)
    await commands.squash(yes=True)

    squashed_report = await commands.verify(strict=True, echo=False)
    assert squashed_report["valid"] is True
    assert squashed_report["checksum_mismatches"] == []
    assert squashed_report["orphaned_records"] == []
    await config.close_pool()


def test_sync_revision_from_schema_populates_sql_and_python_migrations(tmp_path: Path) -> None:
    """SyncMigrationCommands.revision(from_schema=True) populates CREATE and DROP DDL in .sql and .py files."""
    db_path = tmp_path / "from_schema.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    with config.provide_session() as driver:
        driver.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
        driver.execute(
            "CREATE TABLE articles (id INTEGER PRIMARY KEY, category_id INTEGER NOT NULL REFERENCES categories(id))"
        )
        driver.commit()

    commands = SyncMigrationCommands(config)
    sql_file = commands.revision("initial_baseline", file_type="sql", from_schema=True)
    assert isinstance(sql_file, Path)
    assert sql_file.exists()
    sql_content = sql_file.read_text(encoding="utf-8")
    assert "CREATE TABLE categories" in sql_content
    assert "CREATE TABLE articles" in sql_content
    assert "DROP TABLE IF EXISTS articles;" in sql_content
    assert "DROP TABLE IF EXISTS categories;" in sql_content

    sql_file.unlink()
    py_file = commands.revision("initial_baseline_py", file_type="py", from_schema=True)
    assert isinstance(py_file, Path)
    assert py_file.exists()
    py_content = py_file.read_text(encoding="utf-8")
    assert "CREATE TABLE categories" in py_content
    assert "DROP TABLE IF EXISTS categories;" in py_content


@pytest.mark.anyio
async def test_async_revision_from_data_file_and_staging_table(tmp_path: Path) -> None:
    """AsyncMigrationCommands.revision(from_data=...) generates typed DDL from a JSON fixture or staging table."""
    db_path = tmp_path / "from_data.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    fixture_file = tmp_path / "seed_users.json"
    fixture_file.write_text(
        to_json([
            {"id": 1, "email": "alice@example.com", "active": True},
            {"id": 2, "email": "bob@example.com", "active": False},
        ]),
        encoding="utf-8",
    )

    config = AiosqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    async with config.provide_session() as driver:
        await driver.execute("CREATE TABLE raw_staging_users (id TEXT, score TEXT)")
        await driver.execute("INSERT INTO raw_staging_users (id, score) VALUES ('10', '98.5')")
        await driver.execute("INSERT INTO raw_staging_users (id, score) VALUES ('20', '87.25')")
        await driver.commit()

    commands = AsyncMigrationCommands(config)
    from_file_path = await commands.revision("seed_users", from_data=fixture_file, target_table="users")
    assert from_file_path.exists()
    file_ddl = from_file_path.read_text(encoding="utf-8")
    assert "CREATE TABLE users" in file_ddl
    assert "email" in file_ddl
    assert "DROP TABLE IF EXISTS users;" in file_ddl

    from_file_path.unlink()
    from_table_path = await commands.revision("refine_staging", from_data="raw_staging_users", target_table="users")
    assert from_table_path.exists()
    table_ddl = from_table_path.read_text(encoding="utf-8")
    assert 'CREATE TABLE "users"' in table_ddl or "CREATE TABLE users" in table_ddl
    assert "REAL(10, 2)" in table_ddl or "NUMERIC" in table_ddl
    assert "DROP TABLE IF EXISTS users;" in table_ddl
    await config.close_pool()


def test_upgrade_auto_dump_schema_writes_schema_sql(tmp_path: Path) -> None:
    """SyncMigrationCommands.upgrade automatically dumps schema.sql when auto_dump_schema is enabled."""
    db_path = tmp_path / "auto_dump.db"
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    (migrations_dir / "0001_accounts.sql").write_text(
        "-- name: migrate-0001-up\nCREATE TABLE accounts (id INTEGER PRIMARY KEY, code TEXT NOT NULL);\n",
        encoding="utf-8",
    )

    config = SqliteConfig(
        connection_config={"database": str(db_path)},
        migration_config={"script_location": str(migrations_dir), "enabled": True, "auto_dump_schema": True},
    )
    commands = SyncMigrationCommands(config)
    commands.upgrade(echo=False)

    expected_dump = migrations_dir / "schema.sql"
    assert expected_dump.exists()
    assert "CREATE TABLE accounts" in expected_dump.read_text(encoding="utf-8")


def test_squash_and_fix_use_rich_confirm_ask(tmp_path: Path) -> None:
    """SyncMigrationCommands.squash and fix use Rich Confirm.ask when yes=False."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    mig_file = migrations_dir / "20251011120000_init.sql"
    mig_file.write_text("-- name: migrate-20251011120000-up\nCREATE TABLE items (id INT);\n", encoding="utf-8")

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "confirm.db")},
        migration_config={"script_location": str(migrations_dir), "enabled": True},
    )
    commands = SyncMigrationCommands(config)

    with patch("rich.prompt.Confirm.ask", return_value=False) as mock_confirm:
        commands.squash(description="test_squash", yes=False, update_database=False)
        mock_confirm.assert_called_once()
        assert mig_file.exists()

    with patch("rich.prompt.Confirm.ask", return_value=False) as mock_confirm:
        commands.fix(yes=False, update_database=False)
        mock_confirm.assert_called_once()
        assert mig_file.exists()
