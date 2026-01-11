"""Integration tests for CLI commands with sync adapters.

These tests verify that sync adapters (SQLite, DuckDB) work correctly with CLI
migration commands without the async/sync conflict error that was fixed in the
CLI refactoring. This is a regression test to ensure sync adapters don't fail
with: "await_ cannot be called from within an async task running on the same
event loop"

The tests use the full CLI workflow: init -> create-migration -> upgrade -> downgrade
"""

import sys
import uuid
from collections.abc import Generator
from pathlib import Path

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

MODULE_PREFIX = "cli_sync_adapter_test_"


@pytest.fixture
def temp_project_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Create a temporary project directory with cleanup."""
    monkeypatch.chdir(tmp_path)
    yield tmp_path


@pytest.fixture
def cleanup_test_modules() -> Generator[None, None, None]:
    """Fixture to clean up test modules from sys.modules after each test."""
    modules_before = set(sys.modules.keys())
    yield
    modules_after = set(sys.modules.keys())
    test_modules = {m for m in modules_after - modules_before if m.startswith(MODULE_PREFIX)}
    for module in test_modules:
        if module in sys.modules:
            del sys.modules[module]


def _create_config_module(content: str, directory: Path) -> str:
    """Create a temporary Python module with config content."""
    module_name = f"{MODULE_PREFIX}{uuid.uuid4().hex}"
    (directory / f"{module_name}.py").write_text(content)
    return module_name


def test_sqlite_full_migration_workflow(temp_project_dir: Path, cleanup_test_modules: None) -> None:
    """Test full CLI workflow with SQLite: init -> create-migration -> upgrade -> show-revision.

    This is the primary regression test for the async/sync conflict fix.
    The original bug caused this error:
    "await_ cannot be called from within an async task running on the same event loop"
    """
    runner = CliRunner()
    migrations_dir = temp_project_dir / "migrations"
    db_path = temp_project_dir / "test.db"

    config_module = f"""
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    return SqliteConfig(
        bind_key="sqlite_workflow_test",
        connection_config={{"database": "{db_path}"}},
        migration_config={{
            "enabled": True,
            "script_location": "{migrations_dir}"
        }}
    )
"""
    module_name = _create_config_module(config_module, temp_project_dir)
    config_path = f"{module_name}.get_config"

    # Step 1: Initialize migrations
    init_result = runner.invoke(add_migration_commands(), ["--config", config_path, "init", "--no-prompt"])
    assert init_result.exit_code == 0, f"Init failed: {init_result.output}"
    assert "await_ cannot be called" not in init_result.output
    assert migrations_dir.exists(), "Migrations directory was not created"

    # Step 2: Create a migration
    create_result = runner.invoke(
        add_migration_commands(),
        ["--config", config_path, "create-migration", "-m", "create users table", "--no-prompt"],
    )
    assert create_result.exit_code == 0, f"Create migration failed: {create_result.output}"
    assert "await_ cannot be called" not in create_result.output

    # Verify migration file was created
    migration_files = list(migrations_dir.glob("*.sql"))
    assert len(migration_files) == 1, f"Expected 1 migration file, found {len(migration_files)}"

    # Step 3: Show current revision (should be None/empty before upgrade)
    show_result = runner.invoke(add_migration_commands(), ["--config", config_path, "show-current-revision"])
    assert show_result.exit_code == 0, f"Show revision failed: {show_result.output}"
    assert "await_ cannot be called" not in show_result.output

    # Step 4: Upgrade to head
    upgrade_result = runner.invoke(add_migration_commands(), ["--config", config_path, "upgrade", "--no-prompt"])
    assert upgrade_result.exit_code == 0, f"Upgrade failed: {upgrade_result.output}"
    assert "await_ cannot be called" not in upgrade_result.output

    # Step 5: Verify we're at the new revision
    show_after_result = runner.invoke(add_migration_commands(), ["--config", config_path, "show-current-revision"])
    assert show_after_result.exit_code == 0, f"Show revision after upgrade failed: {show_after_result.output}"
    assert "await_ cannot be called" not in show_after_result.output


def test_duckdb_full_migration_workflow(temp_project_dir: Path, cleanup_test_modules: None) -> None:
    """Test full CLI workflow with DuckDB: init -> create-migration -> upgrade.

    DuckDB is another sync adapter that was affected by the async/sync conflict.
    """
    runner = CliRunner()
    migrations_dir = temp_project_dir / "migrations"
    db_path = temp_project_dir / "test.duckdb"

    config_module = f"""
from sqlspec.adapters.duckdb.config import DuckDBConfig

def get_config():
    return DuckDBConfig(
        bind_key="duckdb_workflow_test",
        connection_config={{"database": "{db_path}"}},
        migration_config={{
            "enabled": True,
            "script_location": "{migrations_dir}"
        }}
    )
"""
    module_name = _create_config_module(config_module, temp_project_dir)
    config_path = f"{module_name}.get_config"

    # Step 1: Initialize migrations
    init_result = runner.invoke(add_migration_commands(), ["--config", config_path, "init", "--no-prompt"])
    assert init_result.exit_code == 0, f"Init failed: {init_result.output}"
    assert "await_ cannot be called" not in init_result.output

    # Step 2: Create a migration
    create_result = runner.invoke(
        add_migration_commands(),
        ["--config", config_path, "create-migration", "-m", "create products table", "--no-prompt"],
    )
    assert create_result.exit_code == 0, f"Create migration failed: {create_result.output}"
    assert "await_ cannot be called" not in create_result.output

    # Step 3: Upgrade
    upgrade_result = runner.invoke(add_migration_commands(), ["--config", config_path, "upgrade", "--no-prompt"])
    assert upgrade_result.exit_code == 0, f"Upgrade failed: {upgrade_result.output}"
    assert "await_ cannot be called" not in upgrade_result.output


def test_sqlite_upgrade_downgrade_cycle(temp_project_dir: Path, cleanup_test_modules: None) -> None:
    """Test upgrade and downgrade cycle with SQLite sync adapter."""
    runner = CliRunner()
    migrations_dir = temp_project_dir / "migrations"
    db_path = temp_project_dir / "cycle_test.db"

    config_module = f"""
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    return SqliteConfig(
        bind_key="sqlite_cycle_test",
        connection_config={{"database": "{db_path}"}},
        migration_config={{
            "enabled": True,
            "script_location": "{migrations_dir}"
        }}
    )
"""
    module_name = _create_config_module(config_module, temp_project_dir)
    config_path = f"{module_name}.get_config"

    # Initialize and create migration
    runner.invoke(add_migration_commands(), ["--config", config_path, "init", "--no-prompt"])
    runner.invoke(
        add_migration_commands(), ["--config", config_path, "create-migration", "-m", "initial", "--no-prompt"]
    )

    # Upgrade
    upgrade_result = runner.invoke(add_migration_commands(), ["--config", config_path, "upgrade", "--no-prompt"])
    assert upgrade_result.exit_code == 0, f"Upgrade failed: {upgrade_result.output}"
    assert "await_ cannot be called" not in upgrade_result.output

    # Downgrade
    downgrade_result = runner.invoke(add_migration_commands(), ["--config", config_path, "downgrade", "--no-prompt"])
    assert downgrade_result.exit_code == 0, f"Downgrade failed: {downgrade_result.output}"
    assert "await_ cannot be called" not in downgrade_result.output


def test_multi_sync_adapter_workflow(temp_project_dir: Path, cleanup_test_modules: None) -> None:
    """Test CLI workflow with multiple sync adapters (SQLite + DuckDB).

    This tests the _partition_configs_by_async logic for sync-only configs.
    """
    runner = CliRunner()
    sqlite_migrations = temp_project_dir / "sqlite_migrations"
    duckdb_migrations = temp_project_dir / "duckdb_migrations"
    sqlite_db = temp_project_dir / "multi_sqlite.db"
    duckdb_db = temp_project_dir / "multi_duckdb.duckdb"

    config_module = f"""
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig

def get_configs():
    return [
        SqliteConfig(
            bind_key="sqlite_multi",
            connection_config={{"database": "{sqlite_db}"}},
            migration_config={{
                "enabled": True,
                "script_location": "{sqlite_migrations}"
            }}
        ),
        DuckDBConfig(
            bind_key="duckdb_multi",
            connection_config={{"database": "{duckdb_db}"}},
            migration_config={{
                "enabled": True,
                "script_location": "{duckdb_migrations}"
            }}
        )
    ]
"""
    module_name = _create_config_module(config_module, temp_project_dir)
    config_path = f"{module_name}.get_configs"

    # Initialize both
    init_result = runner.invoke(add_migration_commands(), ["--config", config_path, "init", "--no-prompt"])
    assert init_result.exit_code == 0, f"Init failed: {init_result.output}"
    assert "await_ cannot be called" not in init_result.output
    assert sqlite_migrations.exists(), "SQLite migrations directory not created"
    assert duckdb_migrations.exists(), "DuckDB migrations directory not created"

    # Create migrations for each (using bind-key to target specific config)
    for bind_key in ["sqlite_multi", "duckdb_multi"]:
        create_result = runner.invoke(
            add_migration_commands(),
            ["--config", config_path, "create-migration", "--bind-key", bind_key, "-m", "initial", "--no-prompt"],
        )
        assert create_result.exit_code == 0, f"Create migration for {bind_key} failed: {create_result.output}"

    # Upgrade all configs
    upgrade_result = runner.invoke(add_migration_commands(), ["--config", config_path, "upgrade", "--no-prompt"])
    assert upgrade_result.exit_code == 0, f"Multi-config upgrade failed: {upgrade_result.output}"
    assert "await_ cannot be called" not in upgrade_result.output


def test_sqlite_stamp_command(temp_project_dir: Path, cleanup_test_modules: None) -> None:
    """Test CLI stamp command works with SQLite sync adapter."""
    runner = CliRunner()
    migrations_dir = temp_project_dir / "migrations"
    db_path = temp_project_dir / "stamp_test.db"

    config_module = f"""
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    return SqliteConfig(
        bind_key="sqlite_stamp_test",
        connection_config={{"database": "{db_path}"}},
        migration_config={{
            "enabled": True,
            "script_location": "{migrations_dir}"
        }}
    )
"""
    module_name = _create_config_module(config_module, temp_project_dir)
    config_path = f"{module_name}.get_config"

    # Initialize and create migration
    runner.invoke(add_migration_commands(), ["--config", config_path, "init", "--no-prompt"])
    runner.invoke(
        add_migration_commands(), ["--config", config_path, "create-migration", "-m", "stamp test", "--no-prompt"]
    )

    # Get the version number from the created migration file
    migration_files = list(migrations_dir.glob("*.sql"))
    assert len(migration_files) == 1
    version = migration_files[0].name.split("_")[0]

    # Stamp the database with that version (without running the migration)
    stamp_result = runner.invoke(add_migration_commands(), ["--config", config_path, "stamp", version])
    assert stamp_result.exit_code == 0, f"Stamp failed: {stamp_result.output}"
    assert "await_ cannot be called" not in stamp_result.output
