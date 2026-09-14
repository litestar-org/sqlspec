"""Integration tests for SQLSpec migration CLI configuration and workflows."""

import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

SQLITE_MIGRATION_CONFIG = """
from sqlspec.adapters.sqlite import SqliteConfig

database_config = SqliteConfig(
    bind_key="app",
    connection_config={"database": "app.db"},
    migration_config={"script_location": "migrations", "version_table_name": "schema_versions"},
)
"""


@pytest.fixture
def cli_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Provide an isolated workspace with a test-owned SQLite configuration module."""
    (tmp_path / "database.py").write_text(SQLITE_MIGRATION_CONFIG)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delenv("SQLSPEC_CONFIG", raising=False)
    yield tmp_path
    if "database" in sys.modules:
        del sys.modules["database"]


def test_migration_cli_explicit_config_flag(cli_workspace: Path) -> None:
    """Verify show-config discovers configuration specified via the --config flag."""
    runner = CliRunner()
    result = runner.invoke(add_migration_commands(), ["--config", "database:database_config", "show-config"])
    assert result.exit_code == 0, result.output
    assert "app" in result.output


def test_migration_cli_env_var_config(cli_workspace: Path) -> None:
    """Verify show-config discovers configuration specified via the SQLSPEC_CONFIG environment variable."""
    runner = CliRunner()
    result = runner.invoke(
        add_migration_commands(), ["show-config"], env={"SQLSPEC_CONFIG": "database.database_config"}
    )
    assert result.exit_code == 0, result.output
    assert "app" in result.output


def test_migration_cli_pyproject_discovery(cli_workspace: Path) -> None:
    """Verify show-config discovers configuration defined in pyproject.toml."""
    (cli_workspace / "pyproject.toml").write_text('[tool.sqlspec]\nconfig = "database:database_config"\n')
    runner = CliRunner()
    result = runner.invoke(add_migration_commands(), ["show-config"])
    assert result.exit_code == 0, result.output
    assert "Using config from pyproject.toml" in result.output


def test_migration_cli_init(cli_workspace: Path) -> None:
    """Verify init creates the migrations directory."""
    runner = CliRunner()
    result = runner.invoke(add_migration_commands(), ["--config", "database:database_config", "init", "--no-prompt"])
    assert result.exit_code == 0, result.output
    assert (cli_workspace / "migrations").is_dir()


def test_migration_cli_create_migration(cli_workspace: Path) -> None:
    """Verify create-migration generates a new migration SQL file."""
    runner = CliRunner()
    runner.invoke(add_migration_commands(), ["--config", "database:database_config", "init", "--no-prompt"])
    result = runner.invoke(
        add_migration_commands(),
        ["--config", "database:database_config", "create-migration", "-m", "create users table", "--no-prompt"],
    )
    assert result.exit_code == 0, result.output
    assert len(list((cli_workspace / "migrations").glob("*.sql"))) == 1


def test_migration_cli_upgrade(cli_workspace: Path) -> None:
    """Verify upgrade creates the database file and applies migrations."""
    runner = CliRunner()
    runner.invoke(add_migration_commands(), ["--config", "database:database_config", "init", "--no-prompt"])
    runner.invoke(
        add_migration_commands(),
        ["--config", "database:database_config", "create-migration", "-m", "create users table", "--no-prompt"],
    )
    result = runner.invoke(add_migration_commands(), ["--config", "database:database_config", "upgrade", "--no-prompt"])
    assert result.exit_code == 0, result.output
    assert (cli_workspace / "app.db").is_file()


def test_migration_cli_show_current_revision_and_schema_tracking(cli_workspace: Path) -> None:
    """Verify show-current-revision succeeds and the tracking table exists in SQLite."""
    runner = CliRunner()
    runner.invoke(add_migration_commands(), ["--config", "database:database_config", "init", "--no-prompt"])
    runner.invoke(
        add_migration_commands(),
        ["--config", "database:database_config", "create-migration", "-m", "create users table", "--no-prompt"],
    )
    runner.invoke(add_migration_commands(), ["--config", "database:database_config", "upgrade", "--no-prompt"])
    result = runner.invoke(add_migration_commands(), ["--config", "database:database_config", "show-current-revision"])
    assert result.exit_code == 0, result.output
    with sqlite3.connect(cli_workspace / "app.db") as connection:
        tracker_row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = ? AND name = ?", ("table", "schema_versions")
        ).fetchone()
    assert tracker_row == ("schema_versions",)


def test_migration_cli_quickstart_workflow(cli_workspace: Path) -> None:
    """Verify the complete sequential quickstart workflow as documented."""
    runner = CliRunner()
    config_path = "database:database_config"

    cli_result = runner.invoke(add_migration_commands(), ["--config", config_path, "show-config"])
    assert cli_result.exit_code == 0, cli_result.output
    assert "app" in cli_result.output

    env_result = runner.invoke(
        add_migration_commands(), ["show-config"], env={"SQLSPEC_CONFIG": "database.database_config"}
    )
    assert env_result.exit_code == 0, env_result.output
    assert "app" in env_result.output

    (cli_workspace / "pyproject.toml").write_text('[tool.sqlspec]\nconfig = "database:database_config"\n')
    pyproject_result = runner.invoke(add_migration_commands(), ["show-config"])
    assert pyproject_result.exit_code == 0, pyproject_result.output
    assert "Using config from pyproject.toml" in pyproject_result.output

    commands = (
        ["init", "--no-prompt"],
        ["create-migration", "-m", "create users table", "--no-prompt"],
        ["upgrade", "--no-prompt"],
        ["show-current-revision"],
    )
    for command in commands:
        cmd_result = runner.invoke(add_migration_commands(), ["--config", config_path, *command])
        assert cmd_result.exit_code == 0, cmd_result.output

    assert (cli_workspace / "app.db").is_file()
    assert len(list((cli_workspace / "migrations").glob("*.sql"))) == 1
    with sqlite3.connect(cli_workspace / "app.db") as connection:
        tracker_name = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = ? AND name = ?", ("table", "schema_versions")
        ).fetchone()
    assert tracker_name == ("schema_versions",)
