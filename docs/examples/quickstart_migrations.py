import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

__all__ = ("test_sqlite_migration_quickstart",)


def test_sqlite_migration_quickstart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise the documented config discovery and migration workflow."""
    config_example = Path(__file__).with_name("migration_quickstart_config.py")
    (tmp_path / "database.py").write_text(config_example.read_text())
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SQLSPEC_CONFIG", raising=False)
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

    (tmp_path / "pyproject.toml").write_text('[tool.sqlspec]\nconfig = "database:database_config"\n')
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
        result = runner.invoke(add_migration_commands(), ["--config", config_path, *command])
        assert result.exit_code == 0, result.output

    assert (tmp_path / "app.db").is_file()
    assert len(list((tmp_path / "migrations").glob("*.sql"))) == 1
    with sqlite3.connect(tmp_path / "app.db") as connection:
        tracker_name = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = ? AND name = ?", ("table", "schema_versions")
        ).fetchone()
    assert tracker_name == ("schema_versions",)
