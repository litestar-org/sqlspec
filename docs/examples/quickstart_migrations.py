"""Quickstart example demonstrating the SQLSpec migration CLI workflow."""

import os
import sys
import tempfile
from pathlib import Path

from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

__all__ = ("run_migration_quickstart",)


def run_migration_quickstart() -> None:
    """Execute the standard migration workflow commands against a temporary SQLite database."""
    config_example = Path(__file__).with_name("migration_quickstart_config.py")
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        (temp_path / "database.py").write_text(config_example.read_text())
        old_cwd = Path.cwd()
        sys.path.insert(0, str(temp_path))
        os.chdir(temp_path)
        try:
            config_path = "database:database_config"
            runner.invoke(add_migration_commands(), ["--config", config_path, "show-config"])
            runner.invoke(add_migration_commands(), ["--config", config_path, "init", "--no-prompt"])
            runner.invoke(
                add_migration_commands(),
                ["--config", config_path, "create-migration", "-m", "create users table", "--no-prompt"],
            )
            runner.invoke(add_migration_commands(), ["--config", config_path, "upgrade", "--no-prompt"])
            runner.invoke(add_migration_commands(), ["--config", config_path, "show-current-revision"])
        finally:
            os.chdir(old_cwd)
            if str(temp_path) in sys.path:
                sys.path.remove(str(temp_path))


if __name__ == "__main__":
    run_migration_quickstart()
