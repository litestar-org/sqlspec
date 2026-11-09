"""Quickstart example demonstrating the SQLSpec migration CLI workflow."""

import os
import sys
import tempfile
from pathlib import Path

from click.testing import CliRunner

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.cli import add_migration_commands
from sqlspec.migrations import create_migration_commands

__all__ = ("main", "reconcile_example")


def reconcile_example() -> None:
    """Execute opt-in extension reconciliation alongside a numbered revision."""
    # --8<-- [start:reconcile]
    with tempfile.TemporaryDirectory() as root_dir:
        root = Path(root_dir)
        migrations = root / "migrations"
        migrations.mkdir()
        (migrations / "0001_items.sql").write_text(
            "-- name: migrate-0001-up\n"
            "CREATE TABLE items (id INTEGER PRIMARY KEY);\n"
            "-- name: migrate-0001-down\n"
            "DROP TABLE items;\n",
            encoding="utf-8",
        )
        ext_dir = root / "audit_ext"
        ext_dir.mkdir()
        (ext_dir / "reconcile.py").write_text(
            "from sqlspec.migrations.context import MigrationContext\n"
            "from sqlspec.migrations.schema import (\n"
            "    SchemaTarget,\n"
            "    ensure_schema_sync,\n"
            ")\n\n"
            "def reconcile(context: MigrationContext) -> None:\n"
            "    if context.driver is None:\n"
            "        return\n"
            "    target = SchemaTarget.from_ddl(\n"
            "        'audit_runs',\n"
            "        'CREATE TABLE audit_runs '\n"
            "        '(id INTEGER PRIMARY KEY, note TEXT)',\n"
            "        dialect='sqlite',\n"
            "    )\n"
            "    ensure_schema_sync(\n"
            "        context.driver,\n"
            "        [target],\n"
            "        manage_schema=True,\n"
            "        commit=False,\n"
            "        strict=True,\n"
            "    )\n"
            "    context.driver.execute(\n"
            "        'INSERT INTO audit_runs (note) VALUES (:note)',\n"
            "        note='head',\n"
            "    )\n",
            encoding="utf-8",
        )
        config = SqliteConfig(
            connection_config={"database": str(root / "app.db")},
            migration_config={"script_location": str(migrations), "echo": False},
        )
        audit_extension_migration_settings = {"manage_schema": True}
        config.add_extension_migrations(
            "audit_ext",
            migrations_path=ext_dir,
            settings=audit_extension_migration_settings,
            migration_mode="reconcile",
        )
        try:
            commands = create_migration_commands(config)
            commands.upgrade(echo=False)
            commands.upgrade(echo=False)
            with config.provide_session() as session:
                runs = session.select_value("SELECT COUNT(*) FROM audit_runs")
                rows = session.select("SELECT version_num FROM ddl_migrations")
                ledger = sorted(row["version_num"] for row in rows)
            print(f"callback runs: {runs}, app ledger: {ledger}")
            assert runs == 2
            assert ledger == ["0001"]
        finally:
            config.close_pool()
    # --8<-- [end:reconcile]


def main() -> None:
    """Execute the standard migration workflow commands against a temporary SQLite database."""
    config_example = Path(__file__).with_name("migration_quickstart_config.py")
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        (temp_path / "database.py").write_text(config_example.read_text(encoding="utf-8"), encoding="utf-8")
        old_cwd = Path.cwd()
        sys.path.insert(0, str(temp_path))
        os.chdir(temp_path)
        try:
            # --8<-- [start:example]
            config_path = "database:database_config"
            commands = (
                ["show-config"],
                ["init", "--no-prompt"],
                ["create-migration", "-m", "create users table", "--no-prompt"],
                ["upgrade", "--no-prompt"],
                ["show-current-revision"],
            )
            for command in commands:
                result = runner.invoke(add_migration_commands(), ["--config", config_path, *command])
                print(result.output, end="")
                # --8<-- [end:example]
                if result.exit_code:
                    message = f"Migration command failed: {' '.join(command)}"
                    raise RuntimeError(message) from result.exception
        finally:
            os.chdir(old_cwd)
            if str(temp_path) in sys.path:
                sys.path.remove(str(temp_path))
            sys.modules.pop("database", None)
    reconcile_example()


if __name__ == "__main__":
    main()
