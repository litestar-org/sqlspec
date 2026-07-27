"""Test extension migration discovery functionality."""

import logging
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import SyncMigrationCommands


def test_extension_migration_discovery(tmp_path: Path) -> None:
    """Test that extension migrations are discovered when configured."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={
            "script_location": str(tmp_path),
            "version_table_name": "test_migrations",
            "include_extensions": ["litestar"],
        },
    )

    commands = SyncMigrationCommands(config)

    assert hasattr(commands, "runner")
    assert hasattr(commands.runner, "extension_migrations")

    if "litestar" in commands.runner.extension_migrations:
        litestar_path = commands.runner.extension_migrations["litestar"]
        assert litestar_path.exists()
        assert litestar_path.name == "migrations"


def test_extension_migration_context(tmp_path: Path) -> None:
    """Test that migration context is created with dialect information."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path), "include_extensions": ["litestar"]},
    )

    commands = SyncMigrationCommands(config)

    assert hasattr(commands.runner, "context")
    assert commands.runner.context is not None
    assert commands.runner.context.dialect == "sqlite"


def test_no_extensions_by_default(tmp_path: Path) -> None:
    """Test that no extension migrations are included by default."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path)}
    )

    commands = SyncMigrationCommands(config)

    assert commands.runner.extension_migrations == {}


def test_migration_file_discovery_with_extensions(tmp_path: Path) -> None:
    """Test that migration files are discovered from both primary and extension paths."""
    migrations_dir = tmp_path

    primary_migration = migrations_dir / "0002_user_table.sql"
    primary_migration.write_text("""
-- name: migrate-0002-up
CREATE TABLE users (id INTEGER);

-- name: migrate-0002-down
DROP TABLE users;
""")

    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(migrations_dir), "include_extensions": ["litestar"]},
    )

    commands = SyncMigrationCommands(config)

    migration_files = commands.runner.get_migration_files()

    versions = [version for version, _ in migration_files]

    assert "0002" in versions


@pytest.fixture
def third_party_migrations(tmp_path: Path) -> Path:
    """Create a migrations directory outside the sqlspec.extensions namespace."""
    ext_dir = tmp_path / "vendor" / "migrations"
    ext_dir.mkdir(parents=True)
    (ext_dir / "ext_litestar_queues_0001_init.sql").write_text("""
-- name: migrate-ext_litestar_queues_0001-up
CREATE TABLE queue_tasks (id INTEGER PRIMARY KEY);

-- name: migrate-ext_litestar_queues_0001-down
DROP TABLE queue_tasks;
""")
    return ext_dir


def test_migrations_path_registers_third_party_directory(tmp_path: Path, third_party_migrations: Path) -> None:
    """A package outside sqlspec.extensions registers migrations via migrations_path."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar_queues": {"migrations_path": str(third_party_migrations)}},
        migration_config={"script_location": str(tmp_path / "migrations")},
    )

    commands = SyncMigrationCommands(config)

    assert commands.runner.extension_migrations["litestar_queues"] == third_party_migrations


def test_migrations_path_auto_includes_without_include_extensions(tmp_path: Path, third_party_migrations: Path) -> None:
    """Declaring migrations_path opts the extension in without touching include_extensions."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar_queues": {"migrations_path": str(third_party_migrations)}},
        migration_config={"script_location": str(tmp_path / "migrations")},
    )

    assert config.migration_config.get("include_extensions") == ["litestar_queues"]


def test_migrations_path_honors_exclude_extensions(tmp_path: Path, third_party_migrations: Path) -> None:
    """exclude_extensions opts a third-party extension back out of auto-inclusion."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar_queues": {"migrations_path": str(third_party_migrations)}},
        migration_config={"script_location": str(tmp_path / "migrations"), "exclude_extensions": ["litestar_queues"]},
    )

    assert SyncMigrationCommands(config).runner.extension_migrations == {}


def test_migrations_path_accepts_module_specification(tmp_path: Path) -> None:
    """A '<dotted.module>:<subdir>' value resolves against the installed package."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"vendor_ext": {"migrations_path": "sqlspec.extensions.adk:migrations"}},
        migration_config={"script_location": str(tmp_path / "migrations")},
    )

    resolved = SyncMigrationCommands(config).runner.extension_migrations["vendor_ext"]
    assert resolved.is_dir()
    assert resolved.name == "migrations"


def test_migrations_path_overrides_namespace_convention(tmp_path: Path, third_party_migrations: Path) -> None:
    """An explicit migrations_path wins over the sqlspec.extensions.<name> lookup."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar": {"session_table": "s", "migrations_path": str(third_party_migrations)}},
        migration_config={"script_location": str(tmp_path / "migrations")},
    )

    assert SyncMigrationCommands(config).runner.extension_migrations["litestar"] == third_party_migrations


def test_missing_migrations_path_warns_and_registers_nothing(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """A migrations_path pointing nowhere warns instead of silently doing nothing."""
    caplog.set_level(logging.DEBUG, logger="sqlspec.migrations.base")
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"vendor_ext": {"migrations_path": str(tmp_path / "absent")}},
        migration_config={"script_location": str(tmp_path / "migrations")},
    )

    commands = SyncMigrationCommands(config)

    assert commands.runner.extension_migrations == {}
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("vendor_ext" in message and "not an existing directory" in message for message in warnings)


def test_invalid_migrations_path_raises(tmp_path: Path) -> None:
    """A migrations_path of the wrong type is a configuration error, not a warning."""
    with pytest.raises(MigrationError, match="invalid migrations_path of type int"):
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"vendor_ext": {"migrations_path": 42}},
            migration_config={"script_location": str(tmp_path / "migrations")},
        )


def test_unresolvable_extension_warning_names_module_and_consequence(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The warning states which module was tried and that nothing was registered."""
    caplog.set_level(logging.DEBUG, logger="sqlspec.migrations.base")
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path), "include_extensions": ["litestar_queues"]},
    )

    commands = SyncMigrationCommands(config)

    assert commands.runner.extension_migrations == {}
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        "sqlspec.extensions.litestar_queues" in message
        and "no migrations were registered" in message
        and "migrations_path" in message
        for message in warnings
    )


def test_shipped_extension_without_migrations_does_not_warn(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Extensions that legitimately ship no migrations are not a warning condition."""
    caplog.set_level(logging.DEBUG, logger="sqlspec.migrations.base")
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        migration_config={"script_location": str(tmp_path), "include_extensions": ["fastapi"]},
    )

    commands = SyncMigrationCommands(config)

    assert commands.runner.extension_migrations == {}
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == []


def test_add_extension_migrations_registers_after_construction(tmp_path: Path, third_party_migrations: Path) -> None:
    """The public API registers an extension without touching runner internals."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path / "migrations")}
    )
    assert config.get_migration_commands().runner.extension_migrations == {}

    config.add_extension_migrations("litestar_queues", third_party_migrations, settings={"table_name": "queue_tasks"})

    commands = config.get_migration_commands()
    assert commands.runner.extension_migrations["litestar_queues"] == third_party_migrations
    assert commands.extension_configs["litestar_queues"]["table_name"] == "queue_tasks"
    assert config.migration_config.get("include_extensions") == ["litestar_queues"]


def test_add_extension_migrations_merges_existing_settings(tmp_path: Path, third_party_migrations: Path) -> None:
    """Settings already registered under the extension name are preserved."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar_queues": {"table_name": "original", "poll": 5}},
        migration_config={"script_location": str(tmp_path / "migrations")},
    )

    config.add_extension_migrations("litestar_queues", third_party_migrations, settings={"table_name": "updated"})

    settings = config.get_migration_commands().extension_configs["litestar_queues"]
    assert settings["table_name"] == "updated"
    assert settings["poll"] == 5


def test_add_extension_migrations_preserves_loaded_sql(tmp_path: Path, third_party_migrations: Path) -> None:
    """Rebuilding commands must not discard SQL already loaded into the migration loader."""
    config = SqliteConfig(
        connection_config={"database": ":memory:"}, migration_config={"script_location": str(tmp_path / "migrations")}
    )
    loader = config._ensure_migration_loader()
    loader.add_named_sql("probe", "SELECT 1")

    config.add_extension_migrations("litestar_queues", third_party_migrations)

    assert "probe" in config._ensure_migration_loader().list_queries()


def test_auto_inclusion_does_not_mutate_caller_include_list(tmp_path: Path, third_party_migrations: Path) -> None:
    """A shared include_extensions list is not mutated when configs auto-include extensions."""
    shared = ["fastapi"]

    SqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar_queues": {"migrations_path": str(third_party_migrations)}},
        migration_config={"script_location": str(tmp_path / "migrations"), "include_extensions": shared},
    )

    assert shared == ["fastapi"]
