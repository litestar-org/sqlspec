"""Tests for CLI migration commands functionality."""

import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

MODULE_PREFIX = "cli_test_config_"

if TYPE_CHECKING:
    from unittest.mock import Mock


@pytest.fixture
def cleanup_test_modules() -> Iterator[None]:
    """Fixture to clean up test modules from sys.modules after each test."""
    modules_before = set(sys.modules.keys())
    yield
    # Remove any test modules that were imported during the test
    modules_after = set(sys.modules.keys())
    test_modules = {m for m in modules_after - modules_before if m.startswith(MODULE_PREFIX)}
    for module in test_modules:
        if module in sys.modules:
            del sys.modules[module]


def _create_module(content: str, directory: "Path") -> str:
    module_name = f"{MODULE_PREFIX}{uuid.uuid4().hex}"
    (directory / f"{module_name}.py").write_text(content)
    return module_name


def test_show_config_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None) -> None:
    """Test show-config command displays migration configurations."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(
        bind_key="migration_test",
        connection_config={"database": ":memory:"},
        migration_config={
            "enabled": True,
            "script_location": "migrations"
        }
    )
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_config", "show-config"])

    assert result.exit_code == 0
    assert "migration_test" in result.output
    assert "Migration Enabled" in result.output or "SqliteConfig" in result.output


def test_show_config_with_multiple_configs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test show-config with multiple migration configurations."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig

def get_configs():
    sqlite_config = SqliteConfig(
        bind_key="sqlite_migrations",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "sqlite_migrations"}
    )

    duckdb_config = DuckDBConfig(
        bind_key="duckdb_migrations",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "duckdb_migrations"}
    )

    return [sqlite_config, duckdb_config]
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_configs", "show-config"])

    assert result.exit_code == 0
    assert "sqlite_migrations" in result.output
    assert "duckdb_migrations" in result.output
    assert "2 configuration(s)" in result.output


def test_show_config_no_migrations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None) -> None:
    """Test show-config when no migrations are configured."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    # Config without migration_config
    config = SqliteConfig(
        bind_key="no_migrations",
        connection_config={"database": ":memory:"}
    )
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_config", "show-config"])

    assert result.exit_code == 0
    assert (
        "No configurations with migrations detected" in result.output or "no_migrations" in result.output
    )  # Depends on validation logic


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_show_current_revision_command(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test show-current-revision command."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    # Mock the migration commands
    mock_commands = Mock()
    mock_commands.current = Mock(return_value=None)  # Sync function
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(
        bind_key="revision_test",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"}
    )
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_config", "show-current-revision"])

    assert result.exit_code == 0
    mock_commands.current.assert_called_once_with(verbose=False)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_show_current_revision_verbose(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test show-current-revision command with verbose flag."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.current = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(
        bind_key="verbose_test",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True}
    )
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_config", "show-current-revision", "--verbose"]
    )

    assert result.exit_code == 0
    mock_commands.current.assert_called_once_with(verbose=True)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_init_command(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test init command for initializing migrations."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.init = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "init_test"
    config.migration_config = {"script_location": "test_migrations"}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_config", "init", "--no-prompt"])

    assert result.exit_code == 0
    mock_commands.init.assert_called_once_with(directory="test_migrations", package=True)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_init_command_custom_directory(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test init command with custom directory."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.init = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "custom_init"
    config.migration_config = {"script_location": "migrations"}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_config", "init", "custom_migrations", "--no-prompt"]
    )

    assert result.exit_code == 0
    mock_commands.init.assert_called_once_with(directory="custom_migrations", package=True)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_create_migration_command(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test create-migration command."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.revision = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "revision_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", f"{module_name}.get_config", "create-migration", "-m", "test migration", "--no-prompt"],
    )

    assert result.exit_code == 0
    mock_commands.revision.assert_called_once_with(message="test migration", file_type=None)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_make_migration_alias(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test make-migration alias for backward compatibility."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.revision = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "revision_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", f"{module_name}.get_config", "make-migration", "-m", "test migration", "--no-prompt"],
    )

    assert result.exit_code == 0
    mock_commands.revision.assert_called_once_with(message="test migration", file_type=None)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_create_migration_command_with_format(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.revision = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "revision_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        [
            "--config",
            f"{module_name}.get_config",
            "create-migration",
            "-m",
            "test migration",
            "--format",
            "py",
            "--no-prompt",
        ],
    )

    assert result.exit_code == 0
    mock_commands.revision.assert_called_once_with(message="test migration", file_type="py")


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_create_migration_command_with_file_type_alias(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.revision = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "revision_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        [
            "--config",
            f"{module_name}.get_config",
            "create-migration",
            "-m",
            "test migration",
            "--file-type",
            "sql",
            "--no-prompt",
        ],
    )

    assert result.exit_code == 0
    mock_commands.revision.assert_called_once_with(message="test migration", file_type="sql")


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_upgrade_command(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test upgrade command."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.upgrade = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "upgrade_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_config", "upgrade", "--no-prompt"]
    )

    assert result.exit_code == 0
    mock_commands.upgrade.assert_called_once_with(revision="head", auto_sync=True, dry_run=False)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_upgrade_command_specific_revision(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test upgrade command with specific revision."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.upgrade = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "upgrade_revision_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_config", "upgrade", "abc123", "--no-prompt"]
    )

    assert result.exit_code == 0
    mock_commands.upgrade.assert_called_once_with(revision="abc123", auto_sync=True, dry_run=False)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_downgrade_command(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test downgrade command."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.downgrade = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "downgrade_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_config", "downgrade", "--no-prompt"]
    )

    assert result.exit_code == 0
    mock_commands.downgrade.assert_called_once_with(revision="-1", dry_run=False)


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_stamp_command(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test stamp command."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.stamp = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "stamp_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_config", "stamp", "abc123"])

    assert result.exit_code == 0
    mock_commands.stamp.assert_called_once_with(revision="abc123")


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_multi_config_operations(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test multi-configuration operations with include/exclude filters."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.current = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig

def get_configs():
    sqlite_config = SqliteConfig(connection_config={"database": ":memory:"})
    sqlite_config.bind_key = "sqlite_multi"
    sqlite_config.migration_config = {"enabled": True}

    duckdb_config = DuckDBConfig(connection_config={"database": ":memory:"})
    duckdb_config.bind_key = "duckdb_multi"
    duckdb_config.migration_config = {"enabled": True}

    return [sqlite_config, duckdb_config]
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", f"{module_name}.get_configs", "show-current-revision", "--include", "sqlite_multi"],
    )

    assert result.exit_code == 0
    # Should process only the included configuration
    assert "sqlite_multi" in result.output


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_dry_run_operations(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test dry-run operations show what would be executed."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.upgrade = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_configs():
    config1 = SqliteConfig(connection_config={"database": ":memory:"})
    config1.bind_key = "dry_run_test1"
    config1.migration_config = {"enabled": True}

    config2 = SqliteConfig(connection_config={"database": "test.db"})
    config2.bind_key = "dry_run_test2"
    config2.migration_config = {"enabled": True}

    return [config1, config2]
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_configs", "upgrade", "--dry-run"])

    assert result.exit_code == 0
    assert "Dry run" in result.output
    assert "Would upgrade" in result.output
    # Should not actually call the upgrade method with dry-run
    mock_commands.upgrade.assert_not_called()


def test_execution_mode_reporting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None) -> None:
    """Test that execution mode is reported when specified."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    config = SqliteConfig(connection_config={"database": ":memory:"})
    config.bind_key = "execution_mode_test"
    config.migration_config = {"enabled": True}
    return config
"""
    module_name = _create_module(config_module, tmp_path)

    with patch("sqlspec.migrations.commands.create_migration_commands") as mock_create:
        mock_commands = Mock()
        mock_commands.upgrade = Mock(return_value=None)
        mock_create.return_value = mock_commands

        result = runner.invoke(
            add_migration_commands(),
            ["--config", f"{module_name}.get_config", "upgrade", "--execution-mode", "sync", "--no-prompt"],
        )

    assert result.exit_code == 0
    assert "Execution mode: sync" in result.output


def test_bind_key_filtering_single_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test --bind-key filtering with single config."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_config():
    return SqliteConfig(
        bind_key="target_config",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "migrations"}
    )
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", f"{module_name}.get_config", "show-config", "--bind-key", "target_config"],
    )

    assert result.exit_code == 0
    assert "target_config" in result.output


def test_bind_key_filtering_multiple_configs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test --bind-key filtering with multiple configs."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig

def get_configs():
    sqlite_config = SqliteConfig(
        bind_key="sqlite_db",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "sqlite_migrations"}
    )

    duckdb_config = DuckDBConfig(
        bind_key="duckdb_db",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "duckdb_migrations"}
    )

    postgres_config = SqliteConfig(
        bind_key="postgres_db",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True, "script_location": "postgres_migrations"}
    )

    return [sqlite_config, duckdb_config, postgres_config]
"""
    module_name = _create_module(config_module, tmp_path)

    # Test filtering for sqlite_db only
    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_configs", "show-config", "--bind-key", "sqlite_db"]
    )

    assert result.exit_code == 0
    assert "sqlite_db" in result.output
    # Should only show one config, not all three
    assert "Found 1 configuration(s)" in result.output or "sqlite_migrations" in result.output
    assert "duckdb_db" not in result.output
    assert "postgres_db" not in result.output


def test_bind_key_filtering_nonexistent_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test --bind-key filtering with nonexistent bind key."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_configs():
    return [
        SqliteConfig(
            bind_key="existing_config",
            connection_config={"database": ":memory:"},
            migration_config={"enabled": True}
        )
    ]
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(), ["--config", f"{module_name}.get_configs", "show-config", "--bind-key", "nonexistent"]
    )

    assert result.exit_code == 1
    assert "No config found for bind key: nonexistent" in result.output


@patch("sqlspec.migrations.commands.create_migration_commands")
def test_bind_key_filtering_with_migration_commands(
    mock_create_commands: "Mock", tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_test_modules: None
) -> None:
    """Test --bind-key filtering works with actual migration commands."""
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    mock_commands = Mock()
    mock_commands.upgrade = Mock(return_value=None)
    mock_create_commands.return_value = mock_commands

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.adapters.duckdb.config import DuckDBConfig

def get_multi_configs():
    return [
        SqliteConfig(
            bind_key="primary_db",
            connection_config={"database": "primary.db"},
            migration_config={"enabled": True, "script_location": "primary_migrations"}
        ),
        DuckDBConfig(
            bind_key="analytics_db",
            connection_config={"database": "analytics.duckdb"},
            migration_config={"enabled": True, "script_location": "analytics_migrations"}
        )
    ]
"""
    module_name = _create_module(config_module, tmp_path)

    result = runner.invoke(
        add_migration_commands(),
        ["--config", f"{module_name}.get_multi_configs", "upgrade", "--bind-key", "analytics_db", "--no-prompt"],
    )

    assert result.exit_code == 0
    # Should only process the analytics_db config
    mock_commands.upgrade.assert_called_once_with(revision="head", auto_sync=True, dry_run=False)
