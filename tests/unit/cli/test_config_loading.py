"""Tests for CLI configuration loading functionality."""

import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands

MODULE_PREFIX = "cli_test_config_"


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


def _create_module(path: "Path", content: str) -> str:
    module_name = f"{MODULE_PREFIX}{uuid.uuid4().hex}"
    (path / f"{module_name}.py").write_text(content)
    return module_name


def test_direct_config_instance_loading(
    tmp_path: Path, cleanup_test_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test loading a direct config instance through CLI."""
    runner = CliRunner()

    # Create a test module with a direct config instance
    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

config = SqliteConfig(
    bind_key="test",
    connection_config={"database": ":memory:"},
    migration_config={"enabled": True, "script_location": "migrations"}
)
database_config = config
"""
    module_name = _create_module(tmp_path, config_module)

    # Change to the temp directory
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.database_config", "show-config"])

    assert result.exit_code == 0
    assert "test" in result.output
    assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_sync_callable_config_loading(
    tmp_path: Path, cleanup_test_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test loading config from synchronous callable through CLI."""
    runner = CliRunner()

    # Create a test module with sync callable
    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_database_config():
    config = SqliteConfig(
        bind_key="sync_test",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True}
    )
    return config
"""
    module_name = _create_module(tmp_path, config_module)

    # Change to the temp directory
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_database_config", "show-config"])

    assert result.exit_code == 0
    assert "sync_test" in result.output
    assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_async_callable_config_loading(
    tmp_path: Path, cleanup_test_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test loading config from asynchronous callable through CLI."""
    runner = CliRunner()

    # Create a test module with async callable
    config_module = """
import asyncio
from sqlspec.adapters.sqlite.config import SqliteConfig

async def get_database_config():
    # Simulate some async work
    await asyncio.sleep(0.001)
    config = SqliteConfig(
        bind_key="async_test",
        connection_config={"database": ":memory:"},
        migration_config={"enabled": True}
    )
    return config
"""
    module_name = _create_module(tmp_path, config_module)

    # Change to the temp directory
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.get_database_config", "show-config"])

    if result.exception:
        pass
    assert result.exit_code == 0
    assert "async_test" in result.output
    assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_show_config_with_path_object(
    tmp_path: Path, cleanup_test_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test show-config handles Path objects in script_location without crashing."""
    runner = CliRunner()

    # Create a test module with Path object in script_location
    config_module = """
from pathlib import Path
from sqlspec.adapters.sqlite.config import SqliteConfig

config = SqliteConfig(
    bind_key="path_test",
    connection_config={"database": ":memory:"},
    migration_config={"enabled": True, "script_location": Path("custom_migrations")}
)
database_config = config
"""
    module_name = _create_module(tmp_path, config_module)

    # Change to the temp directory
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}.database_config", "show-config"])

    assert result.exit_code == 0
    assert "path_test" in result.output
    assert "custom_migrations" in result.output
    assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_module_reference_reports_its_config_attributes(
    tmp_path: Path, cleanup_test_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pointing --config at a module names the module:attribute reference to use."""
    runner = CliRunner()

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

database_config = SqliteConfig(
    bind_key="app",
    connection_config={"database": ":memory:"},
    migration_config={"script_location": "migrations"},
)
"""
    module_name = _create_module(tmp_path, config_module)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", module_name, "show-config"])

    assert result.exit_code == 1
    normalized = " ".join(result.output.split())
    assert "names a module, not a database configuration" in normalized
    assert f"{module_name}:database_config" in normalized


def test_unknown_migration_config_key_is_reported_without_a_traceback(
    tmp_path: Path, cleanup_test_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A misspelled migration_config key surfaces as a CLI error, not a stack trace."""
    runner = CliRunner()

    config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

database_config = SqliteConfig(
    bind_key="app",
    connection_config={"database": ":memory:"},
    migration_config={"script_location": "migrations", "version_table": "_schema_versions"},
)
"""
    module_name = _create_module(tmp_path, config_module)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(add_migration_commands(), ["--config", f"{module_name}:database_config", "show-config"])

    assert result.exit_code == 1
    assert "Did you mean 'version_table_name'?" in " ".join(result.output.split())
    assert "Traceback" not in result.output


def test_missing_config_help_shows_the_pyproject_section_name() -> None:
    """The pyproject hint renders literally instead of being consumed as console markup."""
    runner = CliRunner()

    result = runner.invoke(add_migration_commands(), ["show-config"], env={"SQLSPEC_CONFIG": ""})

    assert result.exit_code == 1
    assert "[tool.sqlspec]" in result.output
