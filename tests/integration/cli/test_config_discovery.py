"""Integration tests for CLI config discovery.

Tests the full CLI behavior with environment variables and pyproject.toml config discovery.
"""

import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands, get_sqlspec_group


@pytest.fixture
def mock_config_module(tmp_path: "Path") -> "Generator[Path, None, None]":
    """Create a mock config module that can be imported."""
    config_dir = tmp_path / "test_config"
    config_dir.mkdir()

    # Create __init__.py
    init_file = config_dir / "__init__.py"
    init_file.write_text("")

    # Create config.py with a mock config
    config_file = config_dir / "config.py"
    config_file.write_text("""
from unittest.mock import Mock

def get_test_config():
    config = Mock()
    config.bind_key = "test"
    config.migration_config = {"enabled": True, "script_location": "migrations"}
    config.connection_config = {}  # Required for protocol validation
    config.is_async = False
    return config
""")

    # Add to sys.path so it can be imported
    sys.path.insert(0, str(tmp_path))

    yield config_dir

    # Cleanup
    if str(tmp_path) in sys.path:
        sys.path.remove(str(tmp_path))


def get_cli_with_commands() -> "Any":
    """Get CLI group with migration commands added."""
    cli = get_sqlspec_group()
    return add_migration_commands(cli)


def test_cli_with_env_var(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path") -> None:
    """Test full CLI execution with SQLSPEC_CONFIG env var."""
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(
        cli, ["--validate-config", "show-config"], env={"SQLSPEC_CONFIG": "test_config.config.get_test_config"}
    )

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Successfully loaded 1 config(s)" in result.output


def test_cli_with_pyproject(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path") -> None:
    """Test full CLI execution with pyproject.toml config."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "test_config.config.get_test_config"
""")

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["--validate-config", "show-config"])

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Using config from pyproject.toml" in result.output
    assert "Successfully loaded 1 config(s)" in result.output


def test_cli_flag_overrides_env(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path"
) -> None:
    """Test --config flag overrides SQLSPEC_CONFIG."""
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(
        cli,
        ["--config", "test_config.config.get_test_config", "--validate-config", "show-config"],
        env={"SQLSPEC_CONFIG": "wrong.config.get_wrong"},
    )

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Successfully loaded 1 config(s)" in result.output
    assert "Using config from pyproject.toml" not in result.output


def test_cli_env_overrides_pyproject(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path"
) -> None:
    """Test SQLSPEC_CONFIG overrides pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "wrong.config.get_wrong"
""")

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(
        cli, ["--validate-config", "show-config"], env={"SQLSPEC_CONFIG": "test_config.config.get_test_config"}
    )

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Successfully loaded 1 config(s)" in result.output
    assert "Using config from pyproject.toml" not in result.output


def test_cli_flag_overrides_pyproject(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path"
) -> None:
    """Test --config flag overrides pyproject.toml."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "wrong.config.get_wrong"
""")

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["--config", "test_config.config.get_test_config", "--validate-config", "show-config"])

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Successfully loaded 1 config(s)" in result.output
    assert "Using config from pyproject.toml" not in result.output


def test_cli_no_config_error(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test no config source gives helpful error."""
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["show-config"])

    assert result.exit_code == 1
    assert "Error: No SQLSpec config found" in result.output
    assert "CLI flag:" in result.output
    assert "Environment var:" in result.output
    assert "pyproject.toml:" in result.output


def test_cli_multiple_configs_from_pyproject(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test multiple configs from pyproject.toml using a list-returning callable.

    Note: pyproject.toml lists are joined with commas, but resolve_config_sync doesn't split them.
    The proper way to return multiple configs is via a callable that returns a list.
    """
    # Create fresh config module with multiple configs function
    config_dir = tmp_path / "multiconfig"
    config_dir.mkdir()
    (config_dir / "__init__.py").write_text("")
    (config_dir / "config.py").write_text("""
from unittest.mock import Mock

def get_multiple_configs():
    config1 = Mock()
    config1.bind_key = "primary"
    config1.migration_config = {"enabled": True, "script_location": "migrations"}
    config1.connection_config = {}
    config1.is_async = False

    config2 = Mock()
    config2.bind_key = "secondary"
    config2.migration_config = {"enabled": True, "script_location": "migrations"}
    config2.connection_config = {}
    config2.is_async = False

    return [config1, config2]
""")

    # Add to sys.path
    sys.path.insert(0, str(tmp_path))

    try:
        pyproject = tmp_path / "pyproject.toml"
        pyproject.write_text("""
[tool.sqlspec]
config = "multiconfig.config.get_multiple_configs"
""")

        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        cli = get_cli_with_commands()

        result = runner.invoke(cli, ["--validate-config", "show-config"])

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Using config from pyproject.toml" in result.output
        assert "Successfully loaded 2 config(s)" in result.output
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))


def test_cli_invalid_config_path(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test invalid config path gives clear error."""
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["show-config"], env={"SQLSPEC_CONFIG": "nonexistent.module.get_config"})

    assert result.exit_code == 1
    assert "Error loading config" in result.output


def test_cli_malformed_pyproject(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test malformed pyproject.toml raises ValueError."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("invalid toml {{")

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["show-config"])

    # Malformed TOML raises ValueError which gets caught by CliRunner
    assert result.exit_code != 0
    assert result.exception is not None
    assert isinstance(result.exception, ValueError)


def test_cli_pyproject_in_parent_directory(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path"
) -> None:
    """Test discovers pyproject.toml in parent directory."""
    root = tmp_path / "project"
    root.mkdir()
    subdir = root / "src"
    subdir.mkdir()

    pyproject = root / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = "test_config.config.get_test_config"
""")

    monkeypatch.chdir(subdir)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["--validate-config", "show-config"])

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "Using config from pyproject.toml" in result.output
    assert "Successfully loaded 1 config(s)" in result.output


def test_cli_pyproject_stops_at_git(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test pyproject.toml discovery stops at .git boundary."""
    git_root = tmp_path / "repo"
    git_root.mkdir()
    (git_root / ".git").mkdir()

    parent_pyproject = tmp_path / "pyproject.toml"
    parent_pyproject.write_text("""
[tool.sqlspec]
config = "should_not_find.this"
""")

    subdir = git_root / "src"
    subdir.mkdir()

    monkeypatch.chdir(subdir)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["show-config"])

    assert result.exit_code == 1
    assert "Error: No SQLSpec config found" in result.output


def test_cli_with_multiple_configs_callable(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test callable returning multiple configs."""
    # Create fresh config module
    config_dir = tmp_path / "multiconf2"
    config_dir.mkdir()
    (config_dir / "__init__.py").write_text("")
    (config_dir / "config.py").write_text("""
from unittest.mock import Mock

def get_multiple_configs():
    config1 = Mock()
    config1.bind_key = "primary"
    config1.migration_config = {"enabled": True, "script_location": "migrations"}
    config1.connection_config = {}
    config1.is_async = False

    config2 = Mock()
    config2.bind_key = "secondary"
    config2.migration_config = {"enabled": True, "script_location": "migrations"}
    config2.connection_config = {}
    config2.is_async = False

    return [config1, config2]
""")

    sys.path.insert(0, str(tmp_path))

    try:
        monkeypatch.chdir(tmp_path)

        runner = CliRunner()
        cli = get_cli_with_commands()

        result = runner.invoke(
            cli, ["--validate-config", "show-config"], env={"SQLSPEC_CONFIG": "multiconf2.config.get_multiple_configs"}
        )

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "Successfully loaded 2 config(s)" in result.output
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))


def test_cli_config_with_bind_key(
    tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch", mock_config_module: "Path"
) -> None:
    """Test config with bind_key is displayed correctly."""
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(
        cli, ["--validate-config", "show-config"], env={"SQLSPEC_CONFIG": "test_config.config.get_test_config"}
    )

    assert result.exit_code == 0, f"Output: {result.output}"
    assert "test:" in result.output


def test_cli_pyproject_without_config_key(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test pyproject.toml without config key falls back to error."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
other_setting = "value"
""")

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["show-config"])

    assert result.exit_code == 1
    assert "Error: No SQLSpec config found" in result.output


def test_cli_pyproject_with_empty_config(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test pyproject.toml with empty config list."""
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = []
""")

    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    cli = get_cli_with_commands()

    result = runner.invoke(cli, ["show-config"])

    assert result.exit_code == 1


def test_cli_multiple_config_paths_in_pyproject(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test pyproject.toml with multiple config paths (list format)."""
    # Use unique module name to avoid module caching issues between tests
    config_dir = tmp_path / "multipath_app"
    config_dir.mkdir()

    init_file = config_dir / "__init__.py"
    init_file.write_text("")

    config_file = config_dir / "db.py"
    config_file.write_text("""
from unittest.mock import Mock

def get_primary():
    config = Mock()
    config.bind_key = "primary"
    config.migration_config = {"enabled": True, "script_location": "migrations"}
    config.connection_config = {}
    config.is_async = False
    return config

def get_secondary():
    config = Mock()
    config.bind_key = "secondary"
    config.migration_config = {"enabled": True, "script_location": "migrations"}
    config.connection_config = {}
    config.is_async = False
    return config
""")

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = ["multipath_app.db.get_primary", "multipath_app.db.get_secondary"]
""")

    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    try:
        runner = CliRunner()
        cli = get_cli_with_commands()

        result = runner.invoke(cli, ["--validate-config", "show-config"])

        # Debug: print output if test fails
        if result.exit_code != 0:
            if result.exception:
                import traceback

                traceback.print_exception(type(result.exception), result.exception, result.exception.__traceback__)

        assert result.exit_code == 0
        assert "primary" in result.output
        assert "secondary" in result.output
        assert "2 config(s)" in result.output
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))


def test_cli_config_deduplication_by_bind_key(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test that configs with same bind_key are deduplicated (later wins)."""
    # Use unique module name to avoid module caching issues between tests
    config_dir = tmp_path / "dedup_app"
    config_dir.mkdir()

    init_file = config_dir / "__init__.py"
    init_file.write_text("")

    config_file = config_dir / "db.py"
    config_file.write_text("""
from unittest.mock import Mock

def get_config_v1():
    config = Mock()
    config.bind_key = "main"
    config.migration_config = {"enabled": True, "script_location": "migrations_v1"}
    config.connection_config = {}
    config.is_async = False
    config.version = "v1"
    return config

def get_config_v2():
    config = Mock()
    config.bind_key = "main"  # Same bind_key!
    config.migration_config = {"enabled": True, "script_location": "migrations_v2"}
    config.connection_config = {}
    config.is_async = False
    config.version = "v2"
    return config
""")

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("""
[tool.sqlspec]
config = ["dedup_app.db.get_config_v1", "dedup_app.db.get_config_v2"]
""")

    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    try:
        runner = CliRunner()
        cli = get_cli_with_commands()

        result = runner.invoke(cli, ["--validate-config", "show-config"])

        assert result.exit_code == 0
        # Should only have 1 config (deduplicated by bind_key)
        assert "1 config(s)" in result.output
        assert "main" in result.output
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))


def test_cli_env_var_comma_separated_paths(tmp_path: "Path", monkeypatch: "pytest.MonkeyPatch") -> None:
    """Test SQLSPEC_CONFIG with comma-separated config paths."""
    # Use unique module name to avoid module caching issues between tests
    config_dir = tmp_path / "envvar_app"
    config_dir.mkdir()

    init_file = config_dir / "__init__.py"
    init_file.write_text("")

    config_file = config_dir / "db.py"
    config_file.write_text("""
from unittest.mock import Mock

def get_db1():
    config = Mock()
    config.bind_key = "db1"
    config.migration_config = {"enabled": True, "script_location": "migrations"}
    config.connection_config = {}
    config.is_async = False
    return config

def get_db2():
    config = Mock()
    config.bind_key = "db2"
    config.migration_config = {"enabled": True, "script_location": "migrations"}
    config.connection_config = {}
    config.is_async = True
    return config
""")

    monkeypatch.chdir(tmp_path)
    sys.path.insert(0, str(tmp_path))

    try:
        runner = CliRunner()
        cli = get_cli_with_commands()

        # Use comma-separated paths in env var
        result = runner.invoke(
            cli,
            ["--validate-config", "show-config"],
            env={"SQLSPEC_CONFIG": "envvar_app.db.get_db1, envvar_app.db.get_db2"},  # Note: whitespace after comma
        )

        assert result.exit_code == 0
        assert "db1" in result.output
        assert "db2" in result.output
        assert "2 config(s)" in result.output
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
