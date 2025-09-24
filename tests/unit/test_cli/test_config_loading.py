"""Tests for CLI configuration loading functionality."""

import os
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from sqlspec.cli import add_migration_commands


@pytest.fixture
def cleanup_test_modules() -> Iterator[None]:
    """Fixture to clean up test modules from sys.modules after each test."""
    modules_before = set(sys.modules.keys())
    yield
    # Remove any test modules that were imported during the test
    modules_after = set(sys.modules.keys())
    test_modules = {m for m in modules_after - modules_before if m.startswith("test_config")}
    for module in test_modules:
        if module in sys.modules:
            del sys.modules[module]


def test_direct_config_instance_loading(cleanup_test_modules: None) -> None:
    """Test loading a direct config instance through CLI."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a test module with a direct config instance
        config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

config = SqliteConfig(
    bind_key="test",
    pool_config={"database": ":memory:"},
    migration_config={"enabled": True, "script_location": "migrations"}
)
database_config = config
"""
        (Path(temp_dir) / "test_config.py").write_text(config_module)

        # Change to the temp directory
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            result = runner.invoke(add_migration_commands(), ["--config", "test_config.database_config", "show-config"])
        finally:
            os.chdir(original_cwd)

        assert result.exit_code == 0
        assert "test" in result.output
        assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_sync_callable_config_loading(cleanup_test_modules: None) -> None:
    """Test loading config from synchronous callable through CLI."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a test module with sync callable
        config_module = """
from sqlspec.adapters.sqlite.config import SqliteConfig

def get_database_config():
    config = SqliteConfig(
        bind_key="sync_test",
        pool_config={"database": ":memory:"},
        migration_config={"enabled": True}
    )
    return config
"""
        (Path(temp_dir) / "test_config.py").write_text(config_module)

        # Change to the temp directory
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            result = runner.invoke(
                add_migration_commands(), ["--config", "test_config.get_database_config", "show-config"]
            )
        finally:
            os.chdir(original_cwd)

        assert result.exit_code == 0
        assert "sync_test" in result.output
        assert "Migration Enabled" in result.output or "migrations enabled" in result.output


def test_async_callable_config_loading(cleanup_test_modules: None) -> None:
    """Test loading config from asynchronous callable through CLI."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a test module with async callable
        config_module = """
import asyncio
from sqlspec.adapters.sqlite.config import SqliteConfig

async def get_database_config():
    # Simulate some async work
    await asyncio.sleep(0.001)
    config = SqliteConfig(
        bind_key="async_test",
        pool_config={"database": ":memory:"},
        migration_config={"enabled": True}
    )
    return config
"""
        (Path(temp_dir) / "test_config.py").write_text(config_module)

        # Change to the temp directory
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            result = runner.invoke(
                add_migration_commands(), ["--config", "test_config.get_database_config", "show-config"]
            )
        finally:
            os.chdir(original_cwd)

        if result.exception:
            pass
        assert result.exit_code == 0
        assert "async_test" in result.output
        assert "Migration Enabled" in result.output or "migrations enabled" in result.output
