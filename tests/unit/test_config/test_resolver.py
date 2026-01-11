"""Tests for configuration resolver functionality."""

from typing import Any
from unittest.mock import Mock, NonCallableMock, patch

import pytest

from sqlspec.exceptions import ConfigResolverError
from sqlspec.utils.config_tools import (
    _is_valid_config,  # pyright: ignore[reportPrivateUsage]
    resolve_config_async,
    resolve_config_sync,
)


def _create_mock_config(
    database_url: str = "sqlite:///test.db", bind_key: str = "test", migration_config: dict[str, Any] | None = None
) -> NonCallableMock:
    """Create a non-callable mock config with required attributes.

    Using NonCallableMock is critical because the config resolver checks
    `callable(config_obj)` to determine if it should invoke the config.
    Regular Mock objects are callable by default, which causes them to be
    called and return a NEW Mock without our configured attributes.
    """
    mock_config: NonCallableMock = NonCallableMock()
    mock_config.database_url = database_url
    mock_config.bind_key = bind_key
    mock_config.migration_config = migration_config if migration_config is not None else {}
    return mock_config


async def test_resolve_direct_config_instance() -> None:
    """Test resolving a direct config instance."""
    mock_config = _create_mock_config()

    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_config):
        result = await resolve_config_async("myapp.config.database_config")
        assert hasattr(result, "database_url")
        assert hasattr(result, "bind_key")
        assert hasattr(result, "migration_config")


async def test_resolve_config_list() -> None:
    """Test resolving a list of config instances."""
    mock_config1 = _create_mock_config(database_url="sqlite:///test1.db", bind_key="test1")
    mock_config2 = _create_mock_config(database_url="sqlite:///test2.db", bind_key="test2")
    config_list = [mock_config1, mock_config2]

    with patch("sqlspec.utils.config_tools.import_string", return_value=config_list):
        result = await resolve_config_async("myapp.config.database_configs")
        assert result == config_list
        assert isinstance(result, list) and len(result) == 2


async def test_resolve_sync_callable_config() -> None:
    """Test resolving a synchronous callable that returns config."""
    mock_config = _create_mock_config()

    def get_config() -> NonCallableMock:
        return mock_config

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_config):
        result = await resolve_config_async("myapp.config.get_database_config")
        assert result is mock_config


async def test_resolve_async_callable_config() -> None:
    """Test resolving an asynchronous callable that returns config."""
    mock_config = _create_mock_config()

    async def get_config() -> NonCallableMock:
        return mock_config

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_config):
        result = await resolve_config_async("myapp.config.async_get_database_config")
        assert result is mock_config


async def test_resolve_sync_callable_config_list() -> None:
    """Test resolving a sync callable that returns config list."""
    mock_config = _create_mock_config()

    def get_configs() -> list[NonCallableMock]:
        return [mock_config]

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_configs):
        result = await resolve_config_async("myapp.config.get_database_configs")
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] is mock_config


async def test_import_error_handling() -> None:
    """Test proper handling of import errors."""
    with patch("sqlspec.utils.config_tools.import_string", side_effect=ImportError("Module not found")):
        with pytest.raises(ConfigResolverError, match="Failed to import config from path"):
            await resolve_config_async("nonexistent.config")


async def test_callable_execution_error() -> None:
    """Test handling of errors during callable execution."""

    def failing_config() -> None:
        raise ValueError("Config generation failed")

    with patch("sqlspec.utils.config_tools.import_string", return_value=failing_config):
        with pytest.raises(ConfigResolverError, match="Failed to execute callable config"):
            await resolve_config_async("myapp.config.failing_config")


async def test_none_result_validation() -> None:
    """Test validation when config resolves to None."""

    def none_config() -> None:
        return None

    with patch("sqlspec.utils.config_tools.import_string", return_value=none_config):
        with pytest.raises(ConfigResolverError, match="resolved to None"):
            await resolve_config_async("myapp.config.none_config")


async def test_empty_list_validation() -> None:
    """Test validation when config resolves to empty list."""

    def empty_list_config() -> list[Any]:
        return []

    with patch("sqlspec.utils.config_tools.import_string", return_value=empty_list_config):
        with pytest.raises(ConfigResolverError, match="resolved to empty list"):
            await resolve_config_async("myapp.config.empty_list_config")


async def test_invalid_config_type_validation() -> None:
    """Test validation when config is invalid type."""

    def invalid_config() -> str:
        return "not a config"

    with patch("sqlspec.utils.config_tools.import_string", return_value=invalid_config):
        with pytest.raises(ConfigResolverError, match="returned invalid type"):
            await resolve_config_async("myapp.config.invalid_config")


async def test_invalid_config_in_list_validation() -> None:
    """Test validation when list contains invalid config."""
    mock_valid_config = _create_mock_config()

    def mixed_config_list() -> list[Any]:
        return [mock_valid_config, "invalid_config"]

    with patch("sqlspec.utils.config_tools.import_string", return_value=mixed_config_list):
        with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
            await resolve_config_async("myapp.config.mixed_configs")


async def test_config_validation_attributes() -> None:
    """Test that config validation checks for required attributes."""

    class IncompleteConfig:
        def __init__(self) -> None:
            self.bind_key = "test"
            self.migration_config: dict[str, Any] = {}

    def incomplete_config() -> IncompleteConfig:
        return IncompleteConfig()

    with patch("sqlspec.utils.config_tools.import_string", return_value=incomplete_config):
        with pytest.raises(ConfigResolverError, match="returned invalid type"):
            await resolve_config_async("myapp.config.incomplete_config")


async def test_config_class_rejected() -> None:
    """Test that config classes (not instances) are rejected.

    Note: This test directly validates that _is_valid_config rejects classes.
    When using resolve_config_*, classes are callable and get instantiated,
    so they don't reach direct validation as classes.
    """

    class MockConfigClass:
        """Mock config class to simulate config classes being passed."""

        database_url = "sqlite:///test.db"
        bind_key = "test"
        migration_config: dict[str, Any] = {}

    assert isinstance(MockConfigClass, type), "Should be a class"
    assert not _is_valid_config(MockConfigClass), "Classes should be rejected"

    instance = MockConfigClass()
    assert not isinstance(instance, type), "Should be an instance"
    assert _is_valid_config(instance), "Instances should be accepted"


async def test_config_class_in_list_rejected() -> None:
    """Test that config classes in a list are rejected."""
    mock_instance = Mock()
    mock_instance.database_url = "sqlite:///test.db"
    mock_instance.bind_key = "test"
    mock_instance.migration_config = {}

    class MockConfigClass:
        """Mock config class."""

        database_url = "sqlite:///test.db"
        bind_key = "test"
        migration_config: dict[str, Any] = {}

    def mixed_list() -> list[Any]:
        return [mock_instance, MockConfigClass]

    with patch("sqlspec.utils.config_tools.import_string", return_value=mixed_list):
        with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
            await resolve_config_async("myapp.config.mixed_list")


async def test_config_instance_accepted() -> None:
    """Test that config instances (not classes) are accepted."""

    class MockConfigClass:
        """Mock config class."""

        def __init__(self) -> None:
            self.database_url = "sqlite:///test.db"
            self.bind_key = "test"
            self.migration_config: dict[str, Any] = {}

    mock_instance = MockConfigClass()

    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_instance):
        result = await resolve_config_async("myapp.config.config_instance")
        assert hasattr(result, "database_url")
        assert hasattr(result, "bind_key")
        assert hasattr(result, "migration_config")


def test_resolve_config_sync_wrapper() -> None:
    """Test that the sync wrapper works correctly."""
    mock_config = _create_mock_config()

    with patch("sqlspec.utils.config_tools.import_string", return_value=mock_config):
        result = resolve_config_sync("myapp.config.database_config")
        assert hasattr(result, "database_url")
        assert hasattr(result, "bind_key")
        assert hasattr(result, "migration_config")


def test_resolve_config_sync_callable() -> None:
    """Test sync wrapper with callable config."""
    mock_config = _create_mock_config()

    def get_config() -> NonCallableMock:
        return mock_config

    with patch("sqlspec.utils.config_tools.import_string", return_value=get_config):
        result = resolve_config_sync("myapp.config.get_database_config")
        assert result is mock_config
