"""Tests for configuration resolver functionality."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.utils.config_resolver import (
    ConfigResolverError,
    _is_valid_config,  # pyright: ignore[reportPrivateUsage]
    resolve_config_async,
    resolve_config_sync,
)


class TestConfigResolver:
    """Test the config resolver utility."""

    async def test_resolve_direct_config_instance(self) -> None:
        """Test resolving a direct config instance."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mock_config):
            result = await resolve_config_async("myapp.config.database_config")
            # Check attributes instead of object identity since validation creates a copy
            assert hasattr(result, "database_url")
            assert hasattr(result, "bind_key")
            assert hasattr(result, "migration_config")

    async def test_resolve_config_list(self) -> None:
        """Test resolving a list of config instances."""
        mock_config1 = Mock()
        mock_config1.database_url = "sqlite:///test1.db"
        mock_config1.bind_key = "test1"
        mock_config1.migration_config = {}

        mock_config2 = Mock()
        mock_config2.database_url = "sqlite:///test2.db"
        mock_config2.bind_key = "test2"
        mock_config2.migration_config = {}

        config_list = [mock_config1, mock_config2]

        with patch("sqlspec.utils.config_resolver.import_string", return_value=config_list):
            result = await resolve_config_async("myapp.config.database_configs")
            assert result == config_list
            assert isinstance(result, list) and len(result) == 2

    async def test_resolve_sync_callable_config(self) -> None:
        """Test resolving a synchronous callable that returns config."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        def get_config() -> Mock:
            return mock_config

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_config):
            result = await resolve_config_async("myapp.config.get_database_config")
            assert result is mock_config

    async def test_resolve_async_callable_config(self) -> None:
        """Test resolving an asynchronous callable that returns config."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        async def get_config() -> Mock:
            return mock_config

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_config):
            result = await resolve_config_async("myapp.config.async_get_database_config")
            assert result is mock_config

    async def test_resolve_sync_callable_config_list(self) -> None:
        """Test resolving a sync callable that returns config list."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        def get_configs() -> list[Mock]:
            return [mock_config]

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_configs):
            result = await resolve_config_async("myapp.config.get_database_configs")
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0] is mock_config

    async def test_import_error_handling(self) -> None:
        """Test proper handling of import errors."""
        with patch("sqlspec.utils.config_resolver.import_string", side_effect=ImportError("Module not found")):
            with pytest.raises(ConfigResolverError, match="Failed to import config from path"):
                await resolve_config_async("nonexistent.config")

    async def test_callable_execution_error(self) -> None:
        """Test handling of errors during callable execution."""

        def failing_config() -> None:
            raise ValueError("Config generation failed")

        with patch("sqlspec.utils.config_resolver.import_string", return_value=failing_config):
            with pytest.raises(ConfigResolverError, match="Failed to execute callable config"):
                await resolve_config_async("myapp.config.failing_config")

    async def test_none_result_validation(self) -> None:
        """Test validation when config resolves to None."""

        def none_config() -> None:
            return None

        with patch("sqlspec.utils.config_resolver.import_string", return_value=none_config):
            with pytest.raises(ConfigResolverError, match="resolved to None"):
                await resolve_config_async("myapp.config.none_config")

    async def test_empty_list_validation(self) -> None:
        """Test validation when config resolves to empty list."""

        def empty_list_config() -> list[Any]:
            return []

        with patch("sqlspec.utils.config_resolver.import_string", return_value=empty_list_config):
            with pytest.raises(ConfigResolverError, match="resolved to empty list"):
                await resolve_config_async("myapp.config.empty_list_config")

    async def test_invalid_config_type_validation(self) -> None:
        """Test validation when config is invalid type."""

        def invalid_config() -> str:
            return "not a config"

        with patch("sqlspec.utils.config_resolver.import_string", return_value=invalid_config):
            with pytest.raises(ConfigResolverError, match="returned invalid type"):
                await resolve_config_async("myapp.config.invalid_config")

    async def test_invalid_config_in_list_validation(self) -> None:
        """Test validation when list contains invalid config."""
        mock_valid_config = Mock()
        mock_valid_config.database_url = "sqlite:///test.db"
        mock_valid_config.bind_key = "test"
        mock_valid_config.migration_config = {}

        def mixed_config_list() -> list[Any]:
            return [mock_valid_config, "invalid_config"]

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mixed_config_list):
            with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
                await resolve_config_async("myapp.config.mixed_configs")

    async def test_config_validation_attributes(self) -> None:
        """Test that config validation checks for required attributes."""

        # Test config missing both database_url and connection_config
        class IncompleteConfig:
            def __init__(self) -> None:
                self.bind_key = "test"
                self.migration_config: dict[str, Any] = {}
                # Missing both connection_config and database_url

        def incomplete_config() -> "IncompleteConfig":
            return IncompleteConfig()

        with patch("sqlspec.utils.config_resolver.import_string", return_value=incomplete_config):
            with pytest.raises(ConfigResolverError, match="returned invalid type"):
                await resolve_config_async("myapp.config.incomplete_config")

    async def test_config_class_rejected(self) -> None:
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

        # Directly test that _is_valid_config rejects classes
        assert isinstance(MockConfigClass, type), "Should be a class"
        assert not _is_valid_config(MockConfigClass), "Classes should be rejected"

        # But instances should be accepted
        instance = MockConfigClass()
        assert not isinstance(instance, type), "Should be an instance"
        assert _is_valid_config(instance), "Instances should be accepted"

    async def test_config_class_in_list_rejected(self) -> None:
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
            return [mock_instance, MockConfigClass]  # Class, not instance

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mixed_list):
            with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
                await resolve_config_async("myapp.config.mixed_list")

    async def test_config_instance_accepted(self) -> None:
        """Test that config instances (not classes) are accepted."""

        class MockConfigClass:
            """Mock config class."""

            def __init__(self) -> None:
                self.database_url = "sqlite:///test.db"
                self.bind_key = "test"
                self.migration_config: dict[str, Any] = {}

        # Pass an instance, not the class
        mock_instance = MockConfigClass()

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mock_instance):
            result = await resolve_config_async("myapp.config.config_instance")
            assert hasattr(result, "database_url")
            assert hasattr(result, "bind_key")
            assert hasattr(result, "migration_config")


class TestConfigResolverSync:
    """Test the synchronous wrapper for config resolver."""

    def test_resolve_config_sync_wrapper(self) -> None:
        """Test that the sync wrapper works correctly."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mock_config):
            result = resolve_config_sync("myapp.config.database_config")
            assert hasattr(result, "database_url")
            assert hasattr(result, "bind_key")
            assert hasattr(result, "migration_config")

    def test_resolve_config_sync_callable(self) -> None:
        """Test sync wrapper with callable config."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        def get_config() -> Mock:
            return mock_config

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_config):
            result = resolve_config_sync("myapp.config.get_database_config")
            assert result is mock_config
