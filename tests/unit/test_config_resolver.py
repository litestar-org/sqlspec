"""Tests for configuration resolver functionality."""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from sqlspec.utils.config_resolver import ConfigResolverError, resolve_config


class TestConfigResolver:
    """Test the config resolver utility."""

    def test_resolve_direct_config_instance(self) -> None:
        """Test resolving a direct config instance."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mock_config):
            result = resolve_config("myapp.config.database_config")
            # Check attributes instead of object identity since validation creates a copy
            assert hasattr(result, "database_url")
            assert hasattr(result, "bind_key")
            assert hasattr(result, "migration_config")

    def test_resolve_config_list(self) -> None:
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
            result = resolve_config("myapp.config.database_configs")
            assert result == config_list
            assert isinstance(result, list) and len(result) == 2

    def test_resolve_sync_callable_config(self) -> None:
        """Test resolving a synchronous callable that returns config."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        def get_config() -> Mock:
            return mock_config

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_config):
            result = resolve_config("myapp.config.get_database_config")
            assert result is mock_config

    def test_resolve_async_callable_config(self) -> None:
        """Test resolving an asynchronous callable that returns config."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        async def get_config() -> Mock:
            return mock_config

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_config):
            result = resolve_config("myapp.config.async_get_database_config")
            assert result is mock_config

    def test_resolve_sync_callable_config_list(self) -> None:
        """Test resolving a sync callable that returns config list."""
        mock_config = Mock()
        mock_config.database_url = "sqlite:///test.db"
        mock_config.bind_key = "test"
        mock_config.migration_config = {}

        def get_configs() -> list[Mock]:
            return [mock_config]

        with patch("sqlspec.utils.config_resolver.import_string", return_value=get_configs):
            result = resolve_config("myapp.config.get_database_configs")
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0] is mock_config

    def test_import_error_handling(self) -> None:
        """Test proper handling of import errors."""
        with patch("sqlspec.utils.config_resolver.import_string", side_effect=ImportError("Module not found")):
            with pytest.raises(ConfigResolverError, match="Failed to import config from path"):
                resolve_config("nonexistent.config")

    def test_callable_execution_error(self) -> None:
        """Test handling of errors during callable execution."""

        def failing_config() -> None:
            raise ValueError("Config generation failed")

        with patch("sqlspec.utils.config_resolver.import_string", return_value=failing_config):
            with pytest.raises(ConfigResolverError, match="Failed to execute callable config"):
                resolve_config("myapp.config.failing_config")

    def test_none_result_validation(self) -> None:
        """Test validation when config resolves to None."""

        def none_config() -> None:
            return None

        with patch("sqlspec.utils.config_resolver.import_string", return_value=none_config):
            with pytest.raises(ConfigResolverError, match="resolved to None"):
                resolve_config("myapp.config.none_config")

    def test_empty_list_validation(self) -> None:
        """Test validation when config resolves to empty list."""

        def empty_list_config() -> list[Any]:
            return []

        with patch("sqlspec.utils.config_resolver.import_string", return_value=empty_list_config):
            with pytest.raises(ConfigResolverError, match="resolved to empty list"):
                resolve_config("myapp.config.empty_list_config")

    def test_invalid_config_type_validation(self) -> None:
        """Test validation when config is invalid type."""

        def invalid_config() -> str:
            return "not a config"

        with patch("sqlspec.utils.config_resolver.import_string", return_value=invalid_config):
            with pytest.raises(ConfigResolverError, match="returned invalid type"):
                resolve_config("myapp.config.invalid_config")

    def test_invalid_config_in_list_validation(self) -> None:
        """Test validation when list contains invalid config."""
        mock_valid_config = Mock()
        mock_valid_config.database_url = "sqlite:///test.db"
        mock_valid_config.bind_key = "test"
        mock_valid_config.migration_config = {}

        def mixed_config_list() -> list[Any]:
            return [mock_valid_config, "invalid_config"]

        with patch("sqlspec.utils.config_resolver.import_string", return_value=mixed_config_list):
            with pytest.raises(ConfigResolverError, match="returned invalid config at index"):
                resolve_config("myapp.config.mixed_configs")

    def test_config_validation_attributes(self) -> None:
        """Test that config validation checks for required attributes."""
        # Test config missing database_url
        mock_config = Mock()
        mock_config.bind_key = "test"
        mock_config.migration_config = {}
        del mock_config.database_url  # Remove the attribute

        def incomplete_config() -> Mock:
            return mock_config

        with patch("sqlspec.utils.config_resolver.import_string", return_value=incomplete_config):
            with pytest.raises(ConfigResolverError, match="returned invalid type"):
                resolve_config("myapp.config.incomplete_config")
