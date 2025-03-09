"""Tests for DuckDB configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from _pytest.fixtures import FixtureRequest

from sqlspec.adapters.duckdb.config import DuckDBConfig, ExtensionConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def mock_duckdb_connection() -> Generator[MagicMock, None, None]:
    """Create a mock DuckDB connection."""
    with patch("duckdb.connect") as mock_connect:
        connection = MagicMock()
        mock_connect.return_value = connection
        yield connection


class TestExtensionConfig:
    """Test ExtensionConfig class."""

    def test_default_values(self) -> None:
        """Test default values for ExtensionConfig."""
        config = ExtensionConfig(name="test")
        assert config["name"] == "test"
        assert config.get("config") is None
        assert config.get("force_install") is None
        assert config.get("repository") is None
        assert config.get("repository_url") is None
        assert config.get("version") is None

    def test_from_dict_empty_config(self) -> None:
        """Test from_dict with empty config."""
        config = ExtensionConfig(name="test")
        assert config["name"] == "test"
        assert config.get("config") is None
        assert config.get("force_install") is None

    def test_from_dict_with_install_args(self) -> None:
        """Test from_dict with installation arguments."""
        config = ExtensionConfig(
            name="test",
            force_install=True,
            repository="custom_repo",
            repository_url="https://example.com",
            version="1.0.0",
            config={"some_setting": "value"},
        )
        assert config["name"] == "test"
        assert config.get("force_install")
        assert config.get("repository") == "custom_repo"
        assert config.get("repository_url") == "https://example.com"
        assert config.get("version") == "1.0.0"
        assert config.get("config") == {"some_setting": "value"}

    def test_from_dict_with_only_config(self) -> None:
        """Test from_dict with only config settings."""
        config = ExtensionConfig(
            name="test",
            config={"some_setting": "value"},
        )
        assert config["name"] == "test"
        assert config.get("config") == {"some_setting": "value"}
        assert config.get("force_install") is None


class TestDuckDBConfig:
    """Test DuckDBConfig class."""

    def test_default_values(self) -> None:
        """Test default values for DuckDBConfig."""
        config = DuckDBConfig()
        assert config.database is Empty
        assert config.read_only is Empty
        assert config.config == {}
        assert isinstance(config.extensions, list)
        assert not config.extensions

    def test_connection_config_dict_defaults(self) -> None:
        """Test connection_config_dict with default values."""
        config = DuckDBConfig()
        assert config.connection_config_dict == {"database": ":memory:", "config": {}}

    def test_connection_config_dict_with_values(self) -> None:
        """Test connection_config_dict with custom values."""
        config = DuckDBConfig(database="test.db", read_only=True)
        assert config.connection_config_dict == {"database": "test.db", "read_only": True, "config": {}}

    def test_extensions_from_config_dict(self) -> None:
        """Test extension configuration from config dictionary."""
        config = DuckDBConfig(
            config={
                "extensions": [
                    {"name": "ext1"},
                    {"name": "ext2", "force_install": True, "repository": "repo", "config": {"setting": "value"}},
                ],
            },
        )
        assert isinstance(config.extensions, list)
        assert len(config.extensions) == 2
        ext1 = next(ext for ext in config.extensions if ext["name"] == "ext1")
        ext2 = next(ext for ext in config.extensions if ext["name"] == "ext2")
        assert ext1.get("force_install") is None
        assert ext2.get("force_install")
        assert ext2.get("repository") == "repo"
        assert ext2.get("config") == {"setting": "value"}

    def test_extensions_from_both_sources(self) -> None:
        """Test extension configuration from both extensions and config."""
        config = DuckDBConfig(
            extensions=[{"name": "ext1"}],
            config={"extensions": [{"name": "ext2", "force_install": True}]},
        )
        assert isinstance(config.extensions, list)
        assert len(config.extensions) == 2
        assert {ext["name"] for ext in config.extensions} == {"ext1", "ext2"}

    def test_duplicate_extensions_error(self) -> None:
        """Test error on duplicate extension configuration."""
        with pytest.raises(ImproperConfigurationError, match="Configuring the same extension"):
            DuckDBConfig(
                extensions=[{"name": "ext1"}],
                config={"extensions": {"name": "ext1", "force_install": True}},
            )

    def test_invalid_extensions_type_error(self) -> None:
        """Test error on invalid extensions type."""
        with pytest.raises(
            ImproperConfigurationError,
            match="When configuring extensions in the 'config' dictionary, the value must be a dictionary or sequence of extension names",
        ):
            DuckDBConfig(config={"extensions": 123})

    @pytest.mark.parametrize(
        ("extension_config", "expected_calls"),
        [  # pyright: ignore[reportUnknownArgumentType]
            (
                ExtensionConfig(name="test", force_install=True),
                [
                    (
                        "install_extension",
                        {
                            "extension": "test",
                            "force_install": True,
                            "repository": None,
                            "repository_url": None,
                            "version": None,
                        },
                    ),
                    ("load_extension", {}),
                ],
            ),
            (
                {"name": "test", "force_install": False},
                [("load_extension", {})],
            ),
            (
                {"name": "test", "force_install": True, "config": {"setting": "value"}},
                [
                    (
                        "install_extension",
                        {
                            "extension": "test",
                            "force_install": True,
                            "repository": None,
                            "repository_url": None,
                            "version": None,
                        },
                    ),
                    ("load_extension", {}),
                    ("execute", {"query": "SET setting=value"}),
                ],
            ),
            (
                {
                    "name": "test",
                    "force_install": True,
                    "repository": "repo",
                    "repository_url": "url",
                    "version": "1.0",
                },
                [
                    (
                        "install_extension",
                        {
                            "extension": "test",
                            "force_install": True,
                            "repository": "repo",
                            "repository_url": "url",
                            "version": "1.0",
                        },
                    ),
                    ("load_extension", {}),
                ],
            ),
        ],
    )
    def test_configure_extensions(
        self,
        request: FixtureRequest,
        mock_duckdb_connection: MagicMock,
        extension_config: ExtensionConfig,
        expected_calls: list[tuple[str, dict[str, Any]]],
    ) -> None:
        """Test extension configuration with various settings."""
        config = DuckDBConfig(extensions=[extension_config])

        # Configure the mock to match expected behavior
        for method_name, _kwargs in expected_calls:
            if method_name == "execute":
                continue  # Skip pre-configuring execute calls as they're variable

            getattr(mock_duckdb_connection, method_name).return_value = None

        connection = config.create_connection()

        actual_calls = []
        for method_name, _kwargs in expected_calls:
            method = getattr(connection, method_name)
            assert method.called, f"Method {method_name} was not called"
            if method_name == "execute":
                actual_calls.append((method_name, {"query": method.call_args.args[0]}))  # pyright: ignore[reportUnknownMemberType]
            else:
                actual_calls.append((method_name, method.call_args.kwargs))  # pyright: ignore[reportUnknownMemberType]

        assert actual_calls == expected_calls

    def test_extension_configuration_error(self, mock_duckdb_connection: MagicMock) -> None:
        """Test error handling during extension configuration."""
        # Simulate an error during extension loading
        mock_duckdb_connection.load_extension.side_effect = Exception("Test error")

        # Force the implementation to call load_extension
        mock_duckdb_connection.install_extension.return_value = None

        config = DuckDBConfig(extensions=[{"name": "test", "force_install": True}])

        with pytest.raises(ImproperConfigurationError, match="Failed to configure extension test"):
            config.create_connection()

    def test_connection_creation_error(self) -> None:
        """Test error handling during connection creation."""
        with patch("duckdb.connect", side_effect=Exception("Test error")):
            config = DuckDBConfig()
            with pytest.raises(ImproperConfigurationError, match="Could not configure"):
                config.create_connection()
