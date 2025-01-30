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
        assert config.name == "test"
        assert config.config is None
        assert not config.force_install
        assert config.repository is None
        assert config.repository_url is None
        assert config.version is None

    def test_from_dict_empty_config(self) -> None:
        """Test from_dict with empty config."""
        config = ExtensionConfig.from_dict("test")
        assert config.name == "test"
        assert config.config is None
        assert not config.force_install

    def test_from_dict_with_install_args(self) -> None:
        """Test from_dict with installation arguments."""
        config = ExtensionConfig.from_dict(
            "test",
            {
                "force_install": True,
                "repository": "custom_repo",
                "repository_url": "https://example.com",
                "version": "1.0.0",
                "config": {"some_setting": "value"},
            },
        )
        assert config.name == "test"
        assert config.force_install
        assert config.repository == "custom_repo"
        assert config.repository_url == "https://example.com"
        assert config.version == "1.0.0"
        assert config.config == {"some_setting": "value"}

    def test_from_dict_with_only_config(self) -> None:
        """Test from_dict with only config settings."""
        config = ExtensionConfig.from_dict("test", {"config": {"some_setting": "value"}})
        assert config.name == "test"
        assert config.config == {"some_setting": "value"}
        assert not config.force_install


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
                "extensions": {
                    "ext1": True,
                    "ext2": {
                        "force_install": True,
                        "repository": "repo",
                        "config": {"setting": "value"},
                    },
                }
            },
        )
        assert isinstance(config.extensions, list)
        assert len(config.extensions) == 2
        ext1 = next(ext for ext in config.extensions if ext.name == "ext1")
        ext2 = next(ext for ext in config.extensions if ext.name == "ext2")
        assert ext1.force_install
        assert ext2.force_install
        assert ext2.repository == "repo"
        assert ext2.config == {"setting": "value"}

    def test_extensions_from_list(self) -> None:
        """Test extension configuration from list."""
        config = DuckDBConfig(config={"extensions": ["ext1", "ext2"]})
        assert isinstance(config.extensions, list)
        assert len(config.extensions) == 2
        assert all(isinstance(ext, ExtensionConfig) for ext in config.extensions)
        assert {ext.name for ext in config.extensions} == {"ext1", "ext2"}
        assert all(not ext.force_install for ext in config.extensions)

    def test_extensions_from_both_sources(self) -> None:
        """Test extension configuration from both extensions and config."""
        config = DuckDBConfig(
            extensions=[ExtensionConfig("ext1")],
            config={"extensions": {"ext2": {"force_install": True}}},
        )
        assert isinstance(config.extensions, list)
        assert len(config.extensions) == 2
        assert {ext.name for ext in config.extensions} == {"ext1", "ext2"}

    def test_duplicate_extensions_error(self) -> None:
        """Test error on duplicate extension configuration."""
        with pytest.raises(ImproperConfigurationError, match="Configuring the same extension"):
            DuckDBConfig(
                extensions=[ExtensionConfig("ext1")],
                config={"extensions": {"ext1": True}},
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
                ExtensionConfig(name="test", force_install=False),
                [("load_extension", {})],
            ),
            (
                ExtensionConfig(name="test", force_install=True, config={"setting": "value"}),
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
                ExtensionConfig(
                    "test",
                    force_install=True,
                    repository="repo",
                    repository_url="url",
                    version="1.0",
                ),
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
        mock_duckdb_connection.load_extension.side_effect = Exception("Test error")
        config = DuckDBConfig(extensions=[ExtensionConfig("test")])

        with pytest.raises(ImproperConfigurationError, match="Failed to configure extension test"):
            config.create_connection()

    def test_connection_creation_error(self) -> None:
        """Test error handling during connection creation."""
        with patch("duckdb.connect", side_effect=Exception("Test error")):
            config = DuckDBConfig()
            with pytest.raises(ImproperConfigurationError, match="Could not configure"):
                config.create_connection()
