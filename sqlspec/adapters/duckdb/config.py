from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from duckdb import DuckDBPyConnection

from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence


__all__ = ("DuckDBConfig", "ExtensionConfig")


@dataclass
class ExtensionConfig:
    """Configuration for a DuckDB extension.

    This class provides configuration options for DuckDB extensions, including installation
    and post-install configuration settings.

    For details see: https://duckdb.org/docs/extensions/overview
    """

    name: str
    """The name of the extension to install"""
    config: dict[str, Any] | None = None
    """Optional configuration settings to apply after installation"""
    force_install: bool = False
    """Whether to force reinstall if already present"""
    repository: str | None = None
    """Optional repository name to install from"""
    repository_url: str | None = None
    """Optional repository URL to install from"""
    version: str | None = None
    """Optional version of the extension to install"""

    @classmethod
    def from_dict(cls, name: str, config: dict[str, Any] | bool | None = None) -> ExtensionConfig:
        """Create an ExtensionConfig from a configuration dictionary.

        Args:
            name: The name of the extension
            config: Configuration dictionary that may contain settings

        Returns:
            A new ExtensionConfig instance
        """
        if config is None:
            return cls(name=name)

        if not isinstance(config, dict):
            config = {"force_install": bool(config)}

        install_args = {
            key: config.pop(key)
            for key in ["force_install", "repository", "repository_url", "version", "config", "name"]
            if key in config
        }
        return cls(name=name, **install_args)


@dataclass
class DuckDBConfig(NoPoolSyncConfig[DuckDBPyConnection]):
    """Configuration for DuckDB database connections.

    This class provides configuration options for DuckDB database connections, wrapping all parameters
    available to duckdb.connect().

    For details see: https://duckdb.org/docs/api/python/overview#connection-options
    """

    database: str | EmptyType = Empty
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk. If not specified, an in-memory database will be created."""

    read_only: bool | EmptyType = Empty
    """If True, the database will be opened in read-only mode. This is required if multiple processes want to access the same database file at the same time."""

    config: dict[str, Any] | EmptyType = Empty
    """A dictionary of configuration options to be passed to DuckDB. These can include settings like 'access_mode', 'max_memory', 'threads', etc.

    For details see: https://duckdb.org/docs/api/python/overview#connection-options
    """

    extensions: Sequence[ExtensionConfig] | EmptyType = Empty
    """A sequence of extension configurations to install and configure upon connection creation."""

    def __post_init__(self) -> None:
        """Post-initialization validation and processing.

        This method handles merging extension configurations from both the extensions field
        and the config dictionary, if present. The config['extensions'] field can be either:
        - A dictionary mapping extension names to their configurations
        - A list of extension names (which will be installed with force_install=True)

        Raises:
            ImproperConfigurationError: If there are duplicate extension configurations.
        """
        if self.config is Empty:
            self.config = {}

        if self.extensions is Empty:
            self.extensions = []
        # this is purely for mypy
        assert isinstance(self.config, dict)  # noqa: S101
        assert isinstance(self.extensions, list)  # noqa: S101

        _e = self.config.pop("extensions", {})
        if not isinstance(_e, (dict, list, tuple)):
            msg = "When configuring extensions in the 'config' dictionary, the value must be a dictionary or sequence of extension names"
            raise ImproperConfigurationError(msg)
        if not isinstance(_e, dict):
            _e = {str(ext): {"force_install": False} for ext in _e}  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]

        if len(set(_e.keys()).intersection({ext.name for ext in self.extensions})) > 0:  # pyright: ignore[ reportUnknownArgumentType]
            msg = "Configuring the same extension in both 'extensions' and as a key in 'config['extensions']' is not allowed"
            raise ImproperConfigurationError(msg)

        self.extensions.extend([ExtensionConfig.from_dict(name, ext_config) for name, ext_config in _e.items()])  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]

    def _configure_extensions(self, connection: DuckDBPyConnection) -> None:
        """Configure extensions for the connection.

        Args:
            connection: The DuckDB connection to configure extensions for.

        Raises:
            ImproperConfigurationError: If extension installation or configuration fails.
        """
        if self.extensions is Empty:
            return

        for extension in cast("list[ExtensionConfig]", self.extensions):
            try:
                if extension.force_install:
                    connection.install_extension(
                        extension=extension.name,
                        force_install=extension.force_install,
                        repository=extension.repository,
                        repository_url=extension.repository_url,
                        version=extension.version,
                    )
                connection.load_extension(extension.name)

                if extension.config:
                    for key, value in extension.config.items():
                        connection.execute(f"SET {key}={value}")
            except Exception as e:
                msg = f"Failed to configure extension {extension.name}. Error: {e!s}"
                raise ImproperConfigurationError(msg) from e

    @property
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the duckdb.connect() function.
        """
        config = dataclass_to_dict(self, exclude_empty=True, exclude={"extensions"}, convert_nested=False)
        if not config.get("database"):
            config["database"] = ":memory:"
        return config

    def create_connection(self) -> DuckDBPyConnection:
        """Create and return a new database connection with configured extensions.

        Returns:
            A new DuckDB connection instance with extensions installed and configured.

        Raises:
            ImproperConfigurationError: If the connection could not be established or extensions could not be configured.
        """
        import duckdb

        try:
            connection = duckdb.connect(**self.connection_config_dict)  # pyright: ignore[reportUnknownMemberType]
            self._configure_extensions(connection)
            return connection
        except Exception as e:
            msg = f"Could not configure the DuckDB connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Generator[DuckDBPyConnection, None, None]:
        """Create and provide a database connection.

        Yields:
            A DuckDB connection instance.

        Raises:
            ImproperConfigurationError: If the connection could not be established.
        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()
