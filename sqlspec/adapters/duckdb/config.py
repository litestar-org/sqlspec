from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union, cast

from duckdb import DuckDBPyConnection
from typing_extensions import NotRequired, TypedDict

from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence


__all__ = ("DuckDBConfig", "ExtensionConfig")


class ExtensionConfig(TypedDict):
    """Configuration for a DuckDB extension.

    This class provides configuration options for DuckDB extensions, including installation
    and post-install configuration settings.

    For details see: https://duckdb.org/docs/extensions/overview
    """

    name: str
    """The name of the extension to install"""
    config: "NotRequired[dict[str, Any]]"
    """Optional configuration settings to apply after installation"""
    force_install: "NotRequired[bool]"
    """Whether to force reinstall if already present"""
    repository: "NotRequired[str]"
    """Optional repository name to install from"""
    repository_url: "NotRequired[str]"
    """Optional repository URL to install from"""
    version: "NotRequired[str]"
    """Optional version of the extension to install"""


@dataclass
class DuckDBConfig(NoPoolSyncConfig[DuckDBPyConnection]):
    """Configuration for DuckDB database connections.

    This class provides configuration options for DuckDB database connections, wrapping all parameters
    available to duckdb.connect().

    For details see: https://duckdb.org/docs/api/python/overview#connection-options
    """

    database: "Union[str, EmptyType]" = Empty
    """The path to the database file to be opened. Pass ":memory:" to open a connection to a database that resides in RAM instead of on disk. If not specified, an in-memory database will be created."""

    read_only: "Union[bool, EmptyType]" = Empty
    """If True, the database will be opened in read-only mode. This is required if multiple processes want to access the same database file at the same time."""

    config: "Union[dict[str, Any], EmptyType]" = Empty
    """A dictionary of configuration options to be passed to DuckDB. These can include settings like 'access_mode', 'max_memory', 'threads', etc.

    For details see: https://duckdb.org/docs/api/python/overview#connection-options
    """

    extensions: "Union[Sequence[ExtensionConfig], ExtensionConfig, EmptyType]" = Empty
    """A sequence of extension configurations to install and configure upon connection creation."""

    def __post_init__(self) -> None:
        """Post-initialization validation and processing.


        Raises:
            ImproperConfigurationError: If there are duplicate extension configurations.
        """
        if self.config is Empty:
            self.config = {}

        if self.extensions is Empty:
            self.extensions = []
        if isinstance(self.extensions, dict):
            self.extensions = [self.extensions]
        # this is purely for mypy
        assert isinstance(self.config, dict)  # noqa: S101
        assert isinstance(self.extensions, list)  # noqa: S101
        config_exts: list[ExtensionConfig] = self.config.pop("extensions", [])
        if not isinstance(config_exts, list):  # pyright: ignore[reportUnnecessaryIsInstance]
            config_exts = [config_exts]  # type: ignore[unreachable]

        try:
            if (
                len(set({ext["name"] for ext in config_exts}).intersection({ext["name"] for ext in self.extensions}))
                > 0
            ):  # pyright: ignore[ reportUnknownArgumentType]
                msg = "Configuring the same extension in both 'extensions' and as a key in 'config['extensions']' is not allowed.  Please use only one method to configure extensions."
                raise ImproperConfigurationError(msg)
        except (KeyError, TypeError) as e:
            msg = "When configuring extensions in the 'config' dictionary, the value must be a dictionary or sequence of extension names"
            raise ImproperConfigurationError(msg) from e
        self.extensions.extend(config_exts)

    def _configure_connection(self, connection: "DuckDBPyConnection") -> None:
        """Configure the connection.

        Args:
            connection: The DuckDB connection to configure.
        """
        for config in cast("list[str]", self.config):
            connection.execute(config)

    def _configure_extensions(self, connection: "DuckDBPyConnection") -> None:
        """Configure extensions for the connection.

        Args:
            connection: The DuckDB connection to configure extensions for.


        """
        if self.extensions is Empty:
            return

        for extension in cast("list[ExtensionConfig]", self.extensions):
            self._configure_extension(connection, extension)

    @staticmethod
    def _configure_extension(connection: "DuckDBPyConnection", extension: ExtensionConfig) -> None:
        """Configure a single extension for the connection.

        Args:
            connection: The DuckDB connection to configure extension for.
            extension: The extension configuration to apply.

        Raises:
            ImproperConfigurationError: If extension installation or configuration fails.
        """
        try:
            if extension.get("force_install"):
                connection.install_extension(
                    extension=extension["name"],
                    force_install=extension.get("force_install", False),
                    repository=extension.get("repository"),
                    repository_url=extension.get("repository_url"),
                    version=extension.get("version"),
                )
            connection.load_extension(extension["name"])

            if extension.get("config"):
                for key, value in extension.get("config", {}).items():
                    connection.execute(f"SET {key}={value}")
        except Exception as e:
            msg = f"Failed to configure extension {extension['name']}. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @property
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict.

        Returns:
            A string keyed dict of config kwargs for the duckdb.connect() function.
        """
        config = dataclass_to_dict(self, exclude_empty=True, exclude={"extensions"}, convert_nested=False)
        if not config.get("database"):
            config["database"] = ":memory:"
        return config

    def create_connection(self) -> "DuckDBPyConnection":
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
            self._configure_connection(connection)
        except Exception as e:
            msg = f"Could not configure the DuckDB connection. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e
        else:
            return connection

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBPyConnection, None, None]":
        """Create and provide a database connection.

        Yields:
            A DuckDB connection instance.


        """
        connection = self.create_connection()
        try:
            yield connection
        finally:
            connection.close()
