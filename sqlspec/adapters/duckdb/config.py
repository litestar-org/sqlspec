from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

from duckdb import DuckDBPyConnection
from typing_extensions import Literal, NotRequired, TypedDict

from sqlspec.base import NoPoolSyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.typing import Empty, EmptyType, dataclass_to_dict

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence


__all__ = ("DuckDB", "ExtensionConfig")


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
    install_if_missing: "NotRequired[bool]"
    """Whether to install if missing"""
    force_install: "NotRequired[bool]"
    """Whether to force reinstall if already present"""
    repository: "NotRequired[str]"
    """Optional repository name to install from"""
    repository_url: "NotRequired[str]"
    """Optional repository URL to install from"""
    version: "NotRequired[str]"
    """Optional version of the extension to install"""


class SecretConfig(TypedDict):
    """Configuration for a secret to store in a connection.

    This class provides configuration options for storing a secret in a connection for later retrieval.

    For details see: https://duckdb.org/docs/stable/configuration/secrets_manager
    """

    secret_type: Union[
        Literal[
            "azure", "gcs", "s3", "r2", "huggingface", "http", "mysql", "postgres", "bigquery", "openai", "open_prompt"  # noqa: PYI051
        ],
        str,
    ]
    provider: NotRequired[str]
    """The provider of the secret"""
    name: str
    """The name of the secret to store"""
    value: dict[str, Any]
    """The secret value to store"""
    persist: NotRequired[bool]
    """Whether to persist the secret"""
    replace_if_exists: NotRequired[bool]
    """Whether to replace the secret if it already exists"""


@dataclass
class DuckDB(NoPoolSyncConfig[DuckDBPyConnection]):
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
    secrets: "Union[Sequence[SecretConfig], SecretConfig , EmptyType]" = Empty
    """A dictionary of secrets to store in the connection for later retrieval."""
    auto_update_extensions: "bool" = False
    """Whether to automatically update on connection creation"""
    on_connection_create: "Optional[Callable[[DuckDBPyConnection], None]]" = None
    """A callable to be called after the connection is created."""

    def __post_init__(self) -> None:
        """Post-initialization validation and processing.


        Raises:
            ImproperConfigurationError: If there are duplicate extension configurations.
        """
        if self.config is Empty:
            self.config = {}
        if self.extensions is Empty:
            self.extensions = []
        if self.secrets is Empty:
            self.secrets = []
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
        for key, value in cast("dict[str,Any]", self.config).items():
            connection.execute(f"SET {key}='{value}'")

    def _configure_extensions(self, connection: "DuckDBPyConnection") -> None:
        """Configure extensions for the connection.

        Args:
            connection: The DuckDB connection to configure extensions for.


        """
        if self.extensions is Empty:
            return

        for extension in cast("list[ExtensionConfig]", self.extensions):
            self._configure_extension(connection, extension)
        if self.auto_update_extensions:
            connection.execute("update extensions")

    @staticmethod
    def _secret_exists(connection: "DuckDBPyConnection", name: "str") -> bool:
        """Check if a secret exists in the connection.

        Args:
            connection: The DuckDB connection to check for the secret.
            name: The name of the secret to check for.

        Returns:
            bool: True if the secret exists, False otherwise.
        """
        results = connection.execute("select 1 from duckdb_secrets() where name=?", [name]).fetchone()  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        return results is not None

    @classmethod
    def _is_community_extension(cls, connection: "DuckDBPyConnection", name: "str") -> bool:
        """Check if an extension is a community extension.

        Args:
            connection: The DuckDB connection to check for the extension.
            name: The name of the extension to check.

        Returns:
            bool: True if the extension is a community extension, False otherwise.
        """
        results = connection.execute(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            "select 1 from duckdb_extensions() where extension_name=?", [name]
        ).fetchone()
        return results is None

    @classmethod
    def _extension_installed(cls, connection: "DuckDBPyConnection", name: "str") -> bool:
        """Check if a extension exists in the connection.

        Args:
            connection: The DuckDB connection to check for the secret.
            name: The name of the secret to check for.

        Returns:
            bool: True if the extension is installed, False otherwise.
        """
        results = connection.execute(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            "select 1 from duckdb_extensions() where extension_name=? and installed=true", [name]
        ).fetchone()
        return results is not None

    @classmethod
    def _extension_loaded(cls, connection: "DuckDBPyConnection", name: "str") -> bool:
        """Check if a extension is loaded in the connection.

        Args:
            connection: The DuckDB connection to check for the extension.
            name: The name of the extension to check for.

        Returns:
            bool: True if the extension is loaded, False otherwise.
        """
        results = connection.execute(  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            "select 1 from duckdb_extensions() where extension_name=? and loaded=true", [name]
        ).fetchone()
        return results is not None

    @classmethod
    def _configure_secrets(
        cls,
        connection: "DuckDBPyConnection",
        secrets: "Sequence[SecretConfig]",
    ) -> None:
        """Configure persistent secrets for the connection.

        Args:
            connection: The DuckDB connection to configure secrets for.
            secrets: The list of secrets to store in the connection.

        Raises:
            ImproperConfigurationError: If a secret could not be stored in the connection.
        """
        try:
            for secret in secrets:
                secret_exists = cls._secret_exists(connection, secret["name"])
                if not secret_exists or secret.get("replace_if_exists", False):
                    provider_type = "" if not secret.get("provider") else f"provider {secret.get('provider')},"
                    connection.execute(
                        f"""create or replace {"persistent" if secret.get("persist", False) else ""} secret {secret["name"]} (
                        type {secret["secret_type"]},
                        {provider_type}
                        {" ,".join([f"{k} '{v}'" for k, v in secret["value"].items()])}
                    ) """
                    )
        except Exception as e:
            msg = f"Failed to store secret. Error: {e!s}"
            raise ImproperConfigurationError(msg) from e

    @classmethod
    def _configure_extension(cls, connection: "DuckDBPyConnection", extension: ExtensionConfig) -> None:
        """Configure a single extension for the connection.

        Args:
            connection: The DuckDB connection to configure extension for.
            extension: The extension configuration to apply.

        Raises:
            ImproperConfigurationError: If extension installation or configuration fails.
        """
        try:
            if (
                not cls._extension_installed(connection, extension["name"])
                and extension.get("install_if_missing", True)
            ) or extension.get("force_install"):
                repository = extension.get("repository", None)
                if repository is None and cls._is_community_extension(connection, extension["name"]):
                    repository = "community"
                repository_url = (
                    "https://community-extensions.duckdb.org"
                    if repository == "community" and extension.get("repository_url") is None
                    else extension.get("repository_url", None)
                )
                connection.install_extension(
                    extension=extension["name"],
                    force_install=extension.get("force_install", False),
                    repository=repository,
                    repository_url=repository_url,
                    version=extension.get("version"),
                )
            if not cls._extension_loaded(connection, extension["name"]):
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
        config = dataclass_to_dict(
            self,
            exclude_empty=True,
            exclude={"extensions", "pool_instance", "secrets", "on_connection_create", "auto_update_extensions"},
            convert_nested=False,
        )
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
            self._configure_secrets(connection, cast("list[SecretConfig]", self.secrets))
            self._configure_connection(connection)
            if self.on_connection_create:
                self.on_connection_create(connection)

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
