"""DuckDB database configuration with connection pooling."""

from collections.abc import Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, TypedDict, Union, cast

from typing_extensions import NotRequired

from sqlspec.adapters.duckdb._types import DuckDBConnection
from sqlspec.adapters.duckdb.driver import DuckDBCursor, DuckDBDriver, duckdb_statement_config
from sqlspec.adapters.duckdb.pool import DuckDBConnectionPool
from sqlspec.config import SyncDatabaseConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from sqlspec.core.statement import StatementConfig

__all__ = (
    "DuckDBConfig",
    "DuckDBConnectionParams",
    "DuckDBDriverFeatures",
    "DuckDBExtensionConfig",
    "DuckDBPoolParams",
    "DuckDBSecretConfig",
)


class DuckDBConnectionParams(TypedDict, total=False):
    """DuckDB connection parameters."""

    database: NotRequired[str]
    read_only: NotRequired[bool]
    config: NotRequired[dict[str, Any]]
    memory_limit: NotRequired[str]
    threads: NotRequired[int]
    temp_directory: NotRequired[str]
    max_temp_directory_size: NotRequired[str]
    autoload_known_extensions: NotRequired[bool]
    autoinstall_known_extensions: NotRequired[bool]
    allow_community_extensions: NotRequired[bool]
    allow_unsigned_extensions: NotRequired[bool]
    extension_directory: NotRequired[str]
    custom_extension_repository: NotRequired[str]
    autoinstall_extension_repository: NotRequired[str]
    allow_persistent_secrets: NotRequired[bool]
    enable_external_access: NotRequired[bool]
    secret_directory: NotRequired[str]
    enable_object_cache: NotRequired[bool]
    parquet_metadata_cache: NotRequired[str]
    enable_external_file_cache: NotRequired[bool]
    checkpoint_threshold: NotRequired[str]
    enable_progress_bar: NotRequired[bool]
    progress_bar_time: NotRequired[float]
    enable_logging: NotRequired[bool]
    log_query_path: NotRequired[str]
    logging_level: NotRequired[str]
    preserve_insertion_order: NotRequired[bool]
    default_null_order: NotRequired[str]
    default_order: NotRequired[str]
    ieee_floating_point_ops: NotRequired[bool]
    binary_as_string: NotRequired[bool]
    arrow_large_buffer_size: NotRequired[bool]
    errors_as_json: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class DuckDBPoolParams(DuckDBConnectionParams, total=False):
    """Complete pool configuration for DuckDB adapter.

    Combines standardized pool parameters with DuckDB-specific connection parameters.
    """

    pool_min_size: NotRequired[int]
    pool_max_size: NotRequired[int]
    pool_timeout: NotRequired[float]
    pool_recycle_seconds: NotRequired[int]


class DuckDBExtensionConfig(TypedDict, total=False):
    """DuckDB extension configuration for auto-management."""

    name: str
    """Name of the extension to install/load."""

    version: NotRequired[str]
    """Specific version of the extension."""

    repository: NotRequired[str]
    """Repository for the extension (core, community, or custom URL)."""

    force_install: NotRequired[bool]
    """Force reinstallation of the extension."""


class DuckDBSecretConfig(TypedDict, total=False):
    """DuckDB secret configuration for AI/API integrations."""

    secret_type: str
    """Type of secret (e.g., 'openai', 'aws', 'azure', 'gcp')."""

    name: str
    """Name of the secret."""

    value: dict[str, Any]
    """Secret configuration values."""

    scope: NotRequired[str]
    """Scope of the secret (LOCAL or PERSISTENT)."""


class DuckDBDriverFeatures(TypedDict, total=False):
    """TypedDict for DuckDB driver features configuration."""

    extensions: NotRequired[Sequence[DuckDBExtensionConfig]]
    """List of extensions to install/load on connection creation."""
    secrets: NotRequired[Sequence[DuckDBSecretConfig]]
    """List of secrets to create for AI/API integrations."""
    on_connection_create: NotRequired["Callable[[DuckDBConnection], Optional[DuckDBConnection]]"]
    """Callback executed when connection is created."""


class DuckDBConfig(SyncDatabaseConfig[DuckDBConnection, DuckDBConnectionPool, DuckDBDriver]):
    """DuckDB configuration with connection pooling.

    This configuration supports DuckDB's features including:

    - Connection pooling
    - Extension management and installation
    - Secret management for API integrations
    - Auto configuration settings
    - Arrow integration
    - Direct file querying capabilities

    DuckDB Connection Pool Configuration:
    - Default pool size is 1-4 connections (DuckDB uses single connection by default)
    - Connection recycling is set to 24 hours by default (set to 0 to disable)
    - Shared memory databases use `:memory:shared_db` for proper concurrency
    """

    driver_type: "ClassVar[type[DuckDBDriver]]" = DuckDBDriver
    connection_type: "ClassVar[type[DuckDBConnection]]" = DuckDBConnection

    def __init__(
        self,
        *,
        pool_config: "Optional[Union[DuckDBPoolParams, dict[str, Any]]]" = None,
        pool_instance: "Optional[DuckDBConnectionPool]" = None,
        migration_config: Optional[dict[str, Any]] = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[Union[DuckDBDriverFeatures, dict[str, Any]]]" = None,
    ) -> None:
        """Initialize DuckDB configuration."""
        if pool_config is None:
            pool_config = {}
        if "database" not in pool_config:
            pool_config["database"] = ":memory:shared_db"

        if pool_config.get("database") in {":memory:", ""}:
            pool_config["database"] = ":memory:shared_db"

        super().__init__(
            pool_config=dict(pool_config),
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or duckdb_statement_config,
            driver_features=cast("dict[str, Any]", driver_features),
        )

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""
        return {
            k: v
            for k, v in self.pool_config.items()
            if v is not None
            and k not in {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        }

    def _create_pool(self) -> DuckDBConnectionPool:
        """Create connection pool from configuration."""
        connection_config = self._get_connection_config_dict()

        extensions = self.driver_features.get("extensions", None)
        secrets = self.driver_features.get("secrets", None)
        on_connection_create = self.driver_features.get("on_connection_create", None)

        extensions_dicts = [dict(ext) for ext in extensions] if extensions else None
        secrets_dicts = [dict(secret) for secret in secrets] if secrets else None

        pool_callback = None
        if on_connection_create:

            def wrapped_callback(conn: DuckDBConnection) -> None:
                on_connection_create(conn)

            pool_callback = wrapped_callback

        return DuckDBConnectionPool(
            connection_config=connection_config,
            extensions=extensions_dicts,
            secrets=secrets_dicts,
            on_connection_create=pool_callback,
            **self.pool_config,
        )

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> DuckDBConnection:
        """Get a DuckDB connection from the pool.

        This method ensures the pool is created and returns a connection
        from the pool. The connection is checked out from the pool and must
        be properly managed by the caller.

        Returns:
            DuckDBConnection: A connection from the pool

        Note:
            For automatic connection management, prefer using provide_connection()
            or provide_session() which handle returning connections to the pool.
            The caller is responsible for returning the connection to the pool
            using pool.release(connection) when done.
        """
        pool = self.provide_pool()

        return pool.acquire()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[DuckDBConnection, None, None]":
        """Provide a pooled DuckDB connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A DuckDB connection instance.
        """
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "Generator[DuckDBDriver, None, None]":
        """Provide a DuckDB driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A context manager that yields a DuckDBDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as connection:
            driver = self.driver_type(connection=connection, statement_config=statement_config or self.statement_config)
            yield driver

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for DuckDB types.

        This provides all DuckDB-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({"DuckDBConnection": DuckDBConnection, "DuckDBCursor": DuckDBCursor})
        return namespace
