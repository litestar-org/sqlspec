"""OracleDB database configuration with direct field-based configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

import oracledb
from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.oracledb._numpy_handlers import register_numpy_handlers  # pyright: ignore[reportPrivateUsage]
from sqlspec.adapters.oracledb._typing import (
    OracleAsyncConnection,
    OracleAsyncConnectionPool,
    OracleSyncConnection,
    OracleSyncConnectionPool,
)
from sqlspec.adapters.oracledb._uuid_handlers import register_uuid_handlers
from sqlspec.adapters.oracledb.core import apply_driver_features, default_statement_config, requires_session_callback
from sqlspec.adapters.oracledb.driver import (
    OracleAsyncCursor,
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleAsyncSessionContext,
    OracleSyncCursor,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
    OracleSyncSessionContext,
)
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.utils.config_tools import normalize_connection_config, reject_pool_aliases

if TYPE_CHECKING:
    from collections.abc import Callable

    from oracledb import AuthMode

    from sqlspec.core import StatementConfig


__all__ = (
    "OracleAsyncConfig",
    "OracleConnectionParams",
    "OracleDriverFeatures",
    "OraclePoolParams",
    "OracleSyncConfig",
)


class OracleConnectionParams(TypedDict):
    """OracleDB connection parameters."""

    dsn: NotRequired[str]
    user: NotRequired[str]
    password: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    service_name: NotRequired[str]
    sid: NotRequired[str]
    wallet_location: NotRequired[str]
    wallet_password: NotRequired[str]
    config_dir: NotRequired[str]
    tcp_connect_timeout: NotRequired[float]
    retry_count: NotRequired[int]
    retry_delay: NotRequired[int]
    mode: NotRequired["AuthMode"]
    events: NotRequired[bool]
    edition: NotRequired[str]


class OraclePoolParams(OracleConnectionParams):
    """OracleDB pool parameters."""

    min: NotRequired[int]
    max: NotRequired[int]
    increment: NotRequired[int]
    threaded: NotRequired[bool]
    getmode: NotRequired[Any]
    homogeneous: NotRequired[bool]
    timeout: NotRequired[int]
    wait_timeout: NotRequired[int]
    max_lifetime_session: NotRequired[int]
    session_callback: NotRequired["Callable[..., Any]"]
    max_sessions_per_shard: NotRequired[int]
    soda_metadata_cache: NotRequired[bool]
    ping_interval: NotRequired[int]
    extra: NotRequired["dict[str, Any]"]


class OracleDriverFeatures(TypedDict):
    """Oracle driver feature flags.

    enable_numpy_vectors: Enable automatic NumPy array ↔ Oracle VECTOR conversion.
        Requires NumPy and Oracle Database 23ai or higher with VECTOR data type support.
        Defaults to True when NumPy is installed.
        Provides automatic bidirectional conversion between NumPy ndarrays and Oracle VECTOR columns.
        Supports float32, float64, int8, and uint8 dtypes.
    enable_lowercase_column_names: Normalize implicit Oracle uppercase column names to lowercase.
        Targets unquoted Oracle identifiers that default to uppercase while preserving quoted case-sensitive aliases.
        Defaults to True for compatibility with schema libraries expecting snake_case fields.
    enable_uuid_binary: Enable automatic UUID ↔ RAW(16) binary conversion.
        When True (default), Python UUID objects are automatically converted to/from
        RAW(16) binary format for optimal storage efficiency (16 bytes vs 36 bytes).
        Applies only to RAW(16) columns; other RAW sizes remain unchanged.
        Uses Python's stdlib uuid module (no external dependencies).
        Defaults to True for improved type safety and storage efficiency.
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
        Provides pub/sub capabilities via Oracle Advanced Queuing or table-backed fallback.
        Requires extension_config["events"] for migration setup when using table_queue backend.
    events_backend: Event channel backend selection.
        Options: "advanced_queue", "table_queue"
        - "advanced_queue": Oracle Advanced Queuing (native messaging, requires DBMS_AQADM privileges)
        - "table_queue": Durable table-backed queue with retries and exactly-once delivery
        Defaults to "table_queue" (works on all Oracle editions without special privileges).
    """

    enable_numpy_vectors: NotRequired[bool]
    enable_lowercase_column_names: NotRequired[bool]
    enable_uuid_binary: NotRequired[bool]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class OracleSyncConnectionContext:
    """Context manager for Oracle sync connections."""

    __slots__ = ("_config", "_conn")

    def __init__(self, config: "OracleSyncConfig") -> None:
        self._config = config
        self._conn: OracleSyncConnection | None = None

    def __enter__(self) -> "OracleSyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = self._config.create_pool()
        self._conn = self._config.connection_instance.acquire()
        return self._conn

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._conn:
            if self._config.connection_instance:
                self._config.connection_instance.release(self._conn)
            self._conn = None
        return None


class _OracleSyncSessionConnectionHandler:
    __slots__ = ("_config", "_conn")

    def __init__(self, config: "OracleSyncConfig") -> None:
        self._config = config
        self._conn: OracleSyncConnection | None = None

    def acquire_connection(self) -> "OracleSyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = self._config.create_pool()
        self._conn = self._config.connection_instance.acquire()
        return self._conn

    def release_connection(self, _conn: "OracleSyncConnection") -> None:
        if self._conn is None:
            return
        if self._config.connection_instance:
            self._config.connection_instance.release(self._conn)
        self._conn = None


class OracleSyncConfig(SyncDatabaseConfig[OracleSyncConnection, "OracleSyncConnectionPool", OracleSyncDriver]):
    """Configuration for Oracle synchronous database connections."""

    __slots__ = ()

    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver
    connection_type: "ClassVar[type[OracleSyncConnection]]" = OracleSyncConnection
    migration_tracker_type: "ClassVar[type[OracleSyncMigrationTracker]]" = OracleSyncMigrationTracker
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "OraclePoolParams | dict[str, Any] | None" = None,
        connection_instance: "OracleSyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings).
            **kwargs: Additional keyword arguments.
        """
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)
        statement_config = statement_config or default_statement_config

        driver_features = apply_driver_features(driver_features)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create the actual connection pool."""
        config = dict(self.connection_config)

        if requires_session_callback(self.driver_features):
            config["session_callback"] = self._init_connection

        return oracledb.create_pool(**config)

    def _init_connection(self, connection: "OracleSyncConnection", tag: str) -> None:
        """Initialize connection with optional type handlers.

        Registers NumPy vector handlers and UUID binary handlers when enabled.
        Registration order ensures handler chaining works correctly.

        Args:
            connection: Oracle connection to initialize.
            tag: Connection tag for session state (unused).
        """
        if self.driver_features.get("enable_numpy_vectors", False):
            register_numpy_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

    def _close_pool(self) -> None:
        """Close the actual connection pool."""
        if self.connection_instance:
            self.connection_instance.close()
            self.connection_instance = None

    def create_connection(self) -> "OracleSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            An Oracle Connection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = self.create_pool()
        return self.connection_instance.acquire()

    def provide_connection(self) -> "OracleSyncConnectionContext":
        """Provide a connection context manager.

        Returns:
            An Oracle Connection context manager.
        """
        return OracleSyncConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "OracleSyncSessionContext":
        """Provide a driver session context manager.

        Args:
            *_args: Positional arguments (unused).
            statement_config: Optional statement configuration override.
            **_kwargs: Keyword arguments (unused).

        Returns:
            An OracleSyncDriver session context manager.
        """
        handler = _OracleSyncSessionConnectionHandler(self)

        return OracleSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    def provide_pool(self) -> "OracleSyncConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleDB types.

        Provides OracleDB-specific types for Litestar framework recognition.

        Returns:
            Dictionary mapping type names to types.
        """

        namespace = super().get_signature_namespace()
        namespace.update({
            "OracleAsyncConnection": OracleAsyncConnection,
            "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
            "OracleAsyncCursor": OracleAsyncCursor,
            "OracleAsyncDriver": OracleAsyncDriver,
            "OracleAsyncExceptionHandler": OracleAsyncExceptionHandler,
            "OracleConnectionParams": OracleConnectionParams,
            "OracleDriverFeatures": OracleDriverFeatures,
            "OraclePoolParams": OraclePoolParams,
            "OracleSyncConnectionContext": OracleSyncConnectionContext,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
            "OracleSyncSessionContext": OracleSyncSessionContext,
        })
        return namespace


class OracleAsyncConnectionContext:
    """Async context manager for Oracle connections."""

    __slots__ = ("_config", "_conn")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        self._config = config
        self._conn: OracleAsyncConnection | None = None

    async def __aenter__(self) -> "OracleAsyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = await self._config.create_pool()
        self._conn = cast("OracleAsyncConnection", await self._config.connection_instance.acquire())
        return self._conn

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._conn:
            if self._config.connection_instance:
                await self._config.connection_instance.release(self._conn)
            self._conn = None
        return None


class _OracleAsyncSessionConnectionHandler:
    __slots__ = ("_config", "_conn")

    def __init__(self, config: "OracleAsyncConfig") -> None:
        self._config = config
        self._conn: OracleAsyncConnection | None = None

    async def acquire_connection(self) -> "OracleAsyncConnection":
        if self._config.connection_instance is None:
            self._config.connection_instance = await self._config.create_pool()
        self._conn = cast("OracleAsyncConnection", await self._config.connection_instance.acquire())
        return self._conn

    async def release_connection(self, _conn: "OracleAsyncConnection") -> None:
        if self._conn is None:
            return
        if self._config.connection_instance:
            await self._config.connection_instance.release(self._conn)
        self._conn = None


@mypyc_attr(native_class=False)
class OracleAsyncConfig(AsyncDatabaseConfig[OracleAsyncConnection, "OracleAsyncConnectionPool", OracleAsyncDriver]):
    """Configuration for Oracle asynchronous database connections."""

    __slots__ = ()

    connection_type: "ClassVar[type[OracleAsyncConnection]]" = OracleAsyncConnection
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver
    migration_tracker_type: "ClassVar[type[OracleAsyncMigrationTracker]]" = OracleAsyncMigrationTracker
    supports_transactional_ddl: ClassVar[bool] = False
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "OraclePoolParams | dict[str, Any] | None" = None,
        connection_instance: "OracleAsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters.
            connection_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings).
            **kwargs: Additional keyword arguments.
        """
        reject_pool_aliases(kwargs)

        connection_config = normalize_connection_config(connection_config)

        driver_features = apply_driver_features(driver_features)

        super().__init__(
            connection_config=connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or default_statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

    async def _create_pool(self) -> "OracleAsyncConnectionPool":
        """Create the actual async connection pool."""
        config = dict(self.connection_config)

        if requires_session_callback(self.driver_features):
            config["session_callback"] = self._init_connection

        return oracledb.create_pool_async(**config)

    async def _init_connection(self, connection: "OracleAsyncConnection", tag: str) -> None:
        """Initialize async connection with optional type handlers.

        Registers NumPy vector handlers and UUID binary handlers when enabled.
        Registration order ensures handler chaining works correctly.

        Args:
            connection: Oracle async connection to initialize.
            tag: Connection tag for session state (unused).
        """
        if self.driver_features.get("enable_numpy_vectors", False):
            register_numpy_handlers(connection)

        if self.driver_features.get("enable_uuid_binary", False):
            register_uuid_handlers(connection)

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if self.connection_instance:
            await self.connection_instance.close()
            self.connection_instance = None

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return cast("OracleAsyncConnection", await self.connection_instance.acquire())

    def provide_connection(self) -> "OracleAsyncConnectionContext":
        """Provide an async connection context manager.

        Returns:
            An Oracle AsyncConnection context manager.
        """
        return OracleAsyncConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "OracleAsyncSessionContext":
        """Provide an async driver session context manager.

        Args:
            *_args: Positional arguments (unused).
            statement_config: Optional statement configuration override.
            **_kwargs: Keyword arguments (unused).

        Returns:
            An OracleAsyncDriver session context manager.
        """
        handler = _OracleAsyncSessionConnectionHandler(self)

        return OracleAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def provide_pool(self) -> "OracleAsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleAsyncConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "OracleAsyncConnectionContext": OracleAsyncConnectionContext,
            "OracleAsyncConnection": OracleAsyncConnection,
            "OracleAsyncConnectionPool": OracleAsyncConnectionPool,
            "OracleAsyncCursor": OracleAsyncCursor,
            "OracleAsyncDriver": OracleAsyncDriver,
            "OracleAsyncExceptionHandler": OracleAsyncExceptionHandler,
            "OracleAsyncSessionContext": OracleAsyncSessionContext,
            "OracleConnectionParams": OracleConnectionParams,
            "OracleDriverFeatures": OracleDriverFeatures,
            "OraclePoolParams": OraclePoolParams,
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
        })
        return namespace
