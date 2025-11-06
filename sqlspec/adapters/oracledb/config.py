"""OracleDB database configuration with direct field-based configuration."""

import contextlib
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

import oracledb
from typing_extensions import NotRequired

from sqlspec.adapters.oracledb._numpy_handlers import register_numpy_handlers
from sqlspec.adapters.oracledb._types import (
    OracleAsyncConnection,
    OracleAsyncConnectionPool,
    OracleSyncConnection,
    OracleSyncConnectionPool,
)
from sqlspec.adapters.oracledb._uuid_handlers import register_uuid_handlers
from sqlspec.adapters.oracledb.driver import (
    OracleAsyncCursor,
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleSyncCursor,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
    oracledb_statement_config,
)
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.config import (
    ADKConfig,
    AsyncDatabaseConfig,
    FastAPIConfig,
    FlaskConfig,
    LitestarConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
    StarletteConfig,
    SyncDatabaseConfig,
)
from sqlspec.typing import NUMPY_INSTALLED

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

    from oracledb import AuthMode

    from sqlspec.core import StatementConfig


__all__ = (
    "OracleAsyncConfig",
    "OracleConnectionParams",
    "OracleDriverFeatures",
    "OraclePoolParams",
    "OracleSyncConfig",
)

logger = logging.getLogger(__name__)


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
    extra: NotRequired[dict[str, Any]]


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
    """

    enable_numpy_vectors: NotRequired[bool]
    enable_lowercase_column_names: NotRequired[bool]
    enable_uuid_binary: NotRequired[bool]


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
        pool_config: "OraclePoolParams | dict[str, Any] | None" = None,
        pool_instance: "OracleSyncConnectionPool | None" = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any] | LitestarConfig | FastAPIConfig | StarletteConfig | FlaskConfig | ADKConfig | OpenTelemetryConfig | PrometheusConfig] | None" = None,
    ) -> None:
        """Initialize Oracle synchronous configuration.

        Args:
            pool_config: Pool configuration parameters.
            pool_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings).
        """

        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)
        statement_config = statement_config or oracledb_statement_config

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        processed_driver_features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
        processed_driver_features.setdefault("enable_lowercase_column_names", True)
        processed_driver_features.setdefault("enable_uuid_binary", True)

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
        )

    def _create_pool(self) -> "OracleSyncConnectionPool":
        """Create the actual connection pool."""
        config = dict(self.pool_config)

        needs_session_callback = self.driver_features.get("enable_numpy_vectors", False) or self.driver_features.get(
            "enable_uuid_binary", False
        )
        if needs_session_callback:
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
        if self.pool_instance:
            self.pool_instance.close()

    def create_connection(self) -> "OracleSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            An Oracle Connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return self.pool_instance.acquire()

    @contextlib.contextmanager
    def provide_connection(self) -> "Generator[OracleSyncConnection, None, None]":
        """Provide a connection context manager.

        Yields:
            An Oracle Connection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        conn = self.pool_instance.acquire()
        try:
            yield conn
        finally:
            self.pool_instance.release(conn)

    @contextlib.contextmanager
    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "Generator[OracleSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Positional arguments (unused).
            statement_config: Optional statement configuration override.
            **kwargs: Keyword arguments (unused).

        Yields:
            An OracleSyncDriver instance.
        """
        _ = (args, kwargs)  # Mark as intentionally unused
        with self.provide_connection() as conn:
            driver = self.driver_type(
                connection=conn,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)

    def provide_pool(self) -> "OracleSyncConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = self.create_pool()
        return self.pool_instance

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
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
        })
        return namespace


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
        pool_config: "OraclePoolParams | dict[str, Any] | None" = None,
        pool_instance: "OracleAsyncConnectionPool | None" = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "OracleDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any] | LitestarConfig | FastAPIConfig | StarletteConfig | FlaskConfig | ADKConfig | OpenTelemetryConfig | PrometheusConfig] | None" = None,
    ) -> None:
        """Initialize Oracle asynchronous configuration.

        Args:
            pool_config: Pool configuration parameters.
            pool_instance: Existing pool instance to use.
            migration_config: Migration configuration.
            statement_config: Default SQL statement configuration.
            driver_features: Optional driver feature configuration (TypedDict or dict).
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings).
        """

        processed_pool_config: dict[str, Any] = dict(pool_config) if pool_config else {}
        if "extra" in processed_pool_config:
            extras = processed_pool_config.pop("extra")
            processed_pool_config.update(extras)

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        processed_driver_features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
        processed_driver_features.setdefault("enable_lowercase_column_names", True)
        processed_driver_features.setdefault("enable_uuid_binary", True)

        super().__init__(
            pool_config=processed_pool_config,
            pool_instance=pool_instance,
            migration_config=migration_config,
            statement_config=statement_config or oracledb_statement_config,
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
        )

    async def _create_pool(self) -> "OracleAsyncConnectionPool":
        """Create the actual async connection pool."""
        config = dict(self.pool_config)

        needs_session_callback = self.driver_features.get("enable_numpy_vectors", False) or self.driver_features.get(
            "enable_uuid_binary", False
        )
        if needs_session_callback:
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
        if self.pool_instance:
            await self.pool_instance.close()

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def create_connection(self) -> OracleAsyncConnection:
        """Create a single async connection (not from pool).

        Returns:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return cast("OracleAsyncConnection", await self.pool_instance.acquire())

    @asynccontextmanager
    async def provide_connection(self) -> "AsyncGenerator[OracleAsyncConnection, None]":
        """Provide an async connection context manager.

        Yields:
            An Oracle AsyncConnection instance.
        """
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        conn = await self.pool_instance.acquire()
        try:
            yield conn
        finally:
            await self.pool_instance.release(conn)

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AsyncGenerator[OracleAsyncDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Positional arguments (unused).
            statement_config: Optional statement configuration override.
            **kwargs: Keyword arguments (unused).

        Yields:
            An OracleAsyncDriver instance.
        """
        _ = (args, kwargs)  # Mark as intentionally unused
        async with self.provide_connection() as conn:
            driver = self.driver_type(
                connection=conn,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)

    async def provide_pool(self) -> "OracleAsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.pool_instance:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for OracleDB async types.

        Provides OracleDB async-specific types for Litestar framework recognition.

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
            "OracleSyncConnection": OracleSyncConnection,
            "OracleSyncConnectionPool": OracleSyncConnectionPool,
            "OracleSyncCursor": OracleSyncCursor,
            "OracleSyncDriver": OracleSyncDriver,
            "OracleSyncExceptionHandler": OracleSyncExceptionHandler,
        })
        return namespace
