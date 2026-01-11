"""Aiosqlite database configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from mypy_extensions import mypyc_attr
from typing_extensions import NotRequired

from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection
from sqlspec.adapters.aiosqlite.core import apply_driver_features, build_connection_config, default_statement_config
from sqlspec.adapters.aiosqlite.driver import (
    AiosqliteCursor,
    AiosqliteDriver,
    AiosqliteExceptionHandler,
    AiosqliteSessionContext,
)
from sqlspec.adapters.aiosqlite.pool import (
    AiosqliteConnectionPool,
    AiosqlitePoolConnection,
    AiosqlitePoolConnectionContext,
)
from sqlspec.adapters.sqlite.type_converter import register_type_handlers
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("AiosqliteConfig", "AiosqliteConnectionParams", "AiosqliteDriverFeatures", "AiosqlitePoolParams")

logger = get_logger("sqlspec.adapters.aiosqlite")


class AiosqliteConnectionParams(TypedDict):
    """TypedDict for aiosqlite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[str | None]
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


class AiosqlitePoolParams(AiosqliteConnectionParams):
    """TypedDict for aiosqlite pool parameters, inheriting connection parameters."""

    pool_size: NotRequired[int]
    connect_timeout: NotRequired[float]
    idle_timeout: NotRequired[float]
    operation_timeout: NotRequired[float]
    extra: NotRequired["dict[str, Any]"]


class AiosqliteDriverFeatures(TypedDict):
    """Aiosqlite driver feature configuration.

    Controls optional type handling and serialization features for SQLite connections.

    enable_custom_adapters: Enable custom type adapters for JSON/UUID/datetime conversion.
        Defaults to True for enhanced Python type support.
        Set to False only if you need pure SQLite behavior without type conversions.
    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
        Provides pub/sub capabilities via table-backed queue (SQLite has no native pub/sub).
        Requires extension_config["events"] for migration setup.
    events_backend: Event channel backend selection.
        Only option: "table_queue" (durable table-backed queue with retries and exactly-once delivery).
        SQLite does not have native pub/sub, so table_queue is the only backend.
        Defaults to "table_queue".
    """

    enable_custom_adapters: NotRequired[bool]
    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"
    enable_events: NotRequired[bool]
    events_backend: NotRequired[str]


class _AiosqliteSessionFactory:
    __slots__ = ("_config", "_pool_conn")

    def __init__(self, config: "AiosqliteConfig") -> None:
        self._config = config
        self._pool_conn: AiosqlitePoolConnection | None = None

    async def acquire_connection(self) -> "AiosqliteConnection":
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        pool_conn = await pool.acquire()
        self._pool_conn = pool_conn
        return pool_conn.connection

    async def release_connection(self, _conn: "AiosqliteConnection") -> None:
        if self._pool_conn is not None and self._config.connection_instance is not None:
            await self._config.connection_instance.release(self._pool_conn)
            self._pool_conn = None


class AiosqliteConnectionContext:
    """Async context manager for AioSQLite connections."""

    __slots__ = ("_config", "_ctx")

    def __init__(self, config: "AiosqliteConfig") -> None:
        self._config = config
        self._ctx: AiosqlitePoolConnectionContext | None = None

    async def __aenter__(self) -> AiosqliteConnection:
        pool = self._config.connection_instance
        if pool is None:
            pool = await self._config.create_pool()
            self._config.connection_instance = pool
        self._ctx = pool.get_connection()
        return await self._ctx.__aenter__()

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> bool | None:
        if self._ctx:
            return await self._ctx.__aexit__(exc_type, exc_val, exc_tb)
        return None


@mypyc_attr(native_class=False)
class AiosqliteConfig(AsyncDatabaseConfig["AiosqliteConnection", AiosqliteConnectionPool, AiosqliteDriver]):
    """Database configuration for AioSQLite engine."""

    driver_type: "ClassVar[type[AiosqliteDriver]]" = AiosqliteDriver
    connection_type: "ClassVar[type[AiosqliteConnection]]" = AiosqliteConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        connection_config: "AiosqlitePoolParams | dict[str, Any] | None" = None,
        connection_instance: "AiosqliteConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "AiosqliteDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize AioSQLite configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Optional pre-configured connection pool instance.
            migration_config: Optional migration configuration.
            statement_config: Optional statement configuration.
            driver_features: Optional driver feature configuration.
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.

        """
        config_dict: dict[str, Any] = dict(connection_config) if connection_config else {}

        if "database" not in config_dict or config_dict["database"] == ":memory:":
            config_dict["database"] = "file::memory:?cache=shared"
            config_dict["uri"] = True
        elif "database" in config_dict:
            database_path = str(config_dict["database"])
            if database_path.startswith("file:") and not config_dict.get("uri"):
                logger.debug(
                    "Database URI detected (%s) but uri=True not set. "
                    "Auto-enabling URI mode to prevent physical file creation.",
                    database_path,
                )
                config_dict["uri"] = True

        config_dict = normalize_connection_config(config_dict)

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=config_dict,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AiosqliteConnectionContext":
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Returns:
            An aiosqlite connection context manager.

        """
        return AiosqliteConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "AiosqliteSessionContext":
        """Provide an async driver session context manager.

        Args:
            *_args: Additional arguments.
            statement_config: Optional statement configuration override.
            **_kwargs: Additional keyword arguments.

        Returns:
            An AiosqliteDriver session context manager.

        """
        factory = _AiosqliteSessionFactory(self)
        return AiosqliteSessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )

    async def _create_pool(self) -> AiosqliteConnectionPool:
        """Create the connection pool instance.

        Returns:
            AiosqliteConnectionPool: The connection pool instance.

        """
        config = {k: v for k, v in self.connection_config.items() if v is not None}
        pool_size = config.pop("pool_size", 5)
        connect_timeout = config.pop("connect_timeout", 30.0)
        idle_timeout = config.pop("idle_timeout", 24 * 60 * 60)
        operation_timeout = config.pop("operation_timeout", 10.0)

        pool = AiosqliteConnectionPool(
            connection_parameters=build_connection_config(self.connection_config),
            pool_size=pool_size,
            connect_timeout=connect_timeout,
            idle_timeout=idle_timeout,
            operation_timeout=operation_timeout,
        )

        if self.driver_features.get("enable_custom_adapters", False):
            self._register_type_adapters()

        return pool

    def _register_type_adapters(self) -> None:
        """Register custom type adapters and converters for SQLite.

        Called once during pool creation if enable_custom_adapters is True.
        Registers JSON serialization handlers if configured.
        """
        if self.driver_features.get("enable_custom_adapters", False):
            register_type_handlers(
                json_serializer=self.driver_features.get("json_serializer"),
                json_deserializer=self.driver_features.get("json_deserializer"),
            )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for AiosqliteConfig types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "AiosqliteConnectionContext": AiosqliteConnectionContext,
            "AiosqliteConnection": AiosqliteConnection,
            "AiosqliteConnectionParams": AiosqliteConnectionParams,
            "AiosqliteConnectionPool": AiosqliteConnectionPool,
            "AiosqliteCursor": AiosqliteCursor,
            "AiosqliteDriver": AiosqliteDriver,
            "AiosqliteDriverFeatures": AiosqliteDriverFeatures,
            "AiosqliteExceptionHandler": AiosqliteExceptionHandler,
            "AiosqlitePoolParams": AiosqlitePoolParams,
            "AiosqliteSessionContext": AiosqliteSessionContext,
        })
        return namespace

    async def close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance and not self.connection_instance.is_closed:
            await self.connection_instance.close()
            self.connection_instance = None

    async def create_connection(self) -> "AiosqliteConnection":
        """Create a single async connection from the pool.

        Returns:
            An aiosqlite connection instance.

        """
        pool = self.connection_instance
        if pool is None:
            pool = await self.create_pool()
            self.connection_instance = pool
        pool_connection = await pool.acquire()
        return pool_connection.connection

    async def provide_pool(self) -> AiosqliteConnectionPool:
        """Provide async pool instance.

        Returns:
            The async connection pool.

        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    async def _close_pool(self) -> None:
        """Close the connection pool."""
        await self.close_pool()
