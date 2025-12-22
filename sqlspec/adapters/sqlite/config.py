"""SQLite database configuration with thread-local connections."""

import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._type_handlers import register_type_handlers
from sqlspec.adapters.sqlite._types import SqliteConnection
from sqlspec.adapters.sqlite.driver import SqliteCursor, SqliteDriver, SqliteExceptionHandler, sqlite_statement_config
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool
from sqlspec.config import ExtensionConfigs, SyncDatabaseConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

logger = get_logger("adapters.sqlite")

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig


class SqliteConnectionParams(TypedDict):
    """SQLite connection parameters."""

    database: NotRequired[str]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: "NotRequired[str | None]"
    check_same_thread: NotRequired[bool]
    factory: "NotRequired[type[SqliteConnection] | None]"
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


class SqliteDriverFeatures(TypedDict):
    """SQLite driver feature configuration.

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


__all__ = ("SqliteConfig", "SqliteConnectionParams", "SqliteDriverFeatures")


class SqliteConfig(SyncDatabaseConfig[SqliteConnection, SqliteConnectionPool, SqliteDriver]):
    """SQLite configuration with thread-local connections."""

    driver_type: "ClassVar[type[SqliteDriver]]" = SqliteDriver
    connection_type: "ClassVar[type[SqliteConnection]]" = SqliteConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        connection_config: "SqliteConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "SqliteConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SqliteDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            connection_config: Configuration parameters including connection settings
            connection_instance: Pre-created pool instance
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional bind key for the configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            observability_config: Adapter-level observability overrides for lifecycle hooks and observers
            **kwargs: Additional keyword arguments passed to the base configuration.
        """
        config_dict: dict[str, Any] = dict(connection_config) if connection_config else {}
        if "database" not in config_dict or config_dict["database"] == ":memory:":
            config_dict["database"] = f"file:memory_{uuid.uuid4().hex}?mode=memory&cache=private"
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

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        processed_driver_features.setdefault("enable_custom_adapters", True)
        json_serializer = processed_driver_features.setdefault("json_serializer", to_json)
        json_deserializer = processed_driver_features.setdefault("json_deserializer", from_json)

        base_statement_config = statement_config or sqlite_statement_config
        if json_serializer is not None:
            parameter_config = base_statement_config.parameter_config.with_json_serializers(
                json_serializer, deserializer=json_deserializer
            )
            base_statement_config = base_statement_config.replace(parameter_config=parameter_config)

        super().__init__(
            bind_key=bind_key,
            connection_instance=connection_instance,
            connection_config=config_dict,
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=processed_driver_features,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""

        excluded_keys = {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        return {k: v for k, v in self.connection_config.items() if v is not None and k not in excluded_keys}

    def _create_pool(self) -> SqliteConnectionPool:
        """Create connection pool from configuration."""
        config_dict = self._get_connection_config_dict()

        pool = SqliteConnectionPool(connection_parameters=config_dict, **self.connection_config)

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

    def _close_pool(self) -> None:
        """Close the connection pool."""
        if self.connection_instance:
            self.connection_instance.close()

    def create_connection(self) -> SqliteConnection:
        """Get a SQLite connection from the pool.

        Returns:
            SqliteConnection: A connection from the pool
        """
        pool = self.provide_pool()
        return pool.acquire()

    @contextmanager
    def provide_connection(self, *args: "Any", **kwargs: "Any") -> "Generator[SqliteConnection, None, None]":
        """Provide a SQLite connection context manager.

        Yields:
            SqliteConnection: A thread-local connection
        """
        pool = self.provide_pool()
        with pool.get_connection() as connection:
            yield connection

    @contextmanager
    def provide_session(
        self, *args: "Any", statement_config: "StatementConfig | None" = None, **kwargs: "Any"
    ) -> "Generator[SqliteDriver, None, None]":
        """Provide a SQLite driver session.

        Yields:
            SqliteDriver: A driver instance with thread-local connection
        """
        with self.provide_connection(*args, **kwargs) as connection:
            driver = self.driver_type(
                connection=connection,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )
            yield self._prepare_driver(driver)

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for SQLite types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "SqliteConnection": SqliteConnection,
            "SqliteConnectionParams": SqliteConnectionParams,
            "SqliteConnectionPool": SqliteConnectionPool,
            "SqliteCursor": SqliteCursor,
            "SqliteDriver": SqliteDriver,
            "SqliteDriverFeatures": SqliteDriverFeatures,
            "SqliteExceptionHandler": SqliteExceptionHandler,
        })
        return namespace
