"""SQLite database configuration with thread-local connections."""

import logging
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.sqlite._type_handlers import register_type_handlers
from sqlspec.adapters.sqlite._types import SqliteConnection
from sqlspec.adapters.sqlite.driver import SqliteCursor, SqliteDriver, sqlite_statement_config
from sqlspec.adapters.sqlite.pool import SqliteConnectionPool
from sqlspec.config import ADKConfig, FastAPIConfig, FlaskConfig, LitestarConfig, StarletteConfig, SyncDatabaseConfig
from sqlspec.utils.serializers import from_json, to_json

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from sqlspec.core import StatementConfig


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
    supports_native_parquet_export: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        pool_config: "SqliteConnectionParams | dict[str, Any] | None" = None,
        pool_instance: "SqliteConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "SqliteDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | LitestarConfig | FastAPIConfig | StarletteConfig | FlaskConfig | ADKConfig | None" = None,
    ) -> None:
        """Initialize SQLite configuration.

        Args:
            pool_config: Configuration parameters including connection settings
            pool_instance: Pre-created pool instance
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional bind key for the configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
        """
        if pool_config is None:
            pool_config = {}
        if "database" not in pool_config or pool_config["database"] == ":memory:":
            pool_config["database"] = f"file:memory_{uuid.uuid4().hex}?mode=memory&cache=private"
            pool_config["uri"] = True
        elif "database" in pool_config:
            database_path = str(pool_config["database"])
            if database_path.startswith("file:") and not pool_config.get("uri"):
                logger.debug(
                    "Database URI detected (%s) but uri=True not set. "
                    "Auto-enabling URI mode to prevent physical file creation.",
                    database_path,
                )
                pool_config["uri"] = True

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}

        if "enable_custom_adapters" not in processed_driver_features:
            processed_driver_features["enable_custom_adapters"] = True

        if "json_serializer" not in processed_driver_features:
            processed_driver_features["json_serializer"] = to_json

        if "json_deserializer" not in processed_driver_features:
            processed_driver_features["json_deserializer"] = from_json

        base_statement_config = statement_config or sqlite_statement_config

        json_serializer = processed_driver_features.get("json_serializer")
        json_deserializer = processed_driver_features.get("json_deserializer")
        if json_serializer is not None:
            parameter_config = base_statement_config.parameter_config.with_json_serializers(
                json_serializer, deserializer=json_deserializer
            )
            base_statement_config = base_statement_config.replace(parameter_config=parameter_config)

        super().__init__(
            bind_key=bind_key,
            pool_instance=pool_instance,
            pool_config=cast("dict[str, Any]", pool_config),
            migration_config=migration_config,
            statement_config=base_statement_config,
            driver_features=processed_driver_features,
            extension_config=extension_config,
        )

    def _get_connection_config_dict(self) -> "dict[str, Any]":
        """Get connection configuration as plain dict for pool creation."""

        excluded_keys = {"pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "extra"}
        return {k: v for k, v in self.pool_config.items() if v is not None and k not in excluded_keys}

    def _create_pool(self) -> SqliteConnectionPool:
        """Create connection pool from configuration."""
        config_dict = self._get_connection_config_dict()

        pool = SqliteConnectionPool(connection_parameters=config_dict, **self.pool_config)

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
        if self.pool_instance:
            self.pool_instance.close()

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
            yield self.driver_type(
                connection=connection,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
            )

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for SQLite types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({"SqliteConnection": SqliteConnection, "SqliteCursor": SqliteCursor})
        return namespace
