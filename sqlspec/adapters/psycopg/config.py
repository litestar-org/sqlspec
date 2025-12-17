"""Psycopg database configuration with direct field-based configuration."""

import contextlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypedDict, cast

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from typing_extensions import NotRequired

from sqlspec.adapters.psycopg._type_handlers import register_pgvector_async, register_pgvector_sync
from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.adapters.psycopg.driver import (
    PsycopgAsyncCursor,
    PsycopgAsyncDriver,
    PsycopgAsyncExceptionHandler,
    PsycopgSyncCursor,
    PsycopgSyncDriver,
    PsycopgSyncExceptionHandler,
    build_psycopg_statement_config,
    psycopg_statement_config,
)
from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.config_normalization import apply_pool_deprecations, normalize_connection_config
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Generator

    from sqlspec.core import StatementConfig


class PsycopgConnectionParams(TypedDict):
    """Psycopg connection parameters."""

    conninfo: NotRequired[str]
    host: NotRequired[str]
    port: NotRequired[int]
    user: NotRequired[str]
    password: NotRequired[str]
    dbname: NotRequired[str]
    connect_timeout: NotRequired[int]
    options: NotRequired[str]
    application_name: NotRequired[str]
    sslmode: NotRequired[str]
    sslcert: NotRequired[str]
    sslkey: NotRequired[str]
    sslrootcert: NotRequired[str]
    autocommit: NotRequired[bool]
    extra: NotRequired[dict[str, Any]]


class PsycopgPoolParams(PsycopgConnectionParams):
    """Psycopg pool parameters."""

    min_size: NotRequired[int]
    max_size: NotRequired[int]
    name: NotRequired[str]
    timeout: NotRequired[float]
    max_waiting: NotRequired[int]
    max_lifetime: NotRequired[float]
    max_idle: NotRequired[float]
    reconnect_timeout: NotRequired[float]
    num_workers: NotRequired[int]
    configure: NotRequired["Callable[..., Any]"]
    kwargs: NotRequired[dict[str, Any]]


class PsycopgDriverFeatures(TypedDict):
    """Psycopg driver feature flags.

    enable_pgvector: Enable automatic pgvector extension support for vector similarity search.
        Requires pgvector-python package (pip install pgvector) and PostgreSQL with pgvector extension.
        Defaults to True when pgvector-python is installed.
        Provides automatic conversion between Python objects and PostgreSQL vector types.
        Enables vector similarity operations and index support.
        Set to False to disable pgvector support even when package is available.
    json_serializer: Custom JSON serializer for StatementConfig parameter handling.
    json_deserializer: Custom JSON deserializer reference stored alongside the serializer for parity with asyncpg.
    enable_events: Enable database event channel support.
        Defaults to True when extension_config["events"] is configured.
        Provides pub/sub capabilities via LISTEN/NOTIFY or table-backed fallback.
        Requires extension_config["events"] for migration setup when using table_queue backend.
    events_backend: Event channel backend selection.
        Options: "listen_notify", "table_queue", "listen_notify_durable"
        - "listen_notify": Zero-copy PostgreSQL LISTEN/NOTIFY (ephemeral, real-time) - coming soon
        - "table_queue": Durable table-backed queue with retries and exactly-once delivery (current default)
        - "listen_notify_durable": Hybrid - real-time + durable (available when native support lands)
        Defaults to "table_queue" until native LISTEN/NOTIFY support is implemented.
    """

    enable_pgvector: NotRequired[bool]
    json_serializer: NotRequired["Callable[[Any], str]"]
    json_deserializer: NotRequired["Callable[[str], Any]"]
    enable_events: NotRequired[bool]
    events_backend: NotRequired[Literal["listen_notify", "table_queue", "listen_notify_durable"]]


__all__ = (
    "PsycopgAsyncConfig",
    "PsycopgAsyncCursor",
    "PsycopgConnectionParams",
    "PsycopgDriverFeatures",
    "PsycopgPoolParams",
    "PsycopgSyncConfig",
    "PsycopgSyncCursor",
)


class PsycopgSyncConfig(SyncDatabaseConfig[PsycopgSyncConnection, ConnectionPool, PsycopgSyncDriver]):
    """Configuration for Psycopg synchronous database connections with direct field-based configuration."""

    driver_type: "ClassVar[type[PsycopgSyncDriver]]" = PsycopgSyncDriver
    connection_type: "ClassVar[type[PsycopgSyncConnection]]" = PsycopgSyncConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        connection_config: "PsycopgPoolParams | dict[str, Any] | None" = None,
        connection_instance: "ConnectionPool | None" = None,
        migration_config: dict[str, Any] | None = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Psycopg synchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            **kwargs: Additional keyword arguments (handles deprecated pool_config/pool_instance)
        """
        connection_config, connection_instance = apply_pool_deprecations(
            kwargs=kwargs, connection_config=connection_config, connection_instance=connection_instance
        )

        processed_connection_config = normalize_connection_config(connection_config)

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        serializer = cast("Callable[[Any], str]", processed_driver_features.get("json_serializer", to_json))
        processed_driver_features.setdefault("json_serializer", serializer)
        processed_driver_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

        # Auto-detect events support based on extension_config
        processed_driver_features.setdefault("enable_events", "events" in (extension_config or {}))
        processed_driver_features.setdefault("events_backend", "listen_notify")

        super().__init__(
            connection_config=processed_connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or build_psycopg_statement_config(json_serializer=serializer),
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

    def _create_pool(self) -> "ConnectionPool":
        """Create the actual connection pool."""
        all_config = dict(self.connection_config)

        pool_parameters = {
            "min_size": all_config.pop("min_size", 4),
            "max_size": all_config.pop("max_size", None),
            "name": all_config.pop("name", None),
            "timeout": all_config.pop("timeout", 30.0),
            "max_waiting": all_config.pop("max_waiting", 0),
            "max_lifetime": all_config.pop("max_lifetime", 3600.0),
            "max_idle": all_config.pop("max_idle", 600.0),
            "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
            "num_workers": all_config.pop("num_workers", 3),
        }

        autocommit_setting = all_config.get("autocommit")

        def configure_connection(conn: "PsycopgSyncConnection") -> None:
            conn.row_factory = dict_row
            if autocommit_setting is not None:
                conn.autocommit = autocommit_setting

            if self.driver_features.get("enable_pgvector", False):
                register_pgvector_sync(conn)

        pool_parameters["configure"] = all_config.pop("configure", configure_connection)

        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        if conninfo:
            return ConnectionPool(conninfo, open=True, **pool_parameters)

        kwargs = all_config.pop("kwargs", {})
        all_config.update(kwargs)
        return ConnectionPool("", kwargs=all_config, open=True, **pool_parameters)

    def _close_pool(self) -> None:
        """Close the actual connection pool."""
        if not self.connection_instance:
            return

        try:
            self.connection_instance.close()
        finally:
            self.connection_instance = None

    def create_connection(self) -> "PsycopgSyncConnection":
        """Create a single connection (not from pool).

        Returns:
            A psycopg Connection instance configured with DictRow.
        """
        if self.connection_instance is None:
            self.connection_instance = self.create_pool()
        return cast("PsycopgSyncConnection", self.connection_instance.getconn())  # pyright: ignore

    @contextlib.contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> "Generator[PsycopgSyncConnection, None, None]":
        """Provide a connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psycopg Connection instance.
        """
        if self.connection_instance:
            with self.connection_instance.connection() as conn:
                yield conn  # type: ignore[misc]
        else:
            conn = self.create_connection()  # type: ignore[assignment]
            try:
                yield conn  # type: ignore[misc]
            finally:
                conn.close()

    @contextlib.contextmanager
    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "Generator[PsycopgSyncDriver, None, None]":
        """Provide a driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgSyncDriver instance.
        """
        with self.provide_connection(*args, **kwargs) as conn:
            final_statement_config = statement_config or self.statement_config
            driver = self.driver_type(
                connection=conn, statement_config=final_statement_config, driver_features=self.driver_features
            )
            yield self._prepare_driver(driver)

    def provide_pool(self, *args: Any, **kwargs: Any) -> "ConnectionPool":
        """Provide pool instance.

        Returns:
            The connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Psycopg types.

        This provides all Psycopg-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "PsycopgConnectionParams": PsycopgConnectionParams,
            "PsycopgPoolParams": PsycopgPoolParams,
            "PsycopgSyncConnection": PsycopgSyncConnection,
            "PsycopgSyncCursor": PsycopgSyncCursor,
            "PsycopgSyncDriver": PsycopgSyncDriver,
            "PsycopgSyncExceptionHandler": PsycopgSyncExceptionHandler,
        })
        return namespace


class PsycopgAsyncConfig(AsyncDatabaseConfig[PsycopgAsyncConnection, AsyncConnectionPool, PsycopgAsyncDriver]):
    """Configuration for Psycopg asynchronous database connections with direct field-based configuration."""

    driver_type: ClassVar[type[PsycopgAsyncDriver]] = PsycopgAsyncDriver
    connection_type: "ClassVar[type[PsycopgAsyncConnection]]" = PsycopgAsyncConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        *,
        connection_config: "PsycopgPoolParams | dict[str, Any] | None" = None,
        connection_instance: "AsyncConnectionPool | None" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Psycopg asynchronous configuration.

        Args:
            connection_config: Connection and pool configuration parameters (TypedDict or dict)
            connection_instance: Existing pool instance to use
            migration_config: Migration configuration
            statement_config: Default SQL statement configuration
            driver_features: Optional driver feature configuration
            bind_key: Optional unique identifier for this configuration
            extension_config: Extension-specific configuration (e.g., Litestar plugin settings)
            **kwargs: Additional keyword arguments (handles deprecated pool_config/pool_instance)
        """
        connection_config, connection_instance = apply_pool_deprecations(
            kwargs=kwargs, connection_config=connection_config, connection_instance=connection_instance
        )

        processed_connection_config = normalize_connection_config(connection_config)

        processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
        serializer = cast("Callable[[Any], str]", processed_driver_features.get("json_serializer", to_json))
        processed_driver_features.setdefault("json_serializer", serializer)
        processed_driver_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

        # Auto-detect events support based on extension_config
        processed_driver_features.setdefault("enable_events", "events" in (extension_config or {}))
        processed_driver_features.setdefault("events_backend", "listen_notify")

        super().__init__(
            connection_config=processed_connection_config,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config or build_psycopg_statement_config(json_serializer=serializer),
            driver_features=processed_driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            **kwargs,
        )

    async def _create_pool(self) -> "AsyncConnectionPool":
        """Create the actual async connection pool."""

        all_config = dict(self.connection_config)

        pool_parameters = {
            "min_size": all_config.pop("min_size", 4),
            "max_size": all_config.pop("max_size", None),
            "name": all_config.pop("name", None),
            "timeout": all_config.pop("timeout", 30.0),
            "max_waiting": all_config.pop("max_waiting", 0),
            "max_lifetime": all_config.pop("max_lifetime", 3600.0),
            "max_idle": all_config.pop("max_idle", 600.0),
            "reconnect_timeout": all_config.pop("reconnect_timeout", 300.0),
            "num_workers": all_config.pop("num_workers", 3),
        }

        autocommit_setting = all_config.get("autocommit")

        async def configure_connection(conn: "PsycopgAsyncConnection") -> None:
            conn.row_factory = dict_row
            if autocommit_setting is not None:
                await conn.set_autocommit(autocommit_setting)

                if self.driver_features.get("enable_pgvector", False):
                    await register_pgvector_async(conn)

        pool_parameters["configure"] = all_config.pop("configure", configure_connection)

        pool_parameters = {k: v for k, v in pool_parameters.items() if v is not None}

        conninfo = all_config.pop("conninfo", None)
        if conninfo:
            pool = AsyncConnectionPool(conninfo, open=False, **pool_parameters)
        else:
            kwargs = all_config.pop("kwargs", {})
            all_config.update(kwargs)
            pool = AsyncConnectionPool("", kwargs=all_config, open=False, **pool_parameters)

        await pool.open()

        return pool

    async def _close_pool(self) -> None:
        """Close the actual async connection pool."""
        if not self.connection_instance:
            return

        try:
            await self.connection_instance.close()
        finally:
            self.connection_instance = None

    async def create_connection(self) -> "PsycopgAsyncConnection":  # pyright: ignore
        """Create a single async connection (not from pool).

        Returns:
            A psycopg AsyncConnection instance configured with DictRow.
        """
        if self.connection_instance is None:
            self.connection_instance = await self.create_pool()
        return cast("PsycopgAsyncConnection", await self.connection_instance.getconn())  # pyright: ignore

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> "AsyncGenerator[PsycopgAsyncConnection, None]":  # pyright: ignore
        """Provide an async connection context manager.

        Args:
            *args: Additional arguments.
            **kwargs: Additional keyword arguments.

        Yields:
            A psycopg AsyncConnection instance.
        """
        if self.connection_instance:
            async with self.connection_instance.connection() as conn:
                yield conn  # type: ignore[misc]
        else:
            conn = await self.create_connection()  # type: ignore[assignment]
            try:
                yield conn  # type: ignore[misc]
            finally:
                await conn.close()

    @asynccontextmanager
    async def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AsyncGenerator[PsycopgAsyncDriver, None]":
        """Provide an async driver session context manager.

        Args:
            *args: Additional arguments.
            statement_config: Optional statement configuration override.
            **kwargs: Additional keyword arguments.

        Yields:
            A PsycopgAsyncDriver instance.
        """
        async with self.provide_connection(*args, **kwargs) as conn:
            final_statement_config = statement_config or psycopg_statement_config
            driver = self.driver_type(
                connection=conn, statement_config=final_statement_config, driver_features=self.driver_features
            )
            yield self._prepare_driver(driver)

    async def provide_pool(self, *args: Any, **kwargs: Any) -> "AsyncConnectionPool":
        """Provide async pool instance.

        Returns:
            The async connection pool.
        """
        if not self.connection_instance:
            self.connection_instance = await self.create_pool()
        return self.connection_instance

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Psycopg async types.

        This provides all Psycopg async-specific types that Litestar needs to recognize
        to avoid serialization attempts.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "PsycopgAsyncConnection": PsycopgAsyncConnection,
            "PsycopgAsyncCursor": PsycopgAsyncCursor,
            "PsycopgAsyncDriver": PsycopgAsyncDriver,
            "PsycopgAsyncExceptionHandler": PsycopgAsyncExceptionHandler,
            "PsycopgConnectionParams": PsycopgConnectionParams,
            "PsycopgPoolParams": PsycopgPoolParams,
        })
        return namespace
