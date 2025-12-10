import asyncio
import atexit
from collections.abc import AsyncIterator, Awaitable, Coroutine, Iterator
from contextlib import AbstractAsyncContextManager, AbstractContextManager, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, TypeGuard, cast, overload

from sqlspec.config import (
    AsyncConfigT,
    AsyncDatabaseConfig,
    DatabaseConfigProtocol,
    DriverT,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    SyncConfigT,
    SyncDatabaseConfig,
)
from sqlspec.core import (
    CacheConfig,
    get_cache_config,
    get_cache_statistics,
    log_cache_stats,
    reset_cache_stats,
    update_cache_config,
)
from sqlspec.loader import SQLFileLoader
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime, TelemetryDiagnostics
from sqlspec.typing import ConnectionT
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from sqlspec.core import SQL
    from sqlspec.typing import PoolT


__all__ = ("SQLSpec",)

logger = get_logger()


def _is_async_context_manager(obj: Any) -> TypeGuard[AbstractAsyncContextManager[Any]]:
    return hasattr(obj, "__aenter__")


def _is_sync_context_manager(obj: Any) -> TypeGuard[AbstractContextManager[Any]]:
    return hasattr(obj, "__enter__")


class SQLSpec:
    """Configuration manager and registry for database connections and pools."""

    __slots__ = ("_configs", "_instance_cache_config", "_loader_runtime", "_observability_config", "_sql_loader")

    def __init__(
        self, *, loader: "SQLFileLoader | None" = None, observability_config: "ObservabilityConfig | None" = None
    ) -> None:
        self._configs: dict[int, DatabaseConfigProtocol[Any, Any, Any]] = {}
        atexit.register(self._cleanup_sync_pools)
        self._instance_cache_config: CacheConfig | None = None
        self._sql_loader: SQLFileLoader | None = loader
        self._observability_config = observability_config
        self._loader_runtime = ObservabilityRuntime(observability_config, config_name="SQLFileLoader")
        if self._sql_loader is not None:
            self._sql_loader.set_observability_runtime(self._loader_runtime)

    @staticmethod
    def _get_config_name(obj: Any) -> str:
        """Get display name for configuration object."""
        return getattr(obj, "__name__", str(obj))

    def _cleanup_sync_pools(self) -> None:
        """Clean up only synchronous connection pools at exit."""
        cleaned_count = 0

        for config in self._configs.values():
            if config.supports_connection_pooling and not config.is_async:
                try:
                    config.close_pool()
                    cleaned_count += 1
                except Exception as e:
                    logger.debug("Failed to clean up sync pool for config %s: %s", config.__class__.__name__, e)

        if cleaned_count > 0:
            logger.debug("Sync pool cleanup completed. Cleaned %d pools.", cleaned_count)

    async def close_all_pools(self) -> None:
        """Explicitly close all connection pools (async and sync).

        This method should be called before application shutdown for proper cleanup.
        """
        cleanup_tasks = []
        sync_configs = []

        for config in self._configs.values():
            if config.supports_connection_pooling:
                try:
                    if config.is_async:
                        close_pool_awaitable = config.close_pool()
                        if close_pool_awaitable is not None:
                            cleanup_tasks.append(cast("Coroutine[Any, Any, None]", close_pool_awaitable))
                    else:
                        sync_configs.append(config)
                except Exception as e:
                    logger.debug("Failed to prepare cleanup for config %s: %s", config.__class__.__name__, e)

        if cleanup_tasks:
            try:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                logger.debug("Async pool cleanup completed. Cleaned %d pools.", len(cleanup_tasks))
            except Exception as e:
                logger.debug("Failed to complete async pool cleanup: %s", e)

        for config in sync_configs:
            config.close_pool()

        if sync_configs:
            logger.debug("Sync pool cleanup completed. Cleaned %d pools.", len(sync_configs))

    async def __aenter__(self) -> "SQLSpec":
        """Async context manager entry."""
        return self

    async def __aexit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:
        """Async context manager exit with automatic cleanup."""
        await self.close_all_pools()

    @overload
    def add_config(self, config: "SyncConfigT") -> "SyncConfigT": ...

    @overload
    def add_config(self, config: "AsyncConfigT") -> "AsyncConfigT": ...

    def add_config(self, config: "SyncConfigT | AsyncConfigT") -> "SyncConfigT | AsyncConfigT":
        """Add a configuration instance to the registry.

        Args:
            config: The configuration instance to add.

        Returns:
            The same configuration instance (it IS the handle).
        """
        config_id = id(config)
        if config_id in self._configs:
            logger.debug("Configuration for %s already exists. Overwriting.", config.__class__.__name__)
        if hasattr(config, "attach_observability"):
            config.attach_observability(self._observability_config)
        self._configs[config_id] = config
        return config

    @property
    def configs(self) -> "dict[int, DatabaseConfigProtocol[Any, Any, Any]]":
        """Access the registry of database configurations.

        Returns:
            Dictionary mapping config instance IDs to config instances.
        """
        return self._configs

    def telemetry_snapshot(self) -> "dict[str, Any]":
        """Return aggregated diagnostics across all registered configurations."""

        diagnostics = TelemetryDiagnostics()
        loader_metrics = self._loader_runtime.metrics_snapshot()
        if loader_metrics:
            diagnostics.add_metric_snapshot(loader_metrics)
        for config in self._configs.values():
            runtime = config.get_observability_runtime()
            diagnostics.add_lifecycle_snapshot(runtime.diagnostics_key, runtime.lifecycle_snapshot())
            metrics_snapshot = runtime.metrics_snapshot()
            if metrics_snapshot:
                diagnostics.add_metric_snapshot(metrics_snapshot)
        return diagnostics.snapshot()

    def _ensure_sql_loader(self) -> SQLFileLoader:
        """Return a SQLFileLoader instance configured with observability runtime."""

        if self._sql_loader is None:
            self._sql_loader = SQLFileLoader(runtime=self._loader_runtime)
        else:
            self._sql_loader.set_observability_runtime(self._loader_runtime)
        return self._sql_loader

    @overload
    def get_connection(
        self, config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> "ConnectionT": ...

    @overload
    def get_connection(
        self, config: "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> "Awaitable[ConnectionT]": ...

    def get_connection(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
    ) -> "ConnectionT | Awaitable[ConnectionT]":
        """Get a database connection for the specified configuration.

        Args:
            config: The configuration instance.

        Returns:
            A database connection or an awaitable yielding a connection.
        """
        if id(config) not in self._configs:
            self.add_config(config)

        config_name = config.__class__.__name__
        logger.debug("Getting connection for config: %s", config_name, extra={"config_type": config_name})
        return config.create_connection()

    @overload
    def get_session(
        self, config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> "DriverT": ...

    @overload
    def get_session(
        self, config: "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> "Awaitable[DriverT]": ...

    def get_session(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
    ) -> "DriverT | Awaitable[DriverT]":
        """Get a database session (driver adapter) for the specified configuration.

        Args:
            config: The configuration instance.

        Returns:
            A driver adapter instance or an awaitable yielding one.
        """
        if id(config) not in self._configs:
            self.add_config(config)

        config_name = config.__class__.__name__
        logger.debug("Getting session for config: %s", config_name, extra={"config_type": config_name})

        connection_obj = self.get_connection(config)

        if isinstance(connection_obj, Awaitable):

            async def _create_driver_async() -> "DriverT":
                resolved_connection = await connection_obj  # pyright: ignore
                driver = config.driver_type(  # pyright: ignore
                    connection=resolved_connection,
                    statement_config=config.statement_config,
                    driver_features=config.driver_features,
                )
                return config._prepare_driver(driver)  # pyright: ignore

            return _create_driver_async()

        driver = config.driver_type(  # pyright: ignore
            connection=connection_obj, statement_config=config.statement_config, driver_features=config.driver_features
        )
        return config._prepare_driver(driver)  # pyright: ignore

    @overload
    def provide_connection(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractContextManager[ConnectionT]": ...

    @overload
    def provide_connection(
        self,
        config: "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[ConnectionT]": ...

    def provide_connection(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractContextManager[ConnectionT] | AbstractAsyncContextManager[ConnectionT]":
        """Create and provide a database connection from the specified configuration.

        Args:
            config: The configuration instance.
            *args: Positional arguments to pass to the config's provide_connection.
            **kwargs: Keyword arguments to pass to the config's provide_connection.

        Returns:
            A sync or async context manager yielding a connection.
        """
        if id(config) not in self._configs:
            self.add_config(config)

        config_name = config.__class__.__name__
        logger.debug("Providing connection context for config: %s", config_name, extra={"config_type": config_name})
        connection_context = config.provide_connection(*args, **kwargs)
        runtime = config.get_observability_runtime()

        if _is_async_context_manager(connection_context):
            async_context = cast("AbstractAsyncContextManager[ConnectionT]", connection_context)

            @asynccontextmanager
            async def _async_wrapper() -> AsyncIterator[ConnectionT]:
                connection: ConnectionT | None = None
                try:
                    async with async_context as conn:
                        connection = conn
                        runtime.emit_connection_create(conn)
                        yield conn
                finally:
                    if connection is not None:
                        runtime.emit_connection_destroy(connection)

            return _async_wrapper()

        sync_context = cast("AbstractContextManager[ConnectionT]", connection_context)

        @contextmanager
        def _sync_wrapper() -> Iterator[ConnectionT]:
            connection: ConnectionT | None = None
            try:
                with sync_context as conn:
                    connection = conn
                    runtime.emit_connection_create(conn)
                    yield conn
            finally:
                if connection is not None:
                    runtime.emit_connection_destroy(connection)

        return _sync_wrapper()

    @overload
    def provide_session(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT]": ...

    @overload
    def provide_session(
        self,
        config: "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[DriverT]": ...

    def provide_session(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT] | AbstractAsyncContextManager[DriverT]":
        """Create and provide a database session from the specified configuration.

        Args:
            config: The configuration instance.
            *args: Positional arguments to pass to the config's provide_session.
            **kwargs: Keyword arguments to pass to the config's provide_session.

        Returns:
            A sync or async context manager yielding a driver adapter instance.
        """
        if id(config) not in self._configs:
            self.add_config(config)

        config_name = config.__class__.__name__
        logger.debug("Providing session context for config: %s", config_name, extra={"config_type": config_name})
        session_context = config.provide_session(*args, **kwargs)
        runtime = config.get_observability_runtime()

        if _is_async_context_manager(session_context):
            async_session = cast("AbstractAsyncContextManager[DriverT]", session_context)

            @asynccontextmanager
            async def _async_session_wrapper() -> AsyncIterator[DriverT]:
                driver: DriverT | None = None
                try:
                    async with async_session as session:
                        driver = config._prepare_driver(session)  # pyright: ignore
                        connection = getattr(driver, "connection", None)
                        if connection is not None:
                            runtime.emit_connection_create(connection)
                        runtime.emit_session_start(driver)
                        yield driver
                finally:
                    if driver is not None:
                        runtime.emit_session_end(driver)
                        connection = getattr(driver, "connection", None)
                        if connection is not None:
                            runtime.emit_connection_destroy(connection)

            return _async_session_wrapper()

        sync_session = cast("AbstractContextManager[DriverT]", session_context)

        @contextmanager
        def _sync_session_wrapper() -> Iterator[DriverT]:
            driver: DriverT | None = None
            try:
                with sync_session as session:
                    driver = config._prepare_driver(session)  # pyright: ignore
                    connection = getattr(driver, "connection", None)
                    if connection is not None:
                        runtime.emit_connection_create(connection)
                    runtime.emit_session_start(driver)
                    yield driver
            finally:
                if driver is not None:
                    runtime.emit_session_end(driver)
                    connection = getattr(driver, "connection", None)
                    if connection is not None:
                        runtime.emit_connection_destroy(connection)

        return _sync_session_wrapper()

    @overload
    def get_pool(
        self, config: "NoPoolSyncConfig[ConnectionT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT]"
    ) -> "None": ...
    @overload
    def get_pool(self, config: "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]") -> "type[PoolT]": ...
    @overload
    def get_pool(self, config: "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]") -> "Awaitable[type[PoolT]]": ...

    def get_pool(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
    ) -> "type[PoolT] | Awaitable[type[PoolT]] | None":
        """Get the connection pool for the specified configuration.

        Args:
            config: The configuration instance.

        Returns:
            The connection pool, an awaitable yielding the pool, or None if not supported.
        """
        if id(config) not in self._configs:
            self.add_config(config)

        config_name = config.__class__.__name__

        if config.supports_connection_pooling:
            logger.debug("Getting pool for config: %s", config_name, extra={"config_type": config_name})
            return cast("type[PoolT] | Awaitable[type[PoolT]]", config.create_pool())

        logger.debug("Config %s does not support connection pooling", config_name)
        return None

    @overload
    def close_pool(
        self, config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> "None": ...

    @overload
    def close_pool(
        self, config: "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> "Awaitable[None]": ...

    def close_pool(
        self,
        config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
    ) -> "Awaitable[None] | None":
        """Close the connection pool for the specified configuration.

        Args:
            config: The configuration instance.

        Returns:
            None, or an awaitable if closing an async pool.
        """
        if id(config) not in self._configs:
            self.add_config(config)

        config_name = config.__class__.__name__

        if config.supports_connection_pooling:
            logger.debug("Closing pool for config: %s", config_name, extra={"config_type": config_name})
            return config.close_pool()

        logger.debug("Config %s does not support connection pooling - nothing to close", config_name)
        return None

    @staticmethod
    def get_cache_config() -> CacheConfig:
        """Get the current global cache configuration.

        Returns:
            The current cache configuration.
        """
        return get_cache_config()

    @staticmethod
    def update_cache_config(config: CacheConfig) -> None:
        """Update the global cache configuration.

        Args:
            config: The new cache configuration to apply.
        """
        update_cache_config(config)

    @staticmethod
    def get_cache_stats() -> "dict[str, Any]":
        """Get current cache statistics.

        Returns:
            Cache statistics object with detailed metrics.
        """
        return get_cache_statistics()

    @staticmethod
    def reset_cache_stats() -> None:
        """Reset all cache statistics to zero."""
        reset_cache_stats()

    @staticmethod
    def log_cache_stats() -> None:
        """Log current cache statistics using the configured logger."""
        log_cache_stats()

    @staticmethod
    def configure_cache(
        *,
        sql_cache_size: int | None = None,
        fragment_cache_size: int | None = None,
        optimized_cache_size: int | None = None,
        sql_cache_enabled: bool | None = None,
        fragment_cache_enabled: bool | None = None,
        optimized_cache_enabled: bool | None = None,
    ) -> None:
        """Update cache configuration with partial values.

        Args:
            sql_cache_size: Size of the SQL statement cache.
            fragment_cache_size: Size of the AST fragment cache.
            optimized_cache_size: Size of the optimized expression cache.
            sql_cache_enabled: Enable/disable SQL cache.
            fragment_cache_enabled: Enable/disable fragment cache.
            optimized_cache_enabled: Enable/disable optimized cache.
        """
        current_config = get_cache_config()
        update_cache_config(
            CacheConfig(
                sql_cache_size=sql_cache_size if sql_cache_size is not None else current_config.sql_cache_size,
                fragment_cache_size=fragment_cache_size
                if fragment_cache_size is not None
                else current_config.fragment_cache_size,
                optimized_cache_size=optimized_cache_size
                if optimized_cache_size is not None
                else current_config.optimized_cache_size,
                sql_cache_enabled=sql_cache_enabled
                if sql_cache_enabled is not None
                else current_config.sql_cache_enabled,
                fragment_cache_enabled=fragment_cache_enabled
                if fragment_cache_enabled is not None
                else current_config.fragment_cache_enabled,
                optimized_cache_enabled=optimized_cache_enabled
                if optimized_cache_enabled is not None
                else current_config.optimized_cache_enabled,
            )
        )

    def load_sql_files(self, *paths: "str | Path") -> None:
        """Load SQL files from paths or directories.

        Args:
            *paths: One or more file paths or directory paths to load.
        """
        loader = self._ensure_sql_loader()
        loader.load_sql(*paths)
        logger.debug("Loaded SQL files: %s", paths)

    def add_named_sql(self, name: str, sql: str, dialect: "str | None" = None) -> None:
        """Add a named SQL query directly.

        Args:
            name: Name for the SQL query.
            sql: Raw SQL content.
            dialect: Optional dialect for the SQL statement.
        """
        loader = self._ensure_sql_loader()
        loader.add_named_sql(name, sql, dialect)
        logger.debug("Added named SQL: %s", name)

    def get_sql(self, name: str) -> "SQL":
        """Get a SQL object by name.

        Args:
            name: Name of the statement from SQL file comments.
                  Hyphens in names are converted to underscores.

        Returns:
            SQL object ready for execution.
        """
        loader = self._ensure_sql_loader()
        return loader.get_sql(name)

    def list_sql_queries(self) -> "list[str]":
        """List all available query names.

        Returns:
            Sorted list of query names.
        """
        if self._sql_loader is None:
            return []
        return self._sql_loader.list_queries()

    def has_sql_query(self, name: str) -> bool:
        """Check if a SQL query exists.

        Args:
            name: Query name to check.

        Returns:
            True if the query exists in the loader.
        """
        if self._sql_loader is None:
            return False
        return self._sql_loader.has_query(name)

    def clear_sql_cache(self) -> None:
        """Clear the SQL file cache."""
        if self._sql_loader is not None:
            self._sql_loader.clear_cache()
            logger.debug("Cleared SQL cache")

    def reload_sql_files(self) -> None:
        """Reload all SQL files.

        Note:
            This clears the cache and requires calling load_sql_files again.
        """
        if self._sql_loader is not None:
            self._sql_loader.clear_cache()
            logger.debug("Cleared SQL cache for reload")

    def get_sql_files(self) -> "list[str]":
        """Get list of loaded SQL files.

        Returns:
            Sorted list of file paths.
        """
        if self._sql_loader is None:
            return []
        return self._sql_loader.list_files()
