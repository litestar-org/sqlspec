import asyncio
import atexit
import weakref
from collections.abc import Awaitable, Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeGuard, cast, overload

from typing_extensions import Self, TypeVar

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
    reset_stats_only,
    update_cache_config,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.events import AsyncEventChannel, SyncEventChannel
from sqlspec.loader import SQLFileLoader
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime, TelemetryDiagnostics
from sqlspec.typing import ConnectionT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_name

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

    from sqlspec.core import SQL
    from sqlspec.typing import PoolT


__all__ = ("SQLSpec",)

logger = get_logger()
ContextValueT = TypeVar("ContextValueT")


def _is_async_context_manager(obj: Any) -> TypeGuard[AbstractAsyncContextManager[Any]]:
    return isinstance(obj, AbstractAsyncContextManager)


class _RuntimeContext(Generic[ContextValueT]):
    __slots__ = ("_config", "_context", "_runtime", "_value")

    def __init__(
        self,
        context: "AbstractContextManager[Any]",
        runtime: "ObservabilityRuntime",
        config: "DatabaseConfigProtocol[Any, Any, Any] | None" = None,
    ) -> None:
        self._context = context
        self._runtime = runtime
        self._config = config
        self._value: Any | None = None

    def __enter__(self) -> ContextValueT:
        value = self._context.__enter__()
        config = self._config
        if config is not None:
            driver = config._prepare_driver(value)  # pyright: ignore[reportPrivateUsage]
            self._value = driver
            connection = driver.connection
            if connection is not None:
                self._runtime.emit_connection_create(connection)
            self._runtime.emit_session_start(driver)
            return cast("ContextValueT", driver)

        self._value = value
        self._runtime.emit_connection_create(value)
        return cast("ContextValueT", value)

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        try:
            return self._context.__exit__(exc_type, exc_val, exc_tb)
        finally:
            value = self._value
            if value is not None:
                if self._config is None:
                    self._runtime.emit_connection_destroy(value)
                else:
                    self._runtime.emit_session_end(value)
                    connection = value.connection
                    if connection is not None:
                        self._runtime.emit_connection_destroy(connection)
                self._value = None


class _RuntimeAsyncContext(Generic[ContextValueT]):
    __slots__ = ("_config", "_context", "_runtime", "_value")

    def __init__(
        self,
        context: "AbstractAsyncContextManager[Any]",
        runtime: "ObservabilityRuntime",
        config: "DatabaseConfigProtocol[Any, Any, Any] | None" = None,
    ) -> None:
        self._context = context
        self._runtime = runtime
        self._config = config
        self._value: Any | None = None

    async def __aenter__(self) -> ContextValueT:
        value = await self._context.__aenter__()
        config = self._config
        if config is not None:
            driver = config._prepare_driver(value)  # pyright: ignore[reportPrivateUsage]
            self._value = driver
            connection = driver.connection
            if connection is not None:
                self._runtime.emit_connection_create(connection)
            self._runtime.emit_session_start(driver)
            return cast("ContextValueT", driver)

        self._value = value
        self._runtime.emit_connection_create(value)
        return cast("ContextValueT", value)

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> "bool | None":
        try:
            return await self._context.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            value = self._value
            if value is not None:
                if self._config is None:
                    self._runtime.emit_connection_destroy(value)
                else:
                    self._runtime.emit_session_end(value)
                    connection = value.connection
                    if connection is not None:
                        self._runtime.emit_connection_destroy(connection)
                self._value = None


class _CacheManager:
    """Private coordinator for global SQLSpec cache helpers."""

    __slots__ = ()

    @staticmethod
    def get_cache_config() -> CacheConfig:
        return get_cache_config()

    @staticmethod
    def update_cache_config(config: CacheConfig) -> None:
        update_cache_config(config)

    @staticmethod
    def get_cache_stats() -> "dict[str, Any]":
        return get_cache_statistics()

    @staticmethod
    def reset_cache_stats() -> None:
        reset_cache_stats()

    @staticmethod
    def reset_stats_only() -> None:
        reset_stats_only()

    @staticmethod
    def log_cache_stats() -> None:
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


class _SQLFileManager:
    """Private coordinator for SQL file loader lifecycle and delegation."""

    __slots__ = ("_loader", "runtime")

    def __init__(self, loader: "SQLFileLoader | None", runtime: "ObservabilityRuntime") -> None:
        self._loader = loader
        self.runtime = runtime
        if self._loader is not None:
            self._loader.set_observability_runtime(runtime)

    def _ensure_loader(self) -> SQLFileLoader:
        if self._loader is None:
            self._loader = SQLFileLoader(runtime=self.runtime)
        else:
            self._loader.set_observability_runtime(self.runtime)
        return self._loader

    def load_sql_files(self, *paths: "str | Path") -> None:
        loader = self._ensure_loader()
        loader.load_sql(*paths)
        logger.debug("Loaded SQL files: %s", paths)

    def add_named_sql(self, name: str, sql: str, dialect: "str | None" = None) -> None:
        loader = self._ensure_loader()
        loader.add_named_sql(name, sql, dialect)
        logger.debug("Added named SQL: %s", name)

    def get_sql(self, name: str) -> "SQL":
        return self._ensure_loader().get_sql(name)

    def list_sql_queries(self) -> "list[str]":
        if self._loader is None:
            return []
        return self._loader.list_queries()

    def has_sql_query(self, name: str) -> bool:
        if self._loader is None:
            return False
        return self._loader.has_query(name)

    def clear_sql_cache(self) -> None:
        if self._loader is not None:
            self._loader.clear_cache()
            logger.debug("Cleared SQL cache")

    def reload_sql_files(self) -> None:
        if self._loader is not None:
            self._loader.clear_cache()
            logger.debug("Cleared SQL cache for reload")

    def get_sql_files(self) -> "list[str]":
        if self._loader is None:
            return []
        return self._loader.list_files()


_CACHE_MANAGER = _CacheManager()


class SQLSpec:
    """Configuration manager and registry for database connections and pools."""

    __slots__ = ("__weakref__", "_configs", "_observability_config", "_sql_files")

    _live_instances: "ClassVar[weakref.WeakSet[SQLSpec]]" = weakref.WeakSet()
    _atexit_registered: ClassVar[bool] = False

    def __init__(
        self, *, loader: "SQLFileLoader | None" = None, observability_config: "ObservabilityConfig | None" = None
    ) -> None:
        self._configs: dict[int, DatabaseConfigProtocol[Any, Any, Any]] = {}
        SQLSpec._live_instances.add(self)
        if not SQLSpec._atexit_registered:
            atexit.register(SQLSpec._cleanup_all_sync_pools)
            SQLSpec._atexit_registered = True
        self._observability_config = observability_config
        loader_runtime = ObservabilityRuntime(observability_config, config_name="SQLFileLoader")
        self._sql_files = _SQLFileManager(loader, loader_runtime)

    @classmethod
    def _cleanup_all_sync_pools(cls) -> None:
        """Walk every live SQLSpec and drain its sync pools at process exit."""
        for instance in list(cls._live_instances):
            instance._cleanup_sync_pools()  # pyright: ignore[reportPrivateUsage]

    @staticmethod
    def _get_config_name(obj: Any) -> str:
        """Get display name for configuration object."""
        if isinstance(obj, str):
            return obj
        if has_name(obj):
            return obj.__name__
        return type(obj).__name__

    def _cleanup_sync_pools(self) -> None:
        """Clean up only synchronous connection pools at exit."""
        cleaned_count = 0
        failed_configs: list[str] = []

        for config in self._configs.values():
            if config.supports_connection_pooling and not config.is_async:
                failure = self._safe_close_pool(config)
                if failure is None:
                    cleaned_count += 1
                else:
                    failed_configs.append(failure)

        if cleaned_count or failed_configs:
            summary: dict[str, object] = {"cleaned_pools": cleaned_count, "failed_pools": len(failed_configs)}
            if failed_configs:
                summary["failures"] = failed_configs
            logger.debug("Sync pool cleanup completed.", extra=summary)

    async def close_all_pools(self) -> None:
        """Explicitly close all connection pools (async and sync).

        This method should be called before application shutdown for proper cleanup.
        """
        cleanup_tasks = []
        sync_configs: list[DatabaseConfigProtocol[Any, Any, Any]] = []

        for config in self._configs.values():
            if config.supports_connection_pooling:
                try:
                    if config.is_async:
                        close_pool_awaitable = config.close_pool()
                        if close_pool_awaitable is not None:
                            cleanup_tasks.append(cast("Coroutine[Any, Any, None]", close_pool_awaitable))  # pyright: ignore
                    else:
                        sync_configs.append(config)  # pyright: ignore
                except Exception as e:
                    logger.debug("Failed to prepare cleanup for config %s: %s", config.__class__.__name__, e)

        async_failures: list[str] = []
        if cleanup_tasks:
            try:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)  # pyright: ignore
            except Exception as e:
                async_failures.append(str(e))

        for config in sync_configs:  # pyright: ignore
            failure = self._safe_close_pool(config)
            if failure is not None:
                async_failures.append(failure)

        if cleanup_tasks or sync_configs or async_failures:
            summary: dict[str, object] = {
                "async_pools": len(cleanup_tasks),
                "sync_pools": len(sync_configs),
                "failures": async_failures,
            }
            logger.debug("Pool cleanup completed.", extra=summary)

    @staticmethod
    def _safe_close_pool(config: "DatabaseConfigProtocol[Any, Any, Any]") -> "str | None":
        """Close a pool, returning an error string when it fails."""

        try:
            config.close_pool()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            return f"{config.__class__.__name__}: {exc}"
        return None

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self, _exc_type: "type[BaseException] | None", _exc_val: "BaseException | None", _exc_tb: "TracebackType | None"
    ) -> None:
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

    @overload
    def event_channel(self, config: "type[SyncConfigT]") -> "SyncEventChannel": ...

    @overload
    def event_channel(self, config: "type[AsyncConfigT]") -> "AsyncEventChannel": ...

    @overload
    def event_channel(
        self, config: "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]"
    ) -> "SyncEventChannel": ...

    @overload
    def event_channel(
        self, config: "AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]"
    ) -> "AsyncEventChannel": ...

    def event_channel(
        self,
        config: "type[SyncConfigT | AsyncConfigT] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]",
    ) -> "SyncEventChannel | AsyncEventChannel":
        """Create an event channel for the provided configuration.

        Returns SyncEventChannel for sync configs, AsyncEventChannel for async configs.

        Args:
            config: A registered database configuration instance or type.

        Returns:
            The appropriate event channel type for the configuration.
        """
        if isinstance(config, type):
            config_obj: DatabaseConfigProtocol[Any, Any, Any] | None = None
            for registered_config in self._configs.values():
                if isinstance(registered_config, config):
                    config_obj = registered_config
                    break
            if config_obj is None:
                msg = f"Configuration {self._get_config_name(config)} is not registered"
                raise ImproperConfigurationError(msg)
            if config_obj.is_async:
                return AsyncEventChannel(config_obj)  # type: ignore[arg-type]
            return SyncEventChannel(config_obj)  # type: ignore[arg-type]
        if config.is_async:
            return AsyncEventChannel(config)  # type: ignore[arg-type]
        return SyncEventChannel(config)  # type: ignore[arg-type]

    def telemetry_snapshot(self) -> "dict[str, Any]":
        """Return aggregated diagnostics across all registered configurations."""

        diagnostics = TelemetryDiagnostics()
        loader_metrics = self._sql_files.runtime.metrics_snapshot()
        if loader_metrics:
            diagnostics.add_metric_snapshot(loader_metrics)
        for config in self._configs.values():
            runtime = config.get_observability_runtime()
            diagnostics.add_lifecycle_snapshot(runtime.diagnostics_key, runtime.lifecycle_snapshot())
            metrics_snapshot = runtime.metrics_snapshot()
            if metrics_snapshot:
                diagnostics.add_metric_snapshot(metrics_snapshot)
        return diagnostics.snapshot()

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

        connection_obj = self.get_connection(config)

        if isinstance(connection_obj, Awaitable):
            async_config = cast(
                "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]", config
            )
            return self._create_driver_async(async_config, connection_obj)  # pyright: ignore

        driver = config.driver_type(  # pyright: ignore
            connection=connection_obj, statement_config=config.statement_config, driver_features=config.driver_features
        )
        return config._prepare_driver(driver)  # pyright: ignore

    async def _create_driver_async(
        self,
        config: "NoPoolAsyncConfig[ConnectionT, DriverT] | AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        connection_obj: "Awaitable[ConnectionT]",
    ) -> "DriverT":
        resolved_connection = await connection_obj
        driver = config.driver_type(  # pyright: ignore
            connection=resolved_connection,
            statement_config=config.statement_config,
            driver_features=config.driver_features,
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

        connection_context = config.provide_connection(*args, **kwargs)
        runtime = config.get_observability_runtime()

        if _is_async_context_manager(connection_context):
            async_context = cast("AbstractAsyncContextManager[ConnectionT]", connection_context)
            return _RuntimeAsyncContext[ConnectionT](async_context, runtime)

        sync_context = cast("AbstractContextManager[ConnectionT]", connection_context)
        return _RuntimeContext[ConnectionT](sync_context, runtime)

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

        session_context = config.provide_session(*args, **kwargs)
        runtime = config.get_observability_runtime()

        if _is_async_context_manager(session_context):
            async_session = cast("AbstractAsyncContextManager[DriverT]", session_context)
            return _RuntimeAsyncContext[DriverT](async_session, runtime, config)

        sync_session = cast("AbstractContextManager[DriverT]", session_context)
        return _RuntimeContext[DriverT](sync_session, runtime, config)

    @overload
    def get_pool(
        self, config: "NoPoolSyncConfig[ConnectionT, DriverT] | NoPoolAsyncConfig[ConnectionT, DriverT]"
    ) -> None: ...
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

        if config.supports_connection_pooling:
            return cast("type[PoolT] | Awaitable[type[PoolT]]", config.create_pool())
        return None

    @overload
    def close_pool(
        self, config: "NoPoolSyncConfig[ConnectionT, DriverT] | SyncDatabaseConfig[ConnectionT, PoolT, DriverT]"
    ) -> None: ...

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

        if config.supports_connection_pooling:
            return config.close_pool()
        return None

    @staticmethod
    def get_cache_config() -> CacheConfig:
        """Get the current global cache configuration.

        Returns:
            The current cache configuration.
        """
        return _CACHE_MANAGER.get_cache_config()

    @staticmethod
    def update_cache_config(config: CacheConfig) -> None:
        """Update the global cache configuration.

        Args:
            config: The new cache configuration to apply.
        """
        _CACHE_MANAGER.update_cache_config(config)

    @staticmethod
    def get_cache_stats() -> "dict[str, Any]":
        """Get current cache statistics.

        Returns:
            Cache statistics object with detailed metrics.
        """
        return _CACHE_MANAGER.get_cache_stats()

    @staticmethod
    def reset_cache_stats() -> None:
        """Reset all cache statistics to zero."""
        _CACHE_MANAGER.reset_cache_stats()

    @staticmethod
    def reset_stats_only() -> None:
        """Reset cache statistics without clearing cached data."""
        _CACHE_MANAGER.reset_stats_only()

    @staticmethod
    def log_cache_stats() -> None:
        """Log current cache statistics using the configured logger."""
        _CACHE_MANAGER.log_cache_stats()

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
            sql_cache_size: Size of the statement/builder cache.
            fragment_cache_size: Size of the expression/parameter/file cache.
            optimized_cache_size: Size of the optimized expression cache.
            sql_cache_enabled: Enable/disable statement and builder cache.
            fragment_cache_enabled: Enable/disable expression/parameter/file cache.
            optimized_cache_enabled: Enable/disable optimized expression cache.
        """
        _CACHE_MANAGER.configure_cache(
            sql_cache_size=sql_cache_size,
            fragment_cache_size=fragment_cache_size,
            optimized_cache_size=optimized_cache_size,
            sql_cache_enabled=sql_cache_enabled,
            fragment_cache_enabled=fragment_cache_enabled,
            optimized_cache_enabled=optimized_cache_enabled,
        )

    def load_sql_files(self, *paths: "str | Path") -> None:
        """Load SQL files from paths or directories.

        Args:
            *paths: One or more file paths or directory paths to load.
        """
        self._sql_files.load_sql_files(*paths)

    def add_named_sql(self, name: str, sql: str, dialect: "str | None" = None) -> None:
        """Add a named SQL query directly.

        Args:
            name: Name for the SQL query.
            sql: Raw SQL content.
            dialect: Optional dialect for the SQL statement.
        """
        self._sql_files.add_named_sql(name, sql, dialect)

    def get_sql(self, name: str) -> "SQL":
        """Get a SQL object by name.

        Args:
            name: Name of the statement from SQL file comments.
                  Hyphens in names are converted to underscores.

        Returns:
            SQL object ready for execution.
        """
        return self._sql_files.get_sql(name)

    def list_sql_queries(self) -> "list[str]":
        """List all available query names.

        Returns:
            Sorted list of query names.
        """
        return self._sql_files.list_sql_queries()

    def has_sql_query(self, name: str) -> bool:
        """Check if a SQL query exists.

        Args:
            name: Query name to check.

        Returns:
            True if the query exists in the loader.
        """
        return self._sql_files.has_sql_query(name)

    def clear_sql_cache(self) -> None:
        """Clear the SQL file cache."""
        self._sql_files.clear_sql_cache()

    def reload_sql_files(self) -> None:
        """Reload all SQL files.

        Note:
            This clears the cache and requires calling load_sql_files again.
        """
        self._sql_files.reload_sql_files()

    def get_sql_files(self) -> "list[str]":
        """Get list of loaded SQL files.

        Returns:
            Sorted list of file paths.
        """
        return self._sql_files.get_sql_files()
