import asyncio
import atexit
from collections.abc import Awaitable, Coroutine
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

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
from sqlspec.core.cache import (
    CacheConfig,
    CacheStatsAggregate,
    get_cache_config,
    get_cache_stats,
    log_cache_stats,
    reset_cache_stats,
    update_cache_config,
)
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.typing import ConnectionT, PoolT


__all__ = ("SQLSpec",)

logger = get_logger()


class SQLSpec:
    """Configuration manager and registry for database connections and pools."""

    __slots__ = ("_cleanup_tasks", "_configs", "_instance_cache_config")

    def __init__(self) -> None:
        self._configs: dict[Any, DatabaseConfigProtocol[Any, Any, Any]] = {}
        atexit.register(self._cleanup_pools)
        self._instance_cache_config: Optional[CacheConfig] = None
        self._cleanup_tasks: list[asyncio.Task[None]] = []

    @staticmethod
    def _get_config_name(obj: Any) -> str:
        """Get display name for configuration object."""
        return getattr(obj, "__name__", str(obj))

    def _cleanup_pools(self) -> None:
        """Clean up all registered connection pools."""
        cleaned_count = 0

        for config_type, config in self._configs.items():
            if config.supports_connection_pooling:
                try:
                    if config.is_async:
                        close_pool_awaitable = config.close_pool()
                        if close_pool_awaitable is not None:
                            try:
                                loop = asyncio.get_running_loop()
                                if loop.is_running():
                                    task = asyncio.create_task(cast("Coroutine[Any, Any, None]", close_pool_awaitable))
                                    self._cleanup_tasks.append(task)
                                else:
                                    asyncio.run(cast("Coroutine[Any, Any, None]", close_pool_awaitable))
                            except RuntimeError:
                                asyncio.run(cast("Coroutine[Any, Any, None]", close_pool_awaitable))
                    else:
                        config.close_pool()
                    cleaned_count += 1
                except Exception as e:
                    logger.warning("Failed to clean up pool for config %s: %s", config_type.__name__, e)

        if self._cleanup_tasks:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    asyncio.gather(*self._cleanup_tasks, return_exceptions=True)
            except RuntimeError:
                pass

        self._configs.clear()
        logger.info("Pool cleanup completed. Cleaned %d pools.", cleaned_count)

    @overload
    def add_config(self, config: "SyncConfigT") -> "type[SyncConfigT]":  # pyright: ignore[reportInvalidTypeVarUse]
        ...

    @overload
    def add_config(self, config: "AsyncConfigT") -> "type[AsyncConfigT]":  # pyright: ignore[reportInvalidTypeVarUse]
        ...

    def add_config(self, config: "Union[SyncConfigT, AsyncConfigT]") -> "type[Union[SyncConfigT, AsyncConfigT]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Add a configuration instance to the registry.

        Args:
            config: The configuration instance to add.

        Returns:
            The type of the added configuration, annotated with its ID for potential use in type systems.
        """
        config_type = type(config)
        if config_type in self._configs:
            logger.warning("Configuration for %s already exists. Overwriting.", config_type.__name__)
        self._configs[config_type] = config
        return config_type

    @overload
    def get_config(self, name: "type[SyncConfigT]") -> "SyncConfigT": ...

    @overload
    def get_config(self, name: "type[AsyncConfigT]") -> "AsyncConfigT": ...

    def get_config(
        self, name: "Union[type[DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]], Any]"
    ) -> "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]":
        """Retrieve a configuration instance by its type or a key.

        Args:
            name: The type of the configuration or a key associated with it.

        Returns:
            The configuration instance.

        Raises:
            KeyError: If the configuration is not found.
        """
        config = self._configs.get(name)
        if not config:
            logger.error("No configuration found for %s", name)
            msg = f"No configuration found for {name}"
            raise KeyError(msg)

        logger.debug("Retrieved configuration: %s", self._get_config_name(name))
        return config

    @overload
    def get_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "ConnectionT": ...

    @overload
    def get_connection(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Awaitable[ConnectionT]": ...

    def get_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Union[ConnectionT, Awaitable[ConnectionT]]":
        """Get a database connection for the specified configuration.

        Args:
            name: The configuration name or instance.

        Returns:
            A database connection or an awaitable yielding a connection.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
            config_name = config.__class__.__name__
        else:
            config = self.get_config(name)
            config_name = self._get_config_name(name)

        logger.debug("Getting connection for config: %s", config_name, extra={"config_type": config_name})
        return config.create_connection()

    @overload
    def get_session(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "DriverT": ...

    @overload
    def get_session(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Awaitable[DriverT]": ...

    def get_session(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Union[DriverT, Awaitable[DriverT]]":
        """Get a database session (driver adapter) for the specified configuration.

        Args:
            name: The configuration name or instance.

        Returns:
            A driver adapter instance or an awaitable yielding one.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
            config_name = config.__class__.__name__
        else:
            config = self.get_config(name)
            config_name = self._get_config_name(name)

        logger.debug("Getting session for config: %s", config_name, extra={"config_type": config_name})

        connection_obj = self.get_connection(name)

        if isinstance(connection_obj, Awaitable):

            async def _create_driver_async() -> "DriverT":
                resolved_connection = await connection_obj  # pyright: ignore
                return cast(  # pyright: ignore
                    "DriverT", config.driver_type(connection=resolved_connection)
                )

            return _create_driver_async()

        return cast(  # pyright: ignore
            "DriverT", config.driver_type(connection=connection_obj)
        )

    @overload
    def provide_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractContextManager[ConnectionT]": ...

    @overload
    def provide_connection(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[ConnectionT]": ...

    def provide_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "Union[AbstractContextManager[ConnectionT], AbstractAsyncContextManager[ConnectionT]]":
        """Create and provide a database connection from the specified configuration.

        Args:
            name: The configuration name or instance.
            *args: Positional arguments to pass to the config's provide_connection.
            **kwargs: Keyword arguments to pass to the config's provide_connection.


        Returns:
            A sync or async context manager yielding a connection.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
            config_name = config.__class__.__name__
        else:
            config = self.get_config(name)
            config_name = self._get_config_name(name)

        logger.debug("Providing connection context for config: %s", config_name, extra={"config_type": config_name})
        return config.provide_connection(*args, **kwargs)

    @overload
    def provide_session(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractContextManager[DriverT]": ...

    @overload
    def provide_session(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "AbstractAsyncContextManager[DriverT]": ...

    def provide_session(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "Union[AbstractContextManager[DriverT], AbstractAsyncContextManager[DriverT]]":
        """Create and provide a database session from the specified configuration.

        Args:
            name: The configuration name or instance.
            *args: Positional arguments to pass to the config's provide_session.
            **kwargs: Keyword arguments to pass to the config's provide_session.

        Returns:
            A sync or async context manager yielding a driver adapter instance.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
            config_name = config.__class__.__name__
        else:
            config = self.get_config(name)
            config_name = self._get_config_name(name)

        logger.debug("Providing session context for config: %s", config_name, extra={"config_type": config_name})
        return config.provide_session(*args, **kwargs)

    @overload
    def get_pool(
        self,
        name: "Union[type[Union[NoPoolSyncConfig[ConnectionT, DriverT], NoPoolAsyncConfig[ConnectionT, DriverT]]], NoPoolSyncConfig[ConnectionT, DriverT], NoPoolAsyncConfig[ConnectionT, DriverT]]",
    ) -> "None": ...
    @overload
    def get_pool(
        self,
        name: "Union[type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]], SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
    ) -> "type[PoolT]": ...
    @overload
    def get_pool(
        self,
        name: "Union[type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]],AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
    ) -> "Awaitable[type[PoolT]]": ...

    def get_pool(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Union[type[PoolT], Awaitable[type[PoolT]], None]":
        """Get the connection pool for the specified configuration.

        Args:
            name: The configuration name or instance.

        Returns:
            The connection pool, an awaitable yielding the pool, or None if not supported.
        """
        config = (
            name
            if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig))
            else self.get_config(name)
        )
        config_name = config.__class__.__name__

        if config.supports_connection_pooling:
            logger.debug("Getting pool for config: %s", config_name, extra={"config_type": config_name})
            return cast("Union[type[PoolT], Awaitable[type[PoolT]]]", config.create_pool())

        logger.debug("Config %s does not support connection pooling", config_name)
        return None

    @overload
    def close_pool(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "None": ...

    @overload
    def close_pool(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Awaitable[None]": ...

    def close_pool(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
            "NoPoolAsyncConfig[ConnectionT, DriverT]",
            "AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "Optional[Awaitable[None]]":
        """Close the connection pool for the specified configuration.

        Args:
            name: The configuration name or instance.

        Returns:
            None, or an awaitable if closing an async pool.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
            config_name = config.__class__.__name__
        else:
            config = self.get_config(name)
            config_name = self._get_config_name(name)

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
    def get_cache_stats() -> CacheStatsAggregate:
        """Get current cache statistics.

        Returns:
            Cache statistics object with detailed metrics.
        """
        return get_cache_stats()

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
        sql_cache_size: Optional[int] = None,
        fragment_cache_size: Optional[int] = None,
        optimized_cache_size: Optional[int] = None,
        sql_cache_enabled: Optional[bool] = None,
        fragment_cache_enabled: Optional[bool] = None,
        optimized_cache_enabled: Optional[bool] = None,
    ) -> None:
        """Update cache configuration with partial values.

        Args:
            sql_cache_size: Size of the SQL statement cache
            fragment_cache_size: Size of the AST fragment cache
            optimized_cache_size: Size of the optimized expression cache
            sql_cache_enabled: Enable/disable SQL cache
            fragment_cache_enabled: Enable/disable fragment cache
            optimized_cache_enabled: Enable/disable optimized cache
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
