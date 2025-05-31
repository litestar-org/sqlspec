# ruff: noqa: PLR6301
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from sqlspec.typing import ConnectionT, Counter, Gauge, PoolT  # pyright: ignore

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.statement.result import StatementResult


StatementResultType = Union["StatementResult[dict[str, Any]]", "StatementResult[Any]"]


__all__ = (
    "AsyncDatabaseConfig",
    "DatabaseConfigProtocol",
    "GenericPoolConfig",
    "InstrumentationConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SyncDatabaseConfig",
)

AsyncConfigT = TypeVar("AsyncConfigT", bound="Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]]")
SyncConfigT = TypeVar("SyncConfigT", bound="Union[SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]")
ConfigT = TypeVar(
    "ConfigT",
    bound="Union[Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]], SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]",
)
DriverT = TypeVar("DriverT", bound="Union[SyncDriverAdapterProtocol[Any], AsyncDriverAdapterProtocol[Any]]")

logger = logging.getLogger("sqlspec")


@dataclass
class InstrumentationConfig:
    """Configuration for built-in driver instrumentation."""

    # Core logging (always enabled)
    log_queries: bool = True
    log_runtime: bool = True
    log_parameters: bool = False  # Security: off by default
    log_results_count: bool = True
    log_pool_operations: bool = True

    # Optional advanced instrumentation
    enable_opentelemetry: bool = False
    enable_prometheus: bool = False

    # Performance thresholds
    slow_query_threshold_ms: float = 1000.0
    slow_pool_operation_ms: float = 5000.0

    # Custom span configuration
    service_name: str = "sqlspec"
    custom_tags: dict[str, str] = field(default_factory=dict)
    prometheus_latency_buckets: Optional[list[float]] = None


@dataclass
class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    connection_type: "type[ConnectionT]" = field(init=False)
    driver_type: "type[DriverT]" = field(init=False)
    pool_instance: "Optional[PoolT]" = field(default=None)
    instrumentation: InstrumentationConfig = field(default_factory=InstrumentationConfig)
    _pool_metrics: Optional[dict[str, Any]] = field(default=None, init=False, repr=False, hash=False, compare=False)
    __is_async__: "ClassVar[bool]" = False
    __supports_connection_pooling__: "ClassVar[bool]" = False

    def __hash__(self) -> int:
        return id(self)

    @abstractmethod
    def create_connection(self) -> "Union[ConnectionT, Awaitable[ConnectionT]]":
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> "Union[AbstractContextManager[ConnectionT], AbstractAsyncContextManager[ConnectionT]]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    @abstractmethod
    def provide_session(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> "Union[AbstractContextManager[DriverT], AbstractAsyncContextManager[DriverT]]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @property
    @abstractmethod
    def connection_config_dict(self) -> "dict[str, Any]":
        """Return the connection configuration as a dict."""
        raise NotImplementedError

    @abstractmethod
    def create_pool(self) -> "Union[PoolT, Awaitable[PoolT]]":
        """Create and return connection pool."""
        raise NotImplementedError

    @abstractmethod
    def close_pool(self) -> "Optional[Awaitable[None]]":
        """Terminate the connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> "Union[PoolT, Awaitable[PoolT], AbstractContextManager[PoolT], AbstractAsyncContextManager[PoolT]]":
        """Provide pool instance."""
        raise NotImplementedError

    @property
    def is_async(self) -> bool:
        """Return whether the configuration is for an async database."""
        return self.__is_async__

    @property
    def support_connection_pooling(self) -> bool:
        """Return whether the configuration supports connection pooling."""
        return self.__supports_connection_pooling__

    def instrument_sync_operation(
        self,
        operation_name: str,
        operation_type: str,
        custom_tags_from_decorator: dict[str, Any],
        func_to_execute: Any,
        original_self: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Sync instrumentation for config operations (simplified version for configs)."""
        if self.instrumentation.log_queries:
            logger.info("Config operation: %s", operation_name, extra={"operation_type": operation_type})
        try:
            return func_to_execute(original_self, *args, **kwargs)
        except Exception as e:
            if self.instrumentation.log_queries:
                logger.exception("Config operation failed: %s", operation_name, extra={"error_type": type(e).__name__})
            raise

    async def instrument_async_operation(
        self,
        operation_name: str,
        operation_type: str,
        custom_tags_from_decorator: dict[str, Any],
        func_to_execute: Any,
        original_self: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Async instrumentation for config operations (simplified version for configs)."""
        if self.instrumentation.log_queries:
            logger.info("Config operation: %s", operation_name, extra={"operation_type": operation_type})
        try:
            return await func_to_execute(original_self, *args, **kwargs)
        except Exception as e:
            if self.instrumentation.log_queries:
                logger.exception("Config operation failed: %s", operation_name, extra={"error_type": type(e).__name__})
            raise


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for a sync database configurations that do not implement a pool."""

    __is_async__ = False
    __supports_connection_pooling__ = False
    pool_instance: None = None
    instrumentation: InstrumentationConfig = field(default_factory=InstrumentationConfig)

    def create_connection(self) -> ConnectionT:
        """Create connection with instrumentation."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide connection with instrumentation."""
        raise NotImplementedError

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[DriverT]":
        """Provide session with instrumentation."""
        raise NotImplementedError

    def create_pool(self) -> None:
        return None

    def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for an async database configurations that do not implement a pool."""

    __is_async__ = True
    __supports_connection_pooling__ = False
    pool_instance: None = None
    instrumentation: InstrumentationConfig = field(default_factory=InstrumentationConfig)

    async def create_connection(self) -> ConnectionT:
        """Create connection with instrumentation."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide connection with instrumentation."""
        raise NotImplementedError

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[DriverT]":
        """Provide session with instrumentation."""
        raise NotImplementedError

    async def create_pool(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


@dataclass
class GenericPoolConfig:
    """Generic Database Pool Configuration."""


@dataclass
class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Sync Database Configuration."""

    __is_async__ = False
    __supports_connection_pooling__ = True
    instrumentation: InstrumentationConfig = field(default_factory=InstrumentationConfig)

    def create_pool(self) -> PoolT:
        """Create pool with instrumentation.

        Returns:
            The created pool.
        """
        if self.instrumentation.log_pool_operations:
            logger.info("Creating database connection pool", extra={"adapter": self.__class__.__name__})

        if self._pool_metrics and isinstance(self._pool_metrics.get("pool_operations"), Counter):
            self._pool_metrics["pool_operations"].labels(adapter=self.__class__.__name__, operation="create").inc()

        pool = self._create_pool_impl()

        if self._pool_metrics and isinstance(self._pool_metrics.get("pool_connections"), Gauge):
            pool_size = getattr(self, "pool_size", 10)  # Default fallback
            self._pool_metrics["pool_connections"].labels(adapter=self.__class__.__name__, status="total").set(
                pool_size
            )

        if self.instrumentation.log_pool_operations:
            logger.info("Database connection pool created successfully", extra={"adapter": self.__class__.__name__})

        return pool

    def close_pool(self) -> None:
        """Close pool with instrumentation."""
        if self.instrumentation.log_pool_operations:
            logger.info("Closing database connection pool", extra={"adapter": self.__class__.__name__})

        if self._pool_metrics and isinstance(self._pool_metrics.get("pool_operations"), Counter):
            self._pool_metrics["pool_operations"].labels(adapter=self.__class__.__name__, operation="destroy").inc()

            if isinstance(self._pool_metrics.get("pool_connections"), Gauge):
                for status in ["total", "active", "idle"]:
                    self._pool_metrics["pool_connections"].labels(adapter=self.__class__.__name__, status=status).set(0)

        self._close_pool_impl()

        if self.instrumentation.log_pool_operations:
            logger.info("Database connection pool closed successfully", extra={"adapter": self.__class__.__name__})

    def create_connection(self) -> ConnectionT:
        """Create connection with instrumentation."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide connection with instrumentation."""
        raise NotImplementedError

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[DriverT]":
        """Provide session with instrumentation."""
        raise NotImplementedError

    @abstractmethod
    def _create_pool_impl(self) -> PoolT:
        """Actual pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    def _close_pool_impl(self) -> None:
        """Actual pool destruction implementation."""
        raise NotImplementedError


@dataclass
class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Async Database Configuration."""

    __is_async__ = True
    __supports_connection_pooling__ = True
    instrumentation: InstrumentationConfig = field(default_factory=InstrumentationConfig)

    async def create_pool(self) -> PoolT:
        """Create pool with instrumentation.

        Returns:
            The created pool.
        """
        if self.instrumentation.log_pool_operations:
            logger.info("Creating async database connection pool", extra={"adapter": self.__class__.__name__})

        if self._pool_metrics and isinstance(self._pool_metrics.get("pool_operations"), Counter):
            self._pool_metrics["pool_operations"].labels(adapter=self.__class__.__name__, operation="create").inc()

        pool = await self._create_pool_impl()

        if self._pool_metrics and isinstance(self._pool_metrics.get("pool_connections"), Gauge):
            pool_size = getattr(self, "pool_size", 10)  # Default fallback
            self._pool_metrics["pool_connections"].labels(adapter=self.__class__.__name__, status="total").set(
                pool_size
            )

        if self.instrumentation.log_pool_operations:
            logger.info(
                "Async database connection pool created successfully", extra={"adapter": self.__class__.__name__}
            )

        return pool

    async def close_pool(self) -> None:
        """Close pool with instrumentation."""
        if self.instrumentation.log_pool_operations:
            logger.info("Closing async database connection pool", extra={"adapter": self.__class__.__name__})

        if self._pool_metrics and isinstance(self._pool_metrics.get("pool_operations"), Counter):
            self._pool_metrics["pool_operations"].labels(adapter=self.__class__.__name__, operation="destroy").inc()
            if isinstance(self._pool_metrics.get("pool_connections"), Gauge):
                for status in ["total", "active", "idle"]:
                    self._pool_metrics["pool_connections"].labels(adapter=self.__class__.__name__, status=status).set(0)

        await self._close_pool_impl()

        if self.instrumentation.log_pool_operations:
            logger.info(
                "Async database connection pool closed successfully", extra={"adapter": self.__class__.__name__}
            )

    async def create_connection(self) -> ConnectionT:
        """Create connection with instrumentation."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide connection with instrumentation."""
        raise NotImplementedError

    def provide_session(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[DriverT]":
        """Provide session with instrumentation."""
        raise NotImplementedError

    @abstractmethod
    async def _create_pool_impl(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool_impl(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError
