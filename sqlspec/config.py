# ruff: noqa: PLR6301
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
    cast,
)

from sqlspec.exceptions import wrap_exceptions
from sqlspec.typing import ConnectionT, Counter, Gauge, PoolT  # pyright: ignore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.statement.result import StatementResult


StatementResultType = Union["StatementResult[dict[str, Any]]", "StatementResult[Any]"]


__all__ = (
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
    "DatabaseConfigProtocol",
    "DriverT",
    "GenericPoolConfig",
    "InstrumentationConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "StatementResultType",
    "SyncConfigT",
    "SyncDatabaseConfig",
)

AsyncConfigT = TypeVar("AsyncConfigT", bound="Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]]")
SyncConfigT = TypeVar("SyncConfigT", bound="Union[SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]")
ConfigT = TypeVar(
    "ConfigT",
    bound="Union[Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]], SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]",
)
DriverT = TypeVar("DriverT", bound="Union[SyncDriverAdapterProtocol[Any], AsyncDriverAdapterProtocol[Any]]")

logger = get_logger("config")


@dataclass
class InstrumentationConfig:
    """Configuration for built-in driver instrumentation.

    This configuration controls logging, telemetry, and performance monitoring
    for all database operations in SQLSpec.
    """

    # Core logging settings
    log_queries: bool = True
    """Whether to log database queries."""

    log_runtime: bool = True
    """Whether to log query execution times."""

    log_parameters: bool = False  # Security: off by default
    """Whether to log query parameters (disabled by default for security)."""

    log_results_count: bool = True
    """Whether to log the number of rows returned by queries."""

    log_pool_operations: bool = True
    """Whether to log connection pool operations."""

    log_service_operations: bool = True
    """Whether to log service layer operations."""

    # Enhanced logging options
    debug_mode: bool = False
    """Enable debug-level logging with additional details."""

    log_format: str = "structured"
    """Log format: 'structured' (JSON) or 'simple' (text)."""

    structured_logging: bool = True
    """Whether to use structured logging with extra fields."""

    log_connection_events: bool = True
    """Whether to log connection lifecycle events."""

    log_transaction_events: bool = True
    """Whether to log transaction begin/commit/rollback events."""

    # Optional advanced instrumentation
    enable_opentelemetry: bool = False
    """Whether to enable OpenTelemetry tracing."""

    enable_prometheus: bool = False
    """Whether to enable Prometheus metrics collection."""

    # Performance thresholds
    slow_query_threshold_ms: float = 1000.0
    """Queries slower than this threshold are logged as warnings."""

    slow_pool_operation_ms: float = 5000.0
    """Pool operations slower than this threshold are logged as warnings."""

    # Correlation tracking
    correlation_id_header: str = "X-Correlation-ID"
    """HTTP header name for correlation ID tracking."""

    generate_correlation_id: bool = True
    """Whether to generate correlation IDs if not provided."""

    # Custom span configuration
    service_name: str = "sqlspec"
    """Service name for telemetry spans."""

    custom_tags: dict[str, str] = field(default_factory=dict)
    """Custom tags to add to all telemetry spans."""

    prometheus_latency_buckets: Optional[list[float]] = None
    """Custom latency buckets for Prometheus histograms."""
    # Resource limits
    max_query_log_length: int = 1000
    """Maximum length of queries to log (truncate if longer)."""

    max_parameter_log_count: int = 10
    """Maximum number of parameters to log."""

    # Export configuration
    telemetry_endpoint: Optional[str] = None
    """OpenTelemetry collector endpoint."""

    metrics_endpoint: Optional[str] = None
    """Prometheus metrics endpoint."""

    def __post_init__(self) -> None:
        """Ensure custom_tags is properly isolated and validate configuration."""
        if self.custom_tags is not None:
            self.custom_tags = dict(self.custom_tags)

        # Auto-enable structured logging if format is structured
        if self.log_format == "structured":
            self.structured_logging = True


@dataclass
class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    connection_type: "type[ConnectionT]" = field(init=False)
    driver_type: "type[DriverT]" = field(init=False)
    pool_instance: "Optional[PoolT]" = field(default=None)
    instrumentation: InstrumentationConfig = field(default_factory=InstrumentationConfig)
    _pool_metrics: Optional[dict[str, Any]] = field(default=None, init=False, repr=False, hash=False, compare=False)
    _dialect: "Optional[DialectType]" = field(default=None, init=False, repr=False, hash=False, compare=False)
    __is_async__: "ClassVar[bool]" = False
    __supports_connection_pooling__: "ClassVar[bool]" = False

    # Parameter style support information
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ()
    """Parameter styles supported by this database adapter (e.g., ('qmark', 'named_colon'))."""

    preferred_parameter_style: "ClassVar[str]" = "none"
    """The preferred/native parameter style for this database."""

    def __hash__(self) -> int:
        return id(self)

    @property
    def dialect(self) -> "DialectType":
        """Get the SQL dialect type lazily.

        This property allows dialect to be set either statically as a class attribute
        or dynamically via the _get_dialect() method. If a specific adapter needs
        dynamic dialect detection (e.g., ADBC which supports multiple databases),
        it can override _get_dialect() to provide custom logic.

        Returns:
            The SQL dialect type for this database.
        """
        if self._dialect is None:
            self._dialect = self._get_dialect()
        return self._dialect

    def _get_dialect(self) -> "DialectType":
        """Get the dialect for this database configuration.

        This method should be overridden by configs that need dynamic dialect detection.
        By default, it looks for the dialect on the driver class.

        Returns:
            The SQL dialect type.

        Raises:
            AttributeError: If no dialect is found.
        """
        # Try to get dialect from driver_class (class attribute)
        driver_class = getattr(self.__class__, "driver_class", None)
        if driver_class is not None:
            with wrap_exceptions(suppress=AttributeError):
                return cast("DialectType", driver_class.dialect)

        # Try to get dialect from driver_type (instance property)
        with wrap_exceptions(suppress=AttributeError):
            return cast("DialectType", self.driver_type.dialect)

        # If not found, raise error
        msg = f"No dialect defined for {self.__class__.__name__}. Set driver_class as a class attribute pointing to your driver class."
        raise AttributeError(msg)

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

        pool = self._create_pool()

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

        self._close_pool()

        if self.instrumentation.log_pool_operations:
            logger.info("Database connection pool closed successfully", extra={"adapter": self.__class__.__name__})

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return self.pool_instance

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
    def _create_pool(self) -> PoolT:
        """Actual pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    def _close_pool(self) -> None:
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

        pool = await self._create_pool()

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

        await self._close_pool()

        if self.instrumentation.log_pool_operations:
            logger.info(
                "Async database connection pool closed successfully", extra={"adapter": self.__class__.__name__}
            )

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

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
    async def _create_pool(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError
