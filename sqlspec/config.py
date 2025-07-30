from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, TypeVar, Union

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.statement.sql import SQLConfig


__all__ = (
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
    "DatabaseConfigProtocol",
    "DriverT",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SyncConfigT",
    "SyncDatabaseConfig",
)

AsyncConfigT = TypeVar("AsyncConfigT", bound="Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]]")
SyncConfigT = TypeVar("SyncConfigT", bound="Union[SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]")
ConfigT = TypeVar(
    "ConfigT",
    bound="Union[Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]], SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]",
)

# Define TypeVars for Generic classes
ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")
DriverT = TypeVar("DriverT", bound="Union[SyncDriverAdapterBase, AsyncDriverAdapterBase]")

logger = get_logger("config")
DEFAULT_ADAPTER_CACHE_SIZE = 5000


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    __slots__ = ("adapter_cache_size", "migration_config", "pool_instance", "statement_config")
    driver_type: "ClassVar[type[Any]]"
    connection_type: "ClassVar[type[Any]]"
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    statement_config: "SQLConfig"

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (
            self.pool_instance == other.pool_instance
            and self.migration_config == other.migration_config
            and self.adapter_cache_size == other.adapter_cache_size
        )

    def __repr__(self) -> str:
        parts = ", ".join(
            [
                f"pool_instance={self.pool_instance!r}",
                f"migration_config={self.migration_config!r}",
                f"adapter_cache_size={self.adapter_cache_size!r}",
            ]
        )
        return f"{type(self).__name__}({parts})"

    @abstractmethod
    def create_connection(self) -> "Union[ConnectionT, Awaitable[ConnectionT]]":
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self, *args: Any, **kwargs: Any
    ) -> "Union[AbstractContextManager[ConnectionT], AbstractAsyncContextManager[ConnectionT]]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    @abstractmethod
    def provide_session(
        self, *args: Any, **kwargs: Any
    ) -> "Union[AbstractContextManager[DriverT], AbstractAsyncContextManager[DriverT]]":
        """Provide a database session context manager."""
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
        self, *args: Any, **kwargs: Any
    ) -> "Union[PoolT, Awaitable[PoolT], AbstractContextManager[PoolT], AbstractAsyncContextManager[PoolT]]":
        """Provide pool instance."""
        raise NotImplementedError

    @property
    def enable_adapter_cache(self) -> bool:
        """Check if adapter caching is enabled.

        Returns:
            True if adapter_cache_size > 0, False otherwise.
        """
        return self.adapter_cache_size > 0

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for this database configuration.

        This method returns a dictionary of type names to types that should be
        registered with Litestar's signature namespace to prevent serialization
        attempts on database-specific types.

        Returns:
            Dictionary mapping type names to types.
        """
        return {}


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for a sync database configurations that do not implement a pool."""

    __slots__ = ()
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False

    def __init__(
        self,
        *,
        connection_config: Optional[dict[str, Any]] = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        adapter_cache_size: int = DEFAULT_ADAPTER_CACHE_SIZE,
    ) -> None:
        from sqlspec.statement.sql import SQLConfig

        self.pool_instance = None
        self.connection_config = connection_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}
        self.statement_config = statement_config or SQLConfig()
        self.adapter_cache_size = adapter_cache_size

    def create_connection(self) -> ConnectionT:
        """Create connection with instrumentation."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide connection with instrumentation."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "Optional[SQLConfig]" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
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

    __slots__ = ()

    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False

    def __init__(
        self,
        *,
        connection_config: "Optional[dict[str, Any]]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        adapter_cache_size: int = DEFAULT_ADAPTER_CACHE_SIZE,
    ) -> None:
        from sqlspec.statement.sql import SQLConfig

        self.pool_instance = None
        self.connection_config = connection_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}
        self.statement_config = statement_config or SQLConfig()
        self.adapter_cache_size = adapter_cache_size

    async def create_connection(self) -> ConnectionT:
        """Create connection with instrumentation."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide connection with instrumentation."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "Optional[SQLConfig]" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide session with instrumentation."""
        raise NotImplementedError

    async def create_pool(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Sync Database Configuration."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        pool_config: "Optional[dict[str, Any]]" = None,
        pool_instance: "Optional[PoolT]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        adapter_cache_size: int = DEFAULT_ADAPTER_CACHE_SIZE,
    ) -> None:
        from sqlspec.statement.sql import SQLConfig

        self.pool_instance = pool_instance
        self.pool_config = pool_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}
        self.statement_config = statement_config or SQLConfig()
        self.adapter_cache_size = adapter_cache_size

    def create_pool(self) -> PoolT:
        """Create pool with instrumentation.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = self._create_pool()
        return self.pool_instance

    def close_pool(self) -> None:
        """Close pool with instrumentation."""
        self._close_pool()

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

    def provide_session(
        self, *args: Any, statement_config: "Optional[SQLConfig]" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
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


class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Async Database Configuration."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        pool_config: "Optional[dict[str, Any]]" = None,
        pool_instance: "Optional[PoolT]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[SQLConfig]" = None,
        adapter_cache_size: int = DEFAULT_ADAPTER_CACHE_SIZE,
    ) -> None:
        from sqlspec.statement.sql import SQLConfig

        self.pool_instance = pool_instance
        self.pool_config = pool_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}
        self.statement_config = statement_config or SQLConfig()
        self.adapter_cache_size = adapter_cache_size

    async def create_pool(self) -> PoolT:
        """Create pool with instrumentation.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = await self._create_pool()
        return self.pool_instance

    async def close_pool(self) -> None:
        """Close pool with instrumentation."""
        await self._close_pool()

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

    def provide_session(
        self, *args: Any, statement_config: "Optional[SQLConfig]" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
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
