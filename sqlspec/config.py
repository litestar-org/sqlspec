from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, TypeVar, Union

from sqlspec.typing import ConnectionT, PoolT  # pyright: ignore
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
class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    # Note: __slots__ cannot be used with dataclass fields in Python < 3.10
    # Concrete subclasses can still use __slots__ for any additional attributes
    __slots__ = ()

    is_async: "ClassVar[bool]" = field(init=False, default=False)
    supports_connection_pooling: "ClassVar[bool]" = field(init=False, default=False)
    supports_native_arrow_import: "ClassVar[bool]" = field(init=False, default=False)
    supports_native_arrow_export: "ClassVar[bool]" = field(init=False, default=False)
    supports_native_parquet_import: "ClassVar[bool]" = field(init=False, default=False)
    supports_native_parquet_export: "ClassVar[bool]" = field(init=False, default=False)
    connection_type: "type[ConnectionT]" = field(init=False, repr=False, hash=False, compare=False)
    driver_type: "type[DriverT]" = field(init=False, repr=False, hash=False, compare=False)
    pool_instance: "Optional[PoolT]" = field(default=None)
    default_row_type: "type[Any]" = field(init=False)
    _dialect: "DialectType" = field(default=None, init=False, repr=False, hash=False, compare=False)

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
            self._dialect = self._get_dialect()  # type: ignore[misc]
        return self._dialect

    def _get_dialect(self) -> "DialectType":
        """Get the dialect for this database configuration.

        This method should be overridden by configs that need dynamic dialect detection.
        By default, it looks for the dialect on the driver class.

        Returns:
            The SQL dialect type.
        """
        # Get dialect from driver_class (all drivers must have a dialect attribute)
        return self.driver_type.dialect

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
        self, *args: Any, **kwargs: Any
    ) -> "Union[PoolT, Awaitable[PoolT], AbstractContextManager[PoolT], AbstractAsyncContextManager[PoolT]]":
        """Provide pool instance."""
        raise NotImplementedError

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for this database configuration.

        This method returns a dictionary of type names to types that should be
        registered with Litestar's signature namespace to prevent serialization
        attempts on database-specific types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace: dict[str, type[Any]] = {}

        # Add the driver and config types
        if hasattr(self, "driver_type") and self.driver_type:
            namespace[self.driver_type.__name__] = self.driver_type

        namespace[self.__class__.__name__] = self.__class__

        # Add connection type(s)
        if hasattr(self, "connection_type") and self.connection_type:
            connection_type = self.connection_type

            # Handle Union types (like AsyncPG's Union[Connection, PoolConnectionProxy])
            if hasattr(connection_type, "__args__"):
                # It's a generic type, extract the actual types
                for arg_type in connection_type.__args__:  # type: ignore[attr-defined]
                    if arg_type and hasattr(arg_type, "__name__"):
                        namespace[arg_type.__name__] = arg_type
            elif hasattr(connection_type, "__name__"):
                # Regular type
                namespace[connection_type.__name__] = connection_type

        return namespace


@dataclass
class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for a sync database configurations that do not implement a pool."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = field(init=False, default=False)
    supports_connection_pooling: "ClassVar[bool]" = field(init=False, default=False)
    pool_instance: None = None

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


@dataclass
class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for an async database configurations that do not implement a pool."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = field(init=False, default=True)
    supports_connection_pooling: "ClassVar[bool]" = field(init=False, default=False)
    pool_instance: None = None

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

    __slots__ = ()


@dataclass
class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Sync Database Configuration."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = field(init=False, default=False)
    supports_connection_pooling: "ClassVar[bool]" = field(init=False, default=True)

    def create_pool(self) -> PoolT:
        """Create pool with instrumentation.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = self._create_pool()  # type: ignore[misc]
        return self.pool_instance

    def close_pool(self) -> None:
        """Close pool with instrumentation."""
        self._close_pool()

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()  # type: ignore[misc]
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

    __slots__ = ()

    is_async: "ClassVar[bool]" = field(init=False, default=True)
    supports_connection_pooling: "ClassVar[bool]" = field(init=False, default=True)

    async def create_pool(self) -> PoolT:
        """Create pool with instrumentation.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = await self._create_pool()  # type: ignore[misc]
        return self.pool_instance

    async def close_pool(self) -> None:
        """Close pool with instrumentation."""
        await self._close_pool()

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()  # type: ignore[misc]
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
