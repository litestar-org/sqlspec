from abc import ABC, abstractmethod
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


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    __slots__ = (
        "_dialect",
        "adapter_cache_size",
        "connection_type",
        "default_row_type",
        "driver_type",
        "enable_adapter_cache",
        "enable_prepared_statements",
        "migration_config",
        "pool_instance",
        "prepared_statement_cache_size",
    )

    # ClassVar fields (not in __slots__)
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    supported_parameter_styles: "ClassVar[tuple[str, ...]]" = ()
    """Parameter styles supported by this database adapter (e.g., ('qmark', 'named_colon'))."""
    default_parameter_style: "ClassVar[str]" = "none"
    """The preferred/native parameter style for this database."""

    def __init__(
        self,
        pool_instance: "Optional[PoolT]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 500,
        enable_prepared_statements: bool = False,
        prepared_statement_cache_size: int = 100,
    ) -> None:
        self.pool_instance = pool_instance
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}
        """Migration configuration settings."""
        self.enable_adapter_cache = enable_adapter_cache
        """Enable adapter-level SQL compilation caching."""
        self.adapter_cache_size = adapter_cache_size
        """Maximum number of compiled SQL statements to cache per adapter."""
        self.enable_prepared_statements = enable_prepared_statements
        """Enable prepared statement pooling for supported databases."""
        self.prepared_statement_cache_size = prepared_statement_cache_size
        """Maximum number of prepared statements to maintain."""
        # Non-init fields - subclasses must set these
        self.connection_type: type[ConnectionT]
        self.driver_type: type[DriverT]
        self.default_row_type: type[Any] = dict
        self._dialect: Optional[DialectType] = None

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        # Only compare init fields (respecting compare=False from dataclass)
        return (
            self.pool_instance == other.pool_instance
            and self.migration_config == other.migration_config
            and self.enable_adapter_cache == other.enable_adapter_cache
            and self.adapter_cache_size == other.adapter_cache_size
            and self.enable_prepared_statements == other.enable_prepared_statements
            and self.prepared_statement_cache_size == other.prepared_statement_cache_size
        )

    def _repr_parts(self) -> list[str]:
        # Only include fields that had repr=True in dataclass
        return [
            f"pool_instance={self.pool_instance!r}",
            f"migration_config={self.migration_config!r}",
            f"enable_adapter_cache={self.enable_adapter_cache!r}",
            f"adapter_cache_size={self.adapter_cache_size!r}",
            f"enable_prepared_statements={self.enable_prepared_statements!r}",
            f"prepared_statement_cache_size={self.prepared_statement_cache_size!r}",
        ]

    def __repr__(self) -> str:
        parts = ", ".join(self._repr_parts())
        return f"{type(self).__name__}({parts})"

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
        """
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
        return {}


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for a sync database configurations that do not implement a pool."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False

    def __init__(
        self,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 500,
        enable_prepared_statements: bool = False,
        prepared_statement_cache_size: int = 100,
    ) -> None:
        super().__init__(
            pool_instance=None,
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
            enable_prepared_statements=enable_prepared_statements,
            prepared_statement_cache_size=prepared_statement_cache_size,
        )

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

    __slots__ = ()

    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False

    def __init__(
        self,
        migration_config: "Optional[dict[str, Any]]" = None,
        enable_adapter_cache: bool = True,
        adapter_cache_size: int = 500,
        enable_prepared_statements: bool = False,
        prepared_statement_cache_size: int = 100,
    ) -> None:
        super().__init__(
            pool_instance=None,
            migration_config=migration_config,
            enable_adapter_cache=enable_adapter_cache,
            adapter_cache_size=adapter_cache_size,
            enable_prepared_statements=enable_prepared_statements,
            prepared_statement_cache_size=prepared_statement_cache_size,
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

    async def create_pool(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class GenericPoolConfig:
    """Generic Database Pool Configuration."""

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        # No fields to compare in this empty class
        return True

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Sync Database Configuration."""

    __slots__ = ()  # No additional fields beyond parent

    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True

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


class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Async Database Configuration."""

    __slots__ = ()

    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True

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
