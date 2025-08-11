from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Generic, Optional, TypeVar, Union

from typing_extensions import NotRequired, TypedDict

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase


__all__ = (
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
    "DatabaseConfigProtocol",
    "DriverT",
    "LifecycleConfig",
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


class LifecycleConfig(TypedDict, total=False):
    """Universal lifecycle hooks for all adapters.

    Each hook accepts a list of callables to support multiple handlers.
    """

    on_connection_create: NotRequired[list[Callable[[Any], None]]]
    on_connection_destroy: NotRequired[list[Callable[[Any], None]]]
    on_pool_create: NotRequired[list[Callable[[Any], None]]]
    on_pool_destroy: NotRequired[list[Callable[[Any], None]]]
    on_session_start: NotRequired[list[Callable[[Any], None]]]
    on_session_end: NotRequired[list[Callable[[Any], None]]]
    on_query_start: NotRequired[list[Callable[[str, dict], None]]]
    on_query_complete: NotRequired[list[Callable[[str, dict, Any], None]]]
    on_error: NotRequired[list[Callable[[Exception, str, dict], None]]]


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    __slots__ = ("driver_features", "migration_config", "pool_instance", "statement_config")
    driver_type: "ClassVar[type[Any]]"
    connection_type: "ClassVar[type[Any]]"
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    statement_config: "StatementConfig"
    pool_instance: "Optional[PoolT]"
    migration_config: "dict[str, Any]"

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return bool(self.pool_instance == other.pool_instance and self.migration_config == other.migration_config)

    def __repr__(self) -> str:
        parts = ", ".join([f"pool_instance={self.pool_instance!r}", f"migration_config={self.migration_config!r}"])
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

    __slots__ = ("connection_config",)
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False

    def __init__(
        self,
        *,
        connection_config: Optional[dict[str, Any]] = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.pool_instance = None
        self.connection_config = connection_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="sqlite", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    def create_pool(self) -> None:
        return None

    def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for an async database configurations that do not implement a pool."""

    __slots__ = ("connection_config",)

    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False

    def __init__(
        self,
        *,
        connection_config: "Optional[dict[str, Any]]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.pool_instance = None
        self.connection_config = connection_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="sqlite", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    async def create_pool(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Sync Database Configuration."""

    __slots__ = ("pool_config",)

    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        pool_config: "Optional[dict[str, Any]]" = None,
        pool_instance: "Optional[PoolT]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.pool_instance = pool_instance
        self.pool_config = pool_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="postgres", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = self._create_pool()
        return self.pool_instance

    def close_pool(self) -> None:
        """Close the connection pool."""
        self._close_pool()

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return self.pool_instance

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
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

    __slots__ = ("pool_config",)

    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        pool_config: "Optional[dict[str, Any]]" = None,
        pool_instance: "Optional[PoolT]" = None,
        migration_config: "Optional[dict[str, Any]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self.pool_instance = pool_instance
        self.pool_config = pool_config or {}
        self.migration_config: dict[str, Any] = migration_config if migration_config is not None else {}

        if statement_config is None:
            self.statement_config = StatementConfig(
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
                dialect="postgres",
            )
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    async def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = await self._create_pool()
        return self.pool_instance

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "Optional[StatementConfig]" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @abstractmethod
    async def _create_pool(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError
