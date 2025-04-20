# ruff: noqa: PLR6301
import re
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

from sqlspec.exceptions import NotFoundError
from sqlspec.statement import SQLStatement
from sqlspec.typing import ModelDTOT, StatementParameterType

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from pyarrow import Table as ArrowTable

__all__ = (
    "AsyncArrowBulkOperationsMixin",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterProtocol",
    "CommonDriverAttributes",
    "DatabaseConfigProtocol",
    "GenericPoolConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SQLSpec",
    "SQLStatement",
    "SyncArrowBulkOperationsMixin",
    "SyncDatabaseConfig",
    "SyncDriverAdapterProtocol",
)

T = TypeVar("T")
ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")
PoolT_co = TypeVar("PoolT_co", covariant=True)
AsyncConfigT = TypeVar("AsyncConfigT", bound="Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]]")
SyncConfigT = TypeVar("SyncConfigT", bound="Union[SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]")
ConfigT = TypeVar(
    "ConfigT",
    bound="Union[Union[AsyncDatabaseConfig[Any, Any, Any], NoPoolAsyncConfig[Any, Any]], SyncDatabaseConfig[Any, Any, Any], NoPoolSyncConfig[Any, Any]]",
)
DriverT = TypeVar("DriverT", bound="Union[SyncDriverAdapterProtocol[Any], AsyncDriverAdapterProtocol[Any]]")
# Regex to find :param or %(param)s style placeholders, skipping those inside quotes
PARAM_REGEX = re.compile(
    r"""
    (?P<dquote>"([^"]|\\")*") | # Double-quoted strings
    (?P<squote>'([^']|\\')*') | # Single-quoted strings
    : (?P<var_name_colon>[a-zA-Z_][a-zA-Z0-9_]*) | # :var_name
    % \( (?P<var_name_perc>[a-zA-Z_][a-zA-Z0-9_]*) \) s # %(var_name)s
    """,
    re.VERBOSE,
)


@dataclass
class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    connection_type: "type[ConnectionT]" = field(init=False)
    driver_type: "type[DriverT]" = field(init=False)
    pool_instance: "Optional[PoolT]" = field(default=None)
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


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for a sync database configurations that do not implement a pool."""

    __is_async__ = False
    __supports_connection_pooling__ = False
    pool_instance: None = None

    def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""
        return

    def close_pool(self) -> None:
        return

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""
        return


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for an async database configurations that do not implement a pool."""

    __is_async__ = True
    __supports_connection_pooling__ = False
    pool_instance: None = None

    async def create_pool(self) -> None:
        """This database backend has not implemented the pooling configurations."""
        return

    async def close_pool(self) -> None:
        return

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        """This database backend has not implemented the pooling configurations."""
        return


@dataclass
class GenericPoolConfig:
    """Generic Database Pool Configuration."""


@dataclass
class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Sync Database Configuration."""

    __is_async__ = False
    __supports_connection_pooling__ = True


@dataclass
class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Generic Async Database Configuration."""

    __is_async__ = True
    __supports_connection_pooling__ = True


class SQLSpec:
    """Type-safe configuration manager and registry for database connections and pools."""

    __slots__ = ("_configs",)

    def __init__(self) -> None:
        self._configs: dict[Any, DatabaseConfigProtocol[Any, Any, Any]] = {}

    @overload
    def add_config(self, config: "SyncConfigT") -> "type[SyncConfigT]": ...

    @overload
    def add_config(self, config: "AsyncConfigT") -> "type[AsyncConfigT]": ...

    def add_config(
        self,
        config: "Union[SyncConfigT, AsyncConfigT]",
    ) -> "Union[Annotated[type[SyncConfigT], int], Annotated[type[AsyncConfigT], int]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Add a new configuration to the manager.

        Returns:
            A unique type key that can be used to retrieve the configuration later.
        """
        key = Annotated[type(config), id(config)]  # type: ignore[valid-type]
        self._configs[key] = config
        return key  # type: ignore[return-value]  # pyright: ignore[reportReturnType]

    @overload
    def get_config(self, name: "type[SyncConfigT]") -> "SyncConfigT": ...

    @overload
    def get_config(self, name: "type[AsyncConfigT]") -> "AsyncConfigT": ...

    def get_config(
        self,
        name: "Union[type[DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]], Any]",
    ) -> "DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]":
        """Retrieve a configuration by its type.

        Returns:
            DatabaseConfigProtocol: The configuration instance for the given type.

        Raises:
            KeyError: If no configuration is found for the given type.
        """
        config = self._configs.get(name)
        if not config:
            msg = f"No configuration found for {name}"
            raise KeyError(msg)
        return config

    @overload
    def get_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",  # pyright: ignore[reportInvalidTypeVarUse]
        ],
    ) -> "ConnectionT": ...

    @overload
    def get_connection(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",  # pyright: ignore[reportInvalidTypeVarUse]
        ],
    ) -> "Awaitable[ConnectionT]": ...

    def get_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
        ],
    ) -> "Union[ConnectionT, Awaitable[ConnectionT]]":
        """Create and return a new database connection from the specified configuration.

        Args:
            name: The configuration type to use for creating the connection.

        Returns:
            Either a connection instance or an awaitable that resolves to a connection instance.
        """
        config = self.get_config(name)
        return config.create_connection()

    def get_session(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
        ],
    ) -> "Union[DriverT, Awaitable[DriverT]]":
        """Create and return a new database session from the specified configuration.

        Args:
            name: The configuration type to use for creating the session.

        Returns:
            Either a driver instance or an awaitable that resolves to a driver instance.
        """
        config = self.get_config(name)
        connection = self.get_connection(name)
        if isinstance(connection, Awaitable):

            async def _create_session() -> DriverT:
                return cast("DriverT", config.driver_type(await connection))  # pyright: ignore

            return _create_session()
        return cast("DriverT", config.driver_type(connection))  # pyright: ignore

    def provide_connection(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "Union[AbstractContextManager[ConnectionT], AbstractAsyncContextManager[ConnectionT]]":
        """Create and provide a database connection from the specified configuration.

        Args:
            name: The configuration type to use for creating the connection.
            *args: Positional arguments to pass to the configuration's provide_connection method.
            **kwargs: Keyword arguments to pass to the configuration's provide_connection method.

        Returns:
            Either a synchronous or asynchronous context manager that provides a database connection.
        """
        config = self.get_config(name)
        return config.provide_connection(*args, **kwargs)

    def provide_session(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
        ],
        *args: Any,
        **kwargs: Any,
    ) -> "Union[AbstractContextManager[DriverT], AbstractAsyncContextManager[DriverT]]":
        """Create and provide a database session from the specified configuration.

        Args:
            name: The configuration type to use for creating the session.
            *args: Positional arguments to pass to the configuration's provide_session method.
            **kwargs: Keyword arguments to pass to the configuration's provide_session method.

        Returns:
            Either a synchronous or asynchronous context manager that provides a database session.
        """
        config = self.get_config(name)
        return config.provide_session(*args, **kwargs)

    @overload
    def get_pool(
        self, name: "type[Union[NoPoolSyncConfig[ConnectionT, DriverT], NoPoolAsyncConfig[ConnectionT, DriverT]]]"
    ) -> "None": ...  # pyright: ignore[reportInvalidTypeVarUse]

    @overload
    def get_pool(self, name: "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]") -> "type[PoolT]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    @overload
    def get_pool(self, name: "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]") -> "Awaitable[type[PoolT]]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    def get_pool(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
        ],
    ) -> "Union[type[PoolT], Awaitable[type[PoolT]], None]":
        """Create and return a connection pool from the specified configuration.

        Args:
            name: The configuration type to use for creating the pool.

        Returns:
            Either a pool instance, an awaitable that resolves to a pool instance, or None
            if the configuration does not support connection pooling.
        """
        config = self.get_config(name)
        if config.support_connection_pooling:
            return cast("Union[type[PoolT], Awaitable[type[PoolT]]]", config.create_pool())
        return None

    def close_pool(
        self,
        name: Union[
            "type[NoPoolSyncConfig[ConnectionT, DriverT]]",
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
        ],
    ) -> "Optional[Awaitable[None]]":
        """Close the connection pool for the specified configuration.

        Args:
            name: The configuration type whose pool to close.

        Returns:
            An awaitable if the configuration is async, otherwise None.
        """
        config = self.get_config(name)
        if config.support_connection_pooling:
            return config.close_pool()
        return None


class CommonDriverAttributes(Generic[ConnectionT]):
    """Common attributes and methods for driver adapters."""

    dialect: str
    """The SQL dialect supported by the underlying database driver (e.g., 'postgres', 'mysql')."""
    connection: ConnectionT
    """The connection to the underlying database."""
    __supports_arrow__: ClassVar[bool] = False
    """Indicates if the driver supports Apache Arrow operations."""

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "ConnectionT":
        return connection if connection is not None else self.connection

    @staticmethod
    def check_not_found(item_or_none: Optional[T] = None) -> T:
        """Raise :exc:`sqlspec.exceptions.NotFoundError` if ``item_or_none`` is ``None``.

        Args:
            item_or_none: Item to be tested for existence.

        Raises:
            NotFoundError: If ``item_or_none`` is ``None``

        Returns:
            The item, if it exists.
        """
        if item_or_none is None:
            msg = "No result found when one was expected"
            raise NotFoundError(msg)
        return item_or_none

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None, /, **kwargs: Any
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters using SQLStatement for validation and formatting.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            A tuple containing the processed SQL query and parameters.
        """
        # Instantiate SQLStatement with parameters and kwargs for internal merging
        stmt = SQLStatement(sql=sql, parameters=parameters, dialect=self.dialect, kwargs=kwargs or None)
        # Process uses the merged parameters internally
        return stmt.process()


class SyncArrowBulkOperationsMixin(Generic[ConnectionT]):
    """Mixin for sync drivers supporting bulk Apache Arrow operations."""

    __supports_arrow__: "ClassVar[bool]" = True

    @abstractmethod
    def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        raise NotImplementedError


class SyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT], ABC, Generic[ConnectionT]):
    connection: "ConnectionT"

    def __init__(self, connection: "ConnectionT", **kwargs: Any) -> None:
        self.connection = connection

    @abstractmethod
    def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    def select_one(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]": ...

    @abstractmethod
    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    def select_value(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[T]] = None,
        **kwargs: Any,
    ) -> "Union[Any, T]": ...

    @abstractmethod
    def select_value_or_none(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[T]] = None,
        **kwargs: Any,
    ) -> "Optional[Union[Any, T]]": ...

    @abstractmethod
    def insert_update_delete(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any,
    ) -> int: ...

    @abstractmethod
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]": ...

    @abstractmethod
    def execute_script(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        *,
        connection: Optional[ConnectionT] = None,
        **kwargs: Any,
    ) -> str: ...


class AsyncArrowBulkOperationsMixin(Generic[ConnectionT]):
    """Mixin for async drivers supporting bulk Apache Arrow operations."""

    __supports_arrow__: "ClassVar[bool]" = True

    @abstractmethod
    async def select_arrow(  # pyright: ignore[reportUnknownParameterType]
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> "ArrowTable":  # pyright: ignore[reportUnknownReturnType]
        """Execute a SQL query and return results as an Apache Arrow Table.

        Args:
            sql: The SQL query string.
            parameters: Parameters for the query.
            connection: Optional connection override.
            **kwargs: Additional keyword arguments to merge with parameters if parameters is a dict.

        Returns:
            An Apache Arrow Table containing the query results.
        """
        raise NotImplementedError


class AsyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT], ABC, Generic[ConnectionT]):
    connection: "ConnectionT"

    def __init__(self, connection: "ConnectionT") -> None:
        self.connection = connection

    @abstractmethod
    async def select(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    async def select_one(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[ModelDTOT, dict[str, Any]]": ...

    @abstractmethod
    async def select_one_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    async def select_value(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[Any, T]": ...

    @abstractmethod
    async def select_value_or_none(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[Any, T]]": ...

    @abstractmethod
    async def insert_update_delete(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> int: ...

    @abstractmethod
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]": ...

    @abstractmethod
    async def execute_script(
        self,
        sql: str,
        parameters: "Optional[StatementParameterType]" = None,
        /,
        *,
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> str: ...


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol[ConnectionT], AsyncDriverAdapterProtocol[ConnectionT]]
