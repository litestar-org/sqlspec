# ruff: noqa: PLR6301
import re
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Generator
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass, field
from typing import (
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
from sqlspec.typing import ModelDTOT, StatementParameterType

__all__ = (
    "AsyncDatabaseConfig",
    "DatabaseConfigProtocol",
    "GenericPoolConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SyncDatabaseConfig",
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

# Regex to find :param style placeholders, avoiding those inside quotes
# Handles basic cases, might need refinement for complex SQL
PARAM_REGEX = re.compile(
    r"(?P<dquote>\"(?:[^\"]|\"\")*\")|"  # Double-quoted strings
    r"(?P<squote>'(?:[^']|'')*')|"  # Single-quoted strings
    r"(?P<lead>[^:]):(?P<var_name>[a-zA-Z_][a-zA-Z0-9_]*)"  # :param placeholder
)


@dataclass
class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    connection_type: "type[ConnectionT]" = field(init=False)
    driver_type: "type[DriverT]" = field(init=False)
    pool_instance: "Optional[PoolT]" = field(default=None)
    __is_async__: ClassVar[bool] = False
    __supports_connection_pooling__: ClassVar[bool] = False

    def __hash__(self) -> int:
        return id(self)

    @abstractmethod
    def create_connection(self) -> Union[ConnectionT, Awaitable[ConnectionT]]:
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Union[
        Generator[ConnectionT, None, None],
        AsyncGenerator[ConnectionT, None],
        AbstractContextManager[ConnectionT],
        AbstractAsyncContextManager[ConnectionT],
    ]:
        """Provide a database connection context manager."""
        raise NotImplementedError

    @property
    @abstractmethod
    def connection_config_dict(self) -> dict[str, Any]:
        """Return the connection configuration as a dict."""
        raise NotImplementedError

    @abstractmethod
    def create_pool(self) -> Union[PoolT, Awaitable[PoolT]]:
        """Create and return connection pool."""
        raise NotImplementedError

    @abstractmethod
    def close_pool(self) -> Optional[Awaitable[None]]:
        """Terminate the connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Union[PoolT, Awaitable[PoolT], AbstractContextManager[PoolT], AbstractAsyncContextManager[PoolT]]:
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
    def add_config(self, config: SyncConfigT) -> type[SyncConfigT]: ...

    @overload
    def add_config(self, config: AsyncConfigT) -> type[AsyncConfigT]: ...

    def add_config(
        self,
        config: Union[
            SyncConfigT,
            AsyncConfigT,
        ],
    ) -> Union[Annotated[type[SyncConfigT], int], Annotated[type[AsyncConfigT], int]]:  # pyright: ignore[reportInvalidTypeVarUse]
        """Add a new configuration to the manager.

        Returns:
            A unique type key that can be used to retrieve the configuration later.
        """
        key = Annotated[type(config), id(config)]  # type: ignore[valid-type]
        self._configs[key] = config
        return key  # type: ignore[return-value]  # pyright: ignore[reportReturnType]

    @overload
    def get_config(self, name: type[SyncConfigT]) -> SyncConfigT: ...

    @overload
    def get_config(self, name: type[AsyncConfigT]) -> AsyncConfigT: ...

    def get_config(
        self,
        name: Union[type[DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]], Any],
    ) -> DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]:
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
            type[NoPoolSyncConfig[ConnectionT, DriverT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]],  # pyright: ignore[reportInvalidTypeVarUse]
        ],
    ) -> ConnectionT: ...

    @overload
    def get_connection(
        self,
        name: Union[
            type[NoPoolAsyncConfig[ConnectionT, DriverT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]],  # pyright: ignore[reportInvalidTypeVarUse]
        ],
    ) -> Awaitable[ConnectionT]: ...

    def get_connection(
        self,
        name: Union[
            type[NoPoolSyncConfig[ConnectionT, DriverT]],
            type[NoPoolAsyncConfig[ConnectionT, DriverT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]],
        ],
    ) -> Union[ConnectionT, Awaitable[ConnectionT]]:
        """Create and return a connection from the specified configuration.

        Args:
            name: The configuration type to use for creating the connection.

        Returns:
            Either a connection instance or an awaitable that resolves to a connection,
            depending on whether the configuration is sync or async.
        """
        config = self.get_config(name)
        return config.create_connection()

    @overload
    def get_pool(
        self, name: type[Union[NoPoolSyncConfig[ConnectionT, DriverT], NoPoolAsyncConfig[ConnectionT, DriverT]]]
    ) -> None: ...  # pyright: ignore[reportInvalidTypeVarUse]

    @overload
    def get_pool(self, name: type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]) -> type[PoolT]: ...  # pyright: ignore[reportInvalidTypeVarUse]

    @overload
    def get_pool(self, name: type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]) -> Awaitable[type[PoolT]]: ...  # pyright: ignore[reportInvalidTypeVarUse]

    def get_pool(
        self,
        name: Union[
            type[NoPoolSyncConfig[ConnectionT, DriverT]],
            type[NoPoolAsyncConfig[ConnectionT, DriverT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]],
        ],
    ) -> Union[type[PoolT], Awaitable[type[PoolT]], None]:
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
            type[NoPoolSyncConfig[ConnectionT, DriverT]],
            type[NoPoolAsyncConfig[ConnectionT, DriverT]],
            type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]],
            type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]],
        ],
    ) -> Optional[Awaitable[None]]:
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

    param_style: str = "?"
    """The parameter style placeholder supported by the underlying database driver (e.g., '?', '%s')."""
    connection: ConnectionT
    """The connection to the underlying database."""

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

    def _process_sql_statement(self, sql: str) -> str:
        """Perform any preprocessing of the SQL query string if needed.
        Default implementation returns the SQL unchanged.

        Args:
            sql: The SQL query string.

        Returns:
            The processed SQL query string.
        """
        return sql

    def _process_sql_params(
        self, sql: str, parameters: "Optional[StatementParameterType]" = None
    ) -> "tuple[str, Optional[Union[tuple[Any, ...], list[Any], dict[str, Any]]]]":
        """Process SQL query and parameters for DB-API execution.

        Converts named parameters (:name) to positional parameters specified by `self.param_style`
        if the input parameters are a dictionary.

        Args:
            sql: The SQL query string.
            parameters: The parameters for the query (dict, tuple, list, or None).

        Returns:
            A tuple containing the processed SQL string and the processed parameters
            (always a tuple or None if the input was a dictionary, otherwise the original type).

        Raises:
            ValueError: If a named parameter in the SQL is not found in the dictionary
                        or if a parameter in the dictionary is not used in the SQL.
        """
        if not isinstance(parameters, dict) or not parameters:
            # If parameters are not a dict, or empty dict, assume positional/no params
            # Let the underlying driver handle tuples/lists directly
            return self._process_sql_statement(sql), parameters

        processed_sql = ""
        processed_params_list: list[Any] = []
        last_end = 0
        found_params: set[str] = set()

        for match in PARAM_REGEX.finditer(sql):
            if match.group("dquote") is not None or match.group("squote") is not None:
                # Skip placeholders within quotes
                continue

            var_name = match.group("var_name")
            if var_name is None:  # Should not happen with the regex, but safeguard
                continue

            if var_name not in parameters:
                msg = f"Named parameter ':{var_name}' found in SQL but not provided in parameters dictionary."
                raise ValueError(msg)

            # Append segment before the placeholder + the leading character + the driver's positional placeholder
            # The match.start("var_name") -1 includes the character before the ':'
            processed_sql += sql[last_end : match.start("var_name")] + self.param_style
            processed_params_list.append(parameters[var_name])
            found_params.add(var_name)
            last_end = match.end("var_name")

        # Append the rest of the SQL string
        processed_sql += sql[last_end:]

        # Check if all provided parameters were used
        unused_params = set(parameters.keys()) - found_params
        if unused_params:
            msg = f"Parameters provided but not found in SQL: {unused_params}"
            # Depending on desired strictness, this could be a warning or an error
            # For now, let's raise an error for clarity
            raise ValueError(msg)

        processed_params = tuple(processed_params_list)
        # Pass the processed SQL through the driver-specific processor if needed
        final_sql = self._process_sql_statement(processed_sql)
        return final_sql, processed_params


class SyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT], ABC, Generic[ConnectionT]):
    connection: ConnectionT

    def __init__(self, connection: ConnectionT) -> None:
        self.connection = connection

    @abstractmethod
    def select(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    def select_one(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]": ...

    @abstractmethod
    def select_one_or_none(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    def select_value(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[T]] = None,
    ) -> "Union[Any, T]": ...

    @abstractmethod
    def select_value_or_none(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[T]] = None,
    ) -> "Optional[Union[Any, T]]": ...

    @abstractmethod
    def insert_update_delete(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
    ) -> int: ...

    @abstractmethod
    def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]": ...

    @abstractmethod
    def execute_script(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
    ) -> str: ...


class AsyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT], ABC, Generic[ConnectionT]):
    connection: ConnectionT

    def __init__(self, connection: ConnectionT) -> None:
        self.connection = connection

    @abstractmethod
    async def select(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "list[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    async def select_one(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "Union[ModelDTOT, dict[str, Any]]": ...

    @abstractmethod
    async def select_one_or_none(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "Optional[Union[ModelDTOT, dict[str, Any]]]": ...

    @abstractmethod
    async def select_value(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[T]] = None,
    ) -> "Union[Any, T]": ...

    @abstractmethod
    async def select_value_or_none(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[T]] = None,
    ) -> "Optional[Union[Any, T]]": ...

    @abstractmethod
    async def insert_update_delete(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
    ) -> int: ...

    @abstractmethod
    async def insert_update_delete_returning(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
        schema_type: Optional[type[ModelDTOT]] = None,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]": ...

    @abstractmethod
    async def execute_script(
        self,
        sql: str,
        parameters: Optional[StatementParameterType] = None,
        /,
        connection: Optional[ConnectionT] = None,
    ) -> str: ...


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol[ConnectionT], AsyncDriverAdapterProtocol[ConnectionT]]
