# ruff: noqa: PLR6301
import atexit
import contextlib
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Sequence
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

from sqlglot import exp

from sqlspec.exceptions import NotFoundError, SQLValidationError
from sqlspec.sql.filters import StatementFilter, apply_filter
from sqlspec.sql.statement import SQLStatement, Statement, StatementConfig
from sqlspec.typing import ConnectionT, PoolT, StatementParameterType, T
from sqlspec.utils.sync_tools import ensure_async_

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.sql.parameters import ParameterStyle
    from sqlspec.sql.result import StatementResult

StatementResultType = Union["StatementResult[dict[str, Any]]", "StatementResult[Any]"]


__all__ = (
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterProtocol",
    "CommonDriverAttributes",
    "DatabaseConfigProtocol",
    "GenericPoolConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SQLSpec",
    "SyncDatabaseConfig",
    "SyncDriverAdapterProtocol",
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
        # Register the cleanup handler to run at program exit
        atexit.register(self._cleanup_pools)

    def _cleanup_pools(self) -> None:
        """Clean up all open database pools at program exit."""
        for config in self._configs.values():
            if config.support_connection_pooling and config.pool_instance is not None:
                with contextlib.suppress(Exception):
                    ensure_async_(config.close_pool)()

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
            "NoPoolSyncConfig[ConnectionT, DriverT]",
            "SyncDatabaseConfig[ConnectionT, PoolT, DriverT]",
        ],
    ) -> "ConnectionT": ...

    @overload
    def get_connection(
        self,
        name: Union[
            "type[NoPoolAsyncConfig[ConnectionT, DriverT]]",
            "type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",  # pyright: ignore[reportInvalidTypeVarUse]
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
        """Create and return a new database connection from the specified configuration.

        Args:
            name: The configuration type to use for creating the connection.

        Returns:
            Either a connection instance or an awaitable that resolves to a connection instance.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
        else:
            config = self.get_config(name)
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
        """Create and return a new database session from the specified configuration.

        Args:
            name: The configuration type to use for creating the session.

        Returns:
            Either a driver instance or an awaitable that resolves to a driver instance.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
        else:
            config = self.get_config(name)
        connection = self.get_connection(name)
        if isinstance(connection, Awaitable):

            async def _create_session() -> DriverT:
                return cast("DriverT", config.driver_type(await connection))  # pyright: ignore

            return _create_session()
        return cast("DriverT", config.driver_type(connection))  # pyright: ignore

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
            name: The configuration type to use for creating the connection.
            *args: Positional arguments to pass to the configuration's provide_connection method.
            **kwargs: Keyword arguments to pass to the configuration's provide_connection method.

        Returns:
            Either a synchronous or asynchronous context manager that provides a database connection.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
        else:
            config = self.get_config(name)
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
            name: The configuration type to use for creating the session.
            *args: Positional arguments to pass to the configuration's provide_session method.
            **kwargs: Keyword arguments to pass to the configuration's provide_session method.

        Returns:
            Either a synchronous or asynchronous context manager that provides a database session.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
        else:
            config = self.get_config(name)
        return config.provide_session(*args, **kwargs)

    @overload
    def get_pool(
        self,
        name: "Union[type[Union[NoPoolSyncConfig[ConnectionT, DriverT], NoPoolAsyncConfig[ConnectionT, DriverT]]], NoPoolSyncConfig[ConnectionT, DriverT], NoPoolAsyncConfig[ConnectionT, DriverT]]",
    ) -> "None": ...  # pyright: ignore[reportInvalidTypeVarUse]
    @overload
    def get_pool(
        self,
        name: "Union[type[SyncDatabaseConfig[ConnectionT, PoolT, DriverT]], SyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
    ) -> "type[PoolT]": ...  # pyright: ignore[reportInvalidTypeVarUse]
    @overload
    def get_pool(
        self,
        name: "Union[type[AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]],AsyncDatabaseConfig[ConnectionT, PoolT, DriverT]]",
    ) -> "Awaitable[type[PoolT]]": ...  # pyright: ignore[reportInvalidTypeVarUse]

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
        """Create and return a connection pool from the specified configuration.

        Args:
            name: The configuration type to use for creating the pool.

        Returns:
            Either a pool instance, an awaitable that resolves to a pool instance, or None
            if the configuration does not support connection pooling.
        """
        config = (
            name
            if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig))
            else self.get_config(name)
        )
        if config.support_connection_pooling:
            return cast("Union[type[PoolT], Awaitable[type[PoolT]]]", config.create_pool())
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
            name: The configuration type whose pool to close.

        Returns:
            An awaitable if the configuration is async, otherwise None.
        """
        if isinstance(name, (NoPoolSyncConfig, NoPoolAsyncConfig, SyncDatabaseConfig, AsyncDatabaseConfig)):
            config = name
        else:
            config = self.get_config(name)
        if config.support_connection_pooling:
            return config.close_pool()
        return None


class CommonDriverAttributes(Generic[ConnectionT]):
    """Common attributes and methods for driver adapters."""

    dialect: str
    """The SQL dialect supported by the underlying database driver (e.g., 'postgres', 'mysql')."""
    parameter_style: "ParameterStyle"
    """The parameter style used by the driver (e.g., 'qmark', 'numeric', 'named')."""
    connection: "ConnectionT"
    """The connection to the underlying database."""
    statement_config: "StatementConfig" = field(default_factory=StatementConfig)
    """Configuration for SQL statements, including validation and sanitization settings."""

    __supports_arrow__: "ClassVar[bool]" = False
    """Indicates if the driver supports Apache Arrow operations."""

    @abstractmethod
    def _get_placeholder_style(self) -> "ParameterStyle":
        """Return the paramstyle expected by the driver (e.g., 'qmark', 'numeric', 'named')."""
        raise NotImplementedError

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "ConnectionT":
        return connection if connection is not None else self.connection

    @staticmethod
    def returns_rows(expression: "Optional[exp.Expression]") -> bool:
        """Determine if a SQL expression is expected to return rows.

        Args:
            expression: The parsed sqlglot expression to analyze

        Returns:
            True if the statement returns rows, False otherwise
        """
        if expression is None:
            return False
        if isinstance(expression, (exp.Select, exp.Show, exp.Describe, exp.Pragma, exp.With)):
            return True
        if isinstance(expression, exp.Command):
            return True
        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return bool(expression.find(exp.Returning))
        return False

    @staticmethod
    def check_not_found(item_or_none: "Optional[T]" = None) -> "T":
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
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        statement_config: Optional["StatementConfig"] = None,
        **kwargs: Any,
    ) -> "tuple[str, Union[dict[str, Any], list[Any]], SQLStatement]":
        """Process SQL query and parameters using the Query object for validation and formatting.

        Args:
            sql: The SQL query string or sqlglot Expression.
            parameters: Parameters for the query.
            *filters: Statement filters to apply.
            statement_config: Optional SQLStatementConfig instance. If provided, its attributes determine the processing behavior.
            **kwargs: Additional keyword arguments to merge with parameters for the Query object.

        Raises:
            SQLValidationError: If the SQL statement is not safe according to the validator's min_risk_to_raise.

        Returns:
            A tuple containing the processed SQL query string, a dictionary or list of parameters
            suitable for the adapter, and the SQLStatement object containing the parsed expression.
        """
        processed_parameters = parameters
        if kwargs:
            if processed_parameters is None:
                processed_parameters = kwargs
            elif isinstance(processed_parameters, dict):
                processed_parameters = {**processed_parameters, **kwargs}

        stmt = (
            SQLStatement(
                sql=sql,
                parameters=processed_parameters,
                dialect=getattr(self, "dialect", None),
                statement_config=statement_config or StatementConfig(),
            )
            if not isinstance(sql, SQLStatement)
            else sql
        )
        for filter_obj in filters:
            stmt = apply_filter(stmt, filter_obj)
        validation_result = stmt.validate()

        if (
            validation_result is not None
            and not validation_result.is_safe
            and stmt.config.validator.min_risk_to_raise is not None
            and validation_result.risk_level is not None
            and validation_result.risk_level.value >= stmt.config.validator.min_risk_to_raise.value
        ):
            error_msg = f"SQL validation failed with risk level {validation_result.risk_level}:\n"
            error_msg += "Issues:\n" + "\n".join([f"- {issue}" for issue in validation_result.issues or []])
            if validation_result.warnings:
                error_msg += "\nWarnings:\n" + "\n".join([f"- {warn}" for warn in validation_result.warnings])
            raise SQLValidationError(error_msg, stmt.get_sql(), validation_result.risk_level)

        placeholder_style = self._get_placeholder_style()
        rendered_sql = stmt.get_sql(placeholder_style=placeholder_style)
        rendered_params = stmt.get_parameters(style=placeholder_style)

        if isinstance(rendered_params, (list, dict)):
            return rendered_sql, rendered_params, stmt
        if rendered_params is None:
            from sqlspec.sql.parameters import ParameterStyle

            return (
                rendered_sql,
                (
                    []
                    if placeholder_style
                    in {ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.PYFORMAT_POSITIONAL}
                    else {}
                ),
                stmt,
            )

        return rendered_sql, [rendered_params], stmt


class SyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT], ABC, Generic[ConnectionT]):
    connection: "ConnectionT"

    def __init__(self, connection: "ConnectionT", statement_config: "Optional[StatementConfig]" = None) -> None:
        self.connection = connection
        self.statement_config = statement_config if statement_config is not None else self.statement_config

    @abstractmethod
    def execute(
        self,
        sql: "Statement",
        parameters: Optional["StatementParameterType"] = None,
        *filters: "StatementFilter",
        connection: Optional["ConnectionT"] = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "StatementResultType":
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need:
        - result.all() for all rows
        - result.one() for exactly one row
        - result.one_or_none() for zero or one row
        - result.scalar() for a single value
        - result.affected_rows for INSERT/UPDATE/DELETE count

        For Arrow operations, drivers may return ArrowResult containing Apache Arrow tables.

        Example usage:
            # Regular operation returning SelectResult
            result = driver.execute("SELECT * FROM users WHERE id = ?", [1])
            user = result.one()

            # Arrow operation (if supported by driver)
            result = driver.execute("SELECT * FROM large_table", format="arrow")
            if isinstance(result, ArrowResult):
                df = result.to_pandas()
        """
        ...

    @abstractmethod
    def execute_many(
        self,
        sql: "Statement",
        parameters: "Optional[Sequence[StatementParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> StatementResultType:
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.
        """
        ...

    @abstractmethod
    def execute_script(
        self,
        sql: "Statement",
        parameters: Optional[StatementParameterType] = None,
        *filters: "StatementFilter",
        connection: Optional[ConnectionT] = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        Returns a string with execution results/output.
        """
        ...


class AsyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT], ABC, Generic[ConnectionT]):
    connection: "ConnectionT"

    def __init__(self, connection: "ConnectionT", statement_config: Optional[StatementConfig] = None) -> None:
        self.connection = connection
        self.statement_config = statement_config if statement_config is not None else self.statement_config

    @abstractmethod
    async def execute(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> StatementResultType:
        """Execute a SQL statement and return a StatementResult.

        This is the unified method for all SQL operations (SELECT, INSERT, UPDATE, DELETE).
        Use the StatementResult methods to extract the data you need:
        - result.all() for all rows
        - result.one() for exactly one row
        - result.one_or_none() for zero or one row
        - result.scalar() for a single value
        - result.affected_rows for INSERT/UPDATE/DELETE count

        For Arrow operations, drivers may return ArrowResult containing Apache Arrow tables.
        """
        ...

    @abstractmethod
    async def execute_many(
        self,
        sql: "Statement",
        parameters: Optional[Sequence[StatementParameterType]] = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> StatementResultType:
        """Execute a SQL statement with multiple parameter sets.

        Useful for batch INSERT, UPDATE, or DELETE operations.
        """
        ...

    @abstractmethod
    async def execute_script(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> str:
        """Execute a multi-statement SQL script.

        Returns a string with execution results/output.
        """
        ...


DriverAdapterProtocol = Union[SyncDriverAdapterProtocol[ConnectionT], AsyncDriverAdapterProtocol[ConnectionT]]
