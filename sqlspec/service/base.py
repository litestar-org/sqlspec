# mypy: disable-error-code="arg-type,misc,type-var"
# pyright: reportCallIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union, overload

from sqlspec.typing import ConnectionT

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.statement import SQLConfig, Statement, StatementFilter
    from sqlspec.statement.builder import DeleteBuilder, InsertBuilder, QueryBuilder, SelectBuilder, UpdateBuilder
    from sqlspec.statement.sql import SQL
    from sqlspec.typing import ModelDTOT, RowT, StatementParameters

__all__ = ("SQLSpecAsyncService", "SQLSpecSyncService")


T = TypeVar("T")
SyncDriverT = TypeVar("SyncDriverT", bound="SyncDriverAdapterProtocol[Any]")
AsyncDriverT = TypeVar("AsyncDriverT", bound="AsyncDriverAdapterProtocol[Any]")


class SQLSpecSyncService(Generic[SyncDriverT, ConnectionT]):
    """Sync Service for database operations."""

    def __init__(self, driver: "SyncDriverT", connection: "ConnectionT") -> None:
        self._driver = driver
        self._connection = connection

    @classmethod
    def new(cls, driver: "SyncDriverT", connection: "ConnectionT") -> "SQLSpecSyncService[SyncDriverT, ConnectionT]":
        return cls(driver=driver, connection=connection)

    @property
    def driver(self) -> "SyncDriverT":
        """Get the driver instance."""
        return self._driver

    @property
    def connection(self) -> "ConnectionT":
        """Get the connection instance."""
        return self._connection

    @overload
    def execute(
        self,
        statement: "SelectBuilder",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    def execute(
        self,
        statement: "SelectBuilder",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    @overload
    def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    @overload
    def execute(
        self,
        statement: "Union[str, SQL]",  # exp.Expression
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    def execute(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    def execute(
        self,
        statement: "Union[Statement, QueryBuilder[Any]]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a statement and return the result."""
        result = self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        return result.get_data()

    def execute_many(
        self,
        statement: "Union[Statement, QueryBuilder[Any]]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a statement multiple times and return the result."""
        result = self.driver.execute_many(statement, *parameters, _connection=_connection, _config=_config, **kwargs)
        return result.get_data()

    def execute_script(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a script statement."""
        result = self.driver.execute_script(statement, *parameters, _connection=_connection, _config=_config, **kwargs)
        return result.get_data()

    @overload
    def select_one(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    def select_one(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "RowT": ...

    def select_one(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            msg = "No rows found"
            raise ValueError(msg)
        if len(data) > 1:
            msg = f"Expected exactly one row, found {len(data)}"
            raise ValueError(msg)
        return data[0]

    @overload
    def select_one_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    def select_one_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[RowT]": ...

    def select_one_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        return data[0]

    @overload
    def select(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    def select(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    def select(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return all rows."""
        result = self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        return data

    def select_value(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.
        """
        result = self.driver.execute(
            statement, *parameters, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            msg = "No rows found"
            raise ValueError(msg)
        if len(data) > 1:
            msg = f"Expected exactly one row, found {len(data)}"
            raise ValueError(msg)
        row = data[0]
        # Extract the first column value
        if isinstance(row, dict):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            # Get the first value from the dict
            return next(iter(row.values()))
        if hasattr(row, "__getitem__"):
            # Tuple or list-like row
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)

    def select_value_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.
        """
        result = self.driver.execute(
            statement, *parameters, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        row = data[0]
        # Extract the first column value
        if isinstance(row, dict):
            if not row:
                return None
            # Get the first value from the dict
            return next(iter(row.values()))
        if hasattr(row, "__getitem__"):
            # Tuple or list-like row
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)


class SQLSpecAsyncService(Generic[AsyncDriverT, ConnectionT]):
    """Async Service for database operations."""

    def __init__(self, driver: "AsyncDriverT", connection: "ConnectionT") -> None:
        self._driver = driver
        self._connection = connection

    @classmethod
    def new(cls, driver: "AsyncDriverT", connection: "ConnectionT") -> "SQLSpecAsyncService[AsyncDriverT, ConnectionT]":
        return cls(driver=driver, connection=connection)

    @property
    def driver(self) -> "AsyncDriverT":
        """Get the driver instance."""
        return self._driver

    @property
    def connection(self) -> "ConnectionT":
        """Get the connection instance."""
        return self._connection

    @overload
    async def execute(
        self,
        statement: "SelectBuilder",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    async def execute(
        self,
        statement: "SelectBuilder",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[str, SQL]",  # exp.Expression
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    async def execute(
        self,
        statement: "Union[Statement, QueryBuilder[Any]]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a statement and return the result."""
        result = await self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        return result.get_data()

    async def execute_many(
        self,
        statement: "Union[Statement, QueryBuilder[Any]]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a statement multiple times and return the result."""
        result = await self.driver.execute_many(
            statement, *parameters, _connection=_connection, _config=_config, **kwargs
        )
        return result.get_data()

    async def execute_script(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a script statement."""
        result = await self.driver.execute_script(
            statement, *parameters, _connection=_connection, _config=_config, **kwargs
        )
        return result.get_data()

    @overload
    async def select_one(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    async def select_one(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "RowT": ...

    async def select_one(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = await self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            msg = "No rows found"
            raise ValueError(msg)
        if len(data) > 1:
            msg = f"Expected exactly one row, found {len(data)}"
            raise ValueError(msg)
        return data[0]

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[RowT]": ...

    async def select_one_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = await self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        return data[0]

    @overload
    async def select(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    async def select(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    async def select(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return all rows."""
        result = await self.driver.execute(
            statement, *parameters, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        return data

    async def select_value(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.
        """
        result = await self.driver.execute(
            statement, *parameters, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            msg = "No rows found"
            raise ValueError(msg)
        if len(data) > 1:
            msg = f"Expected exactly one row, found {len(data)}"
            raise ValueError(msg)
        row = data[0]
        # Extract the first column value
        if isinstance(row, dict):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            # Get the first value from the dict
            return next(iter(row.values()))
        if hasattr(row, "__getitem__"):
            # Tuple or list-like row
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)

    async def select_value_or_none(
        self,
        statement: "Union[Statement, SelectBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.
        """
        result = await self.driver.execute(
            statement, *parameters, _connection=_connection, _config=_config, **kwargs
        )
        data = result.get_data()
        # For select operations, data should be a list
        if not isinstance(data, list):
            msg = "Expected list result from select operation"
            raise TypeError(msg)
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        row = data[0]
        # Extract the first column value
        if isinstance(row, dict):
            if not row:
                return None
            # Get the first value from the dict
            return next(iter(row.values()))
        if hasattr(row, "__getitem__"):
            # Tuple or list-like row
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)
