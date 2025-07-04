# mypy: disable-error-code="arg-type,misc,type-var"
# pyright: reportCallIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar, Union, overload

from sqlglot import exp, parse_one

from sqlspec.typing import ConnectionT
from sqlspec.utils.type_guards import (
    is_dict_row,
    is_indexable_row,
    is_limit_offset_filter,
    is_select_builder,
    is_statement_filter,
)

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
    from sqlspec.service.pagination import OffsetPagination
    from sqlspec.statement import SQLConfig, Statement, StatementFilter
    from sqlspec.statement.builder import Delete, Insert, QueryBuilder, Select, Update
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

    def _normalize_statement(
        self,
        statement: "Union[Statement, Select]",
        params: "Optional[dict[str, Any]]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Normalize a statement of any supported type into a SQL object.

        Args:
            statement: The statement to normalize (str, Expression, SQL, or Select)
            params: Optional parameters (ignored for Select and SQL objects)
            config: Optional SQL configuration

        Returns:
            A normalized SQL object
        """
        from sqlspec.statement.sql import SQL

        if is_select_builder(statement):
            # Select has its own parameters via build(), ignore external params
            safe_query = statement.build()
            return SQL(safe_query.sql, parameters=safe_query.parameters, config=config)

        if isinstance(statement, SQL):
            # SQL object is already complete, ignore external params
            return statement

        if isinstance(statement, (str, exp.Expression)):
            return SQL(statement, parameters=params, config=config)

        # Fallback for type safety
        msg = f"Unsupported statement type: {type(statement).__name__}"
        raise TypeError(msg)

    @overload
    def execute(
        self,
        statement: "Select",
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
        statement: "Select",
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
        statement: "Union[Insert, Update, Delete]",
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
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "RowT": ...

    def select_one(
        self,
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[RowT]": ...

    def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    def select(
        self,
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
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
        result = self.driver.execute(statement, *parameters, _connection=_connection, _config=_config, **kwargs)
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
        if is_dict_row(row):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            return next(iter(row.values()))
        if is_indexable_row(row):
            # Tuple or list-like row
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)

    def select_value_or_none(
        self,
        statement: "Union[Statement, Select]",
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
        result = self.driver.execute(statement, *parameters, _connection=_connection, _config=_config, **kwargs)
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
        if isinstance(row, dict):
            if not row:
                return None
            return next(iter(row.values()))
        if isinstance(row, (tuple, list)):
            # Tuple or list-like row
            return row[0]
        try:
            return row[0]
        except (TypeError, IndexError) as e:
            msg = f"Cannot extract value from row type {type(row).__name__}: {e}"
            raise TypeError(msg) from e

    @overload
    def paginate(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "OffsetPagination[ModelDTOT]": ...

    @overload
    def paginate(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "OffsetPagination[RowT]": ...

    def paginate(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a paginated query with automatic counting.

        This method performs two queries:
        1. A count query to get the total number of results
        2. A data query with limit/offset applied

        Pagination can be specified either via LimitOffsetFilter in parameters
        or via 'limit' and 'offset' in kwargs.

        Args:
            statement: The SELECT statement to paginate
            *parameters: Statement parameters and filters (can include LimitOffsetFilter)
            schema_type: Optional model type for automatic schema conversion
            _connection: Optional connection to use
            _config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments. Can include 'limit' and 'offset'
                      if LimitOffsetFilter is not provided

        Returns:
            OffsetPagination object containing items, limit, offset, and total count

        Raises:
            ValueError: If neither LimitOffsetFilter nor limit/offset kwargs are provided

        Example:
            >>> # Using LimitOffsetFilter (recommended)
            >>> from sqlspec.statement.filters import LimitOffsetFilter
            >>> result = service.paginate(
            ...     sql.select("*").from_("users"),
            ...     LimitOffsetFilter(limit=10, offset=20),
            ... )
            >>> print(
            ...     f"Showing {len(result.items)} of {result.total} users"
            ... )

            >>> # Using kwargs (convenience)
            >>> result = service.paginate(
            ...     sql.select("*").from_("users"), limit=10, offset=20
            ... )

            >>> # With schema conversion
            >>> result = service.paginate(
            ...     sql.select("*").from_("users"),
            ...     LimitOffsetFilter(limit=10, offset=0),
            ...     schema_type=User,
            ... )
            >>> # result.items is list[User] with proper type inference

            >>> # With multiple filters
            >>> from sqlspec.statement.filters import (
            ...     LimitOffsetFilter,
            ...     OrderByFilter,
            ... )
            >>> result = service.paginate(
            ...     sql.select("*").from_("users"),
            ...     OrderByFilter("created_at", "desc"),
            ...     LimitOffsetFilter(limit=20, offset=40),
            ...     schema_type=User,
            ... )
        """
        from sqlspec.service.pagination import OffsetPagination
        from sqlspec.statement.sql import SQL

        # Separate filters from parameters
        filters: list[StatementFilter] = []
        params: list[Any] = []

        for p in parameters:
            # Use type guard to check if it implements the StatementFilter protocol
            if is_statement_filter(p):
                filters.append(p)
            else:
                params.append(p)

        # Check for LimitOffsetFilter in filters
        limit_offset_filter = None
        other_filters = []
        for f in filters:
            if is_limit_offset_filter(f):
                limit_offset_filter = f
            else:
                other_filters.append(f)

        if limit_offset_filter is not None:
            limit = limit_offset_filter.limit
            offset = limit_offset_filter.offset
        elif "limit" in kwargs and "offset" in kwargs:
            limit = kwargs.pop("limit")
            offset = kwargs.pop("offset")
        else:
            msg = "Pagination requires either a LimitOffsetFilter in parameters or 'limit' and 'offset' in kwargs."
            raise ValueError(msg)

        base_stmt = self._normalize_statement(statement, params, _config)

        filtered_stmt = base_stmt
        for filter_obj in other_filters:
            filtered_stmt = filter_obj.append_to_statement(filtered_stmt)

        sql_str = filtered_stmt.to_sql()

        # Parse and transform the AST to create a count query
        parsed = parse_one(sql_str)

        # Using exp.Subquery to properly wrap the parsed expression
        subquery = exp.Subquery(this=parsed, alias="_count_subquery")
        count_ast = exp.Select().select(exp.func("COUNT", exp.Star()).as_("total")).from_(subquery)

        count_stmt = SQL(count_ast.sql(), _config=_config)

        # Execute count query
        total = self.select_value(count_stmt, _connection=_connection, _config=_config, **kwargs)

        data_stmt = self._normalize_statement(statement, params, _config)

        for filter_obj in other_filters:
            data_stmt = filter_obj.append_to_statement(data_stmt)

        data_stmt = data_stmt.limit(limit).offset(offset)

        # Execute data query
        items = self.select(data_stmt, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs)

        return OffsetPagination(items=items, limit=limit, offset=offset, total=total)


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

    def _normalize_statement(
        self,
        statement: "Union[Statement, Select]",
        params: "Optional[dict[str, Any]]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Normalize a statement of any supported type into a SQL object.

        Args:
            statement: The statement to normalize (str, Expression, SQL, or Select)
            params: Optional parameters (ignored for Select and SQL objects)
            config: Optional SQL configuration

        Returns:
            A normalized SQL object
        """
        from sqlspec.statement.sql import SQL

        if is_select_builder(statement):
            # Select has its own parameters via build(), ignore external params
            safe_query = statement.build()
            return SQL(safe_query.sql, parameters=safe_query.parameters, config=config)

        if isinstance(statement, SQL):
            # SQL object is already complete, ignore external params
            return statement

        if isinstance(statement, (str, exp.Expression)):
            return SQL(statement, parameters=params, config=config)

        # Fallback for type safety
        msg = f"Unsupported statement type: {type(statement).__name__}"
        raise TypeError(msg)

    @overload
    async def execute(
        self,
        statement: "Select",
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
        statement: "Select",
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
        statement: "Union[Insert, Update, Delete]",
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
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "RowT": ...

    async def select_one(
        self,
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[RowT]": ...

    async def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[RowT]": ...

    async def select(
        self,
        statement: "Union[Statement, Select]",
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
        statement: "Union[Statement, Select]",
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
        result = await self.driver.execute(statement, *parameters, _connection=_connection, _config=_config, **kwargs)
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
        if is_dict_row(row):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            return next(iter(row.values()))
        if is_indexable_row(row):
            # Tuple or list-like row
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)

    async def select_value_or_none(
        self,
        statement: "Union[Statement, Select]",
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
        result = await self.driver.execute(statement, *parameters, _connection=_connection, _config=_config, **kwargs)
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
        if isinstance(row, dict):
            if not row:
                return None
            return next(iter(row.values()))
        if isinstance(row, (tuple, list)):
            # Tuple or list-like row
            return row[0]
        # Try indexing - if it fails, we'll get a proper error
        try:
            return row[0]
        except (TypeError, IndexError) as e:
            msg = f"Cannot extract value from row type {type(row).__name__}: {e}"
            raise TypeError(msg) from e

    @overload
    async def paginate(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "OffsetPagination[ModelDTOT]": ...

    @overload
    async def paginate(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "OffsetPagination[RowT]": ...

    async def paginate(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a paginated query with automatic counting.

        This method performs two queries:
        1. A count query to get the total number of results
        2. A data query with limit/offset applied

        Pagination can be specified either via LimitOffsetFilter in parameters
        or via 'limit' and 'offset' in kwargs.

        Args:
            statement: The SELECT statement to paginate
            *parameters: Statement parameters and filters (can include LimitOffsetFilter)
            schema_type: Optional model type for automatic schema conversion
            _connection: Optional connection to use
            _config: Optional SQL configuration
            **kwargs: Additional driver-specific arguments. Can include 'limit' and 'offset'
                      if LimitOffsetFilter is not provided

        Returns:
            OffsetPagination object containing items, limit, offset, and total count

        Raises:
            ValueError: If neither LimitOffsetFilter nor limit/offset kwargs are provided

        Example:
            >>> # Basic pagination
            >>> from sqlspec.statement.filters import LimitOffsetFilter
            >>> result = await service.paginate(
            ...     sql.select("*").from_("users"),
            ...     LimitOffsetFilter(limit=10, offset=20),
            ... )
            >>> print(
            ...     f"Showing {len(result.items)} of {result.total} users"
            ... )

            >>> # With schema conversion
            >>> result = await service.paginate(
            ...     sql.select("*").from_("users"),
            ...     LimitOffsetFilter(limit=10, offset=0),
            ...     schema_type=User,
            ... )
            >>> # result.items is list[User] with proper type inference

            >>> # With multiple filters
            >>> from sqlspec.statement.filters import (
            ...     LimitOffsetFilter,
            ...     OrderByFilter,
            ... )
            >>> result = await service.paginate(
            ...     sql.select("*").from_("users"),
            ...     OrderByFilter("created_at", "desc"),
            ...     LimitOffsetFilter(limit=20, offset=40),
            ...     schema_type=User,
            ... )
        """
        from sqlspec.service.pagination import OffsetPagination
        from sqlspec.statement.sql import SQL

        # Separate filters from parameters
        filters: list[StatementFilter] = []
        params: list[Any] = []

        for p in parameters:
            # Use type guard to check if it implements the StatementFilter protocol
            if is_statement_filter(p):
                filters.append(p)
            else:
                params.append(p)

        # Check for LimitOffsetFilter in filters
        limit_offset_filter = None
        other_filters = []
        for f in filters:
            if is_limit_offset_filter(f):
                limit_offset_filter = f
            else:
                other_filters.append(f)

        if limit_offset_filter is not None:
            limit = limit_offset_filter.limit
            offset = limit_offset_filter.offset
        elif "limit" in kwargs and "offset" in kwargs:
            limit = kwargs.pop("limit")
            offset = kwargs.pop("offset")
        else:
            msg = "Pagination requires either a LimitOffsetFilter in parameters or 'limit' and 'offset' in kwargs."
            raise ValueError(msg)

        base_stmt = self._normalize_statement(statement, params, _config)

        filtered_stmt = base_stmt
        for filter_obj in other_filters:
            filtered_stmt = filter_obj.append_to_statement(filtered_stmt)

        sql_str = filtered_stmt.to_sql()

        # Parse and transform the AST to create a count query
        parsed = parse_one(sql_str)

        # Using exp.Subquery to properly wrap the parsed expression
        subquery = exp.Subquery(this=parsed, alias="_count_subquery")
        count_ast = exp.Select().select(exp.func("COUNT", exp.Star()).as_("total")).from_(subquery)

        count_stmt = SQL(count_ast.sql(), _config=_config)

        # Execute count query
        total = await self.select_value(count_stmt, _connection=_connection, _config=_config, **kwargs)

        data_stmt = self._normalize_statement(statement, params, _config)

        for filter_obj in other_filters:
            data_stmt = filter_obj.append_to_statement(data_stmt)

        data_stmt = data_stmt.limit(limit).offset(offset)

        # Execute data query
        items = await self.select(
            data_stmt, schema_type=schema_type, _connection=_connection, _config=_config, **kwargs
        )

        return OffsetPagination(items=items, limit=limit, offset=offset, total=total)
