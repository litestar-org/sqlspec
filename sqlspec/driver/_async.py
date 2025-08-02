"""Asynchronous driver protocol implementation."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from sqlspec.driver._common import CommonDriverAttributesMixin, ExecutionResult
from sqlspec.driver.mixins import SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.builder import QueryBuilder
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, Statement, StatementConfig
    from sqlspec.typing import ModelDTOT, ModelT, RowT, StatementParameters

logger = get_logger("sqlspec")

__all__ = ("AsyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class AsyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin):
    __slots__ = ()

    async def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        This method orchestrates the common execution flow, delegating
        database-specific steps to abstract methods that concrete adapters
        must implement.

        Args:
            statement: The SQL statement to execute.
            connection: The database connection to use.

        Returns:
            The result of the SQL execution.
        """

        # Ensure statement is processed before special handling to make metadata available
        statement._ensure_processed()

        async with self.with_cursor(connection) as cursor:
            special_result = await self._try_special_handling(cursor, statement)
            if special_result is not None:
                return special_result

            sql, params = self._get_compiled_sql(statement, self.statement_config)

            # Single execution path - data handled in _execute_statement
            if statement.is_script:
                execution_result = await self._execute_script(cursor, sql, params, self.statement_config, statement)
            elif statement.is_many:
                # For execute_many, use param_list directly - parameters are ready to execute
                execution_result = await self._execute_many(cursor, sql, statement.param_list, statement)
            else:
                execution_result = await self._execute_statement(cursor, sql, params, statement)

            return self.build_statement_result(statement, execution_result)

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return an async context manager for cursor acquisition and cleanup.

        This method should return an async context manager that yields a cursor.
        For async drivers, this is typically implemented using a custom async
        context manager class.
        """

    @abstractmethod
    async def begin(self) -> None:
        """Begin a database transaction on the current connection."""

    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""

    @abstractmethod
    async def commit(self) -> None:
        """Commit the current transaction on the current connection."""

    @abstractmethod
    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for database-specific special operations (e.g., PostgreSQL COPY, bulk operations).

        This method is called first in dispatch_statement_execution() to allow drivers to handle
        special operations that don't follow the standard SQL execution pattern.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if the special operation was handled and completed,
            None if standard execution should proceed
        """

    async def _execute_script(
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig", statement: "SQL"
    ) -> ExecutionResult:
        """Execute a SQL script (multiple statements).

        Default implementation splits script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL script
            prepared_params: Prepared parameters
            statement_config: Statement configuration for dialect information
            statement: Original SQL statement object with metadata

        Returns:
            ExecutionResult with script execution data including statement counts
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)  # Script splitter already filters empty statements

        last_result = None
        for stmt in statements:
            last_result = await self._execute_statement(cursor, stmt, prepared_params, statement)

        # Row count will be provided by individual drivers in ExecutionResult

        return self.create_execution_result(
            last_result,
            statement_count=statement_count,
            successful_statements=statement_count,  # Assume all successful if no exception
            is_script_result=True,
        )

    @abstractmethod
    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL statement
            prepared_params: List of prepared parameter sets
            statement: Original SQL statement object with metadata

        Returns:
            ExecutionResult with execution data for the many operation

        Raises:
            NotImplementedError: Must be implemented by driver subclasses
        """

    @abstractmethod
    async def _execute_statement(
        self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL"
    ) -> ExecutionResult:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL statement
            prepared_params: Prepared parameters
            statement: Original SQL statement object with metadata

        Returns:
            ExecutionResult with execution data

        Raises:
            NotImplementedError: Must be implemented by driver subclasses
        """

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        # Parameter handling is delegated to SQL class constructor
        # which properly handles tuple expansion and parameter processing

        sql_statement = self.prepare_statement(
            statement, *parameters, statement_config=statement_config or self.statement_config, **kwargs
        )
        return await self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters.

        Parameters passed will be used as the batch execution sequence.
        """
        sql_statement = self.prepare_statement(
            statement, *filters, statement_config=statement_config or self.statement_config, **kwargs
        )
        return await self.dispatch_statement_execution(
            statement=sql_statement.as_many(parameters), connection=self.connection
        )

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script.

        By default, validates each statement and logs warnings for dangerous
        operations. Use suppress_warnings=True for migrations and admin scripts.
        """
        script_config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, *parameters, statement_config=script_config, **kwargs)

        return await self.dispatch_statement_execution(statement=sql_statement.as_script(), connection=self.connection)

    # Syntax Sugar Methods for Selecting Data Below:
    @overload
    async def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    async def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelT, RowT, dict[str, Any]]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    async def select_one(  # type: ignore[misc]
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelT, RowT,ModelDTOT]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
        if not data:
            msg = "No rows found"
            raise NotFoundError(msg)
        if len(data) > 1:
            msg = f"Expected exactly one row, found {len(data)}"
            raise ValueError(msg)
        return cast(
            "Union[ModelT, RowT, ModelDTOT]",
            self.to_schema(data[0], schema_type=schema_type) if schema_type else data[0],
        )

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelT]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    async def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelT, ModelDTOT]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        return cast("Optional[Union[ModelT, ModelDTOT]]", self.to_schema(data[0], schema_type=schema_type))

    @overload
    async def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    async def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelT]": ...  # pyright: ignore[reportInvalidTypeVarUse]
    async def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[list[ModelT], list[ModelDTOT]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return all rows."""
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return cast(
            "Union[list[ModelT], list[ModelDTOT]]",
            self.to_schema(cast("list[ModelT]", result.get_data()), schema_type=schema_type),  # type: ignore[arg-type]
        )

    async def select_value(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            row = result.one()
        except ValueError as e:
            msg = "No rows found"
            raise NotFoundError(msg) from e
        if not row:
            msg = "No rows found"
            raise NotFoundError(msg)
        if is_dict_row(row):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            return next(iter(row.values()))
        if is_indexable_row(row):
            if not row:
                msg = "Row has no columns"
                raise ValueError(msg)
            return row[0]
        msg = f"Unexpected row type: {type(row)}"
        raise ValueError(msg)

    async def select_value_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.
        """
        result = await self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
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
            return row[0]
        try:
            return row[0]
        except (TypeError, IndexError) as e:
            msg = f"Cannot extract value from row type {type(row).__name__}: {e}"
            raise TypeError(msg) from e

    @overload
    async def select_with_total(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[list[ModelDTOT], int]": ...

    @overload
    async def select_with_total(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...

    async def select_with_total(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[Union[list[dict[str, Any]], list[ModelDTOT]], int]":
        """Execute a select statement and return both the data and total count.

        This method is designed for pagination scenarios where you need both
        the current page of data and the total number of rows that match the query.

        Args:
            statement: The SQL statement, QueryBuilder, or raw SQL string
            *parameters: Parameters for the SQL statement
            schema_type: Optional schema type for data transformation
            statement_config: Optional SQL configuration
            **kwargs: Additional keyword arguments

        Returns:
            A tuple containing:
            - List of data rows (transformed by schema_type if provided)
            - Total count of rows matching the query (ignoring LIMIT/OFFSET)

        Example:
            >>> data, total = await driver.select_with_total(
            ...     "SELECT * FROM users WHERE active = ? LIMIT 10 OFFSET 20",
            ...     True,
            ... )
            >>> print(f"Page data: {len(data)} rows, Total: {total} rows")
        """
        # 1. Prepare original SQL statement
        sql_statement = self.prepare_statement(
            statement, *parameters, statement_config=statement_config or self.statement_config, **kwargs
        )
        count_result = await self.dispatch_statement_execution(self._create_count_query(sql_statement), self.connection)
        select_result = await self.execute(sql_statement)
        data = self.to_schema(select_result.get_data(), schema_type=schema_type)

        return (data, count_result.scalar())
