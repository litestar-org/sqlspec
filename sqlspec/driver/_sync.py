"""Synchronous driver protocol implementation.

This module provides the sync driver infrastructure for database adapters,
including connection management, transaction support, and result processing.
"""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Final, NoReturn, Optional, Union, cast, overload

from sqlspec.core import SQL
from sqlspec.driver._common import CommonDriverAttributesMixin, ExecutionResult
from sqlspec.driver.mixins import SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractContextManager

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQLResult, Statement, StatementConfig, StatementFilter
    from sqlspec.typing import ModelDTOT, StatementParameters

_LOGGER_NAME: Final[str] = "sqlspec"
logger = get_logger(_LOGGER_NAME)

__all__ = ("SyncDriverAdapterBase",)


EMPTY_FILTERS: Final["list[StatementFilter]"] = []


class SyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin):
    """Base class for synchronous database drivers.

    Provides the foundation for sync database adapters, including connection management,
    transaction support, and SQL execution methods. All database operations are performed
    synchronously and support context manager patterns for proper resource cleanup.
    """

    __slots__ = ()

    def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        Orchestrates the common execution flow, delegating database-specific steps
        to abstract methods that concrete adapters must implement.
        All database operations are wrapped in exception handling.

        Args:
            statement: The SQL statement to execute
            connection: The database connection to use

        Returns:
            The result of the SQL execution
        """
        with self.handle_database_exceptions(), self.with_cursor(connection) as cursor:
            special_result = self._try_special_handling(cursor, statement)
            if special_result is not None:
                return special_result

            if statement.is_script:
                execution_result = self._execute_script(cursor, statement)
            elif statement.is_many:
                execution_result = self._execute_many(cursor, statement)
            else:
                execution_result = self._execute_statement(cursor, statement)

            return self.build_statement_result(statement, execution_result)

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return a context manager for cursor acquisition and cleanup.

        Returns a context manager that yields a cursor for database operations.
        Concrete implementations handle database-specific cursor creation and cleanup.
        """

    @abstractmethod
    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            ContextManager that can be used in with statements
        """

    @abstractmethod
    def begin(self) -> None:
        """Begin a database transaction on the current connection."""

    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""

    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction on the current connection."""

    @abstractmethod
    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
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

    def _execute_script(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a SQL script containing multiple statements.

        Default implementation splits the script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with script execution data including statement counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        statement_count: int = len(statements)
        successful_count: int = 0

        for stmt in statements:
            single_stmt = statement.copy(statement=stmt, parameters=prepared_parameters)
            self._execute_statement(cursor, single_stmt)
            successful_count += 1

        return self.create_execution_result(
            cursor, statement_count=statement_count, successful_statements=successful_count, is_script_result=True
        )

    @abstractmethod
    def _execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data for the many operation
        """

    @abstractmethod
    def _execute_statement(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with execution data
        """

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with parameter handling."""
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        return self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    def execute_many(
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
        config = statement_config or self.statement_config

        if isinstance(statement, SQL):
            sql_statement = SQL(statement._raw_sql, parameters, statement_config=config, is_many=True, **kwargs)
        else:
            base_statement = self.prepare_statement(statement, filters, statement_config=config, kwargs=kwargs)
            sql_statement = SQL(base_statement._raw_sql, parameters, statement_config=config, is_many=True, **kwargs)

        return self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)

    def execute_script(
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
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)

        return self.dispatch_statement_execution(statement=sql_statement.as_script(), connection=self.connection)

    @overload
    def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...

    def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
        data_len: int = len(data)
        if data_len == 0:
            self._raise_no_rows_found()
        if data_len > 1:
            self._raise_expected_one_row(data_len)
        first_row = data[0]
        return self.to_schema(first_row, schema_type=schema_type) if schema_type else first_row

    @overload
    def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...

    def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
        data_len: int = len(data)
        if data_len == 0:
            return None
        if data_len > 1:
            self._raise_expected_at_most_one_row(data_len)
        first_row = data[0]
        return cast(
            "Optional[Union[dict[str, Any], ModelDTOT]]",
            self.to_schema(first_row, schema_type=schema_type) if schema_type else first_row,
        )

    @overload
    def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...

    def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[list[dict[str, Any]], list[ModelDTOT]]":
        """Execute a select statement and return all rows."""
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return cast(
            "Union[list[dict[str, Any]], list[ModelDTOT]]", self.to_schema(result.get_data(), schema_type=schema_type)
        )

    def select_value(
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
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        try:
            row = result.one()
        except ValueError as e:
            self._raise_no_rows_found_from_exception(e)
        if not row:
            self._raise_no_rows_found()
        if is_dict_row(row):
            if not row:
                self._raise_row_no_columns()
            return next(iter(row.values()))
        if is_indexable_row(row):
            if not row:
                self._raise_row_no_columns()
            return row[0]
        self._raise_unexpected_row_type(type(row))
        return None

    def select_value_or_none(
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
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
        data_len: int = len(data)
        if data_len == 0:
            return None
        if data_len > 1:
            msg = f"Expected at most one row, found {data_len}"
            raise ValueError(msg)
        row = data[0]
        if isinstance(row, dict):
            if not row:
                return None
            return next(iter(row.values()))
        if isinstance(row, (tuple, list)):
            return row[0]
        msg = f"Cannot extract value from row type {type(row).__name__}"
        raise TypeError(msg)

    @overload
    def select_with_total(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[list[ModelDTOT], int]": ...

    @overload
    def select_with_total(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...

    def select_with_total(
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
        """
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        count_result = self.dispatch_statement_execution(self._create_count_query(sql_statement), self.connection)
        select_result = self.execute(sql_statement)

        return (self.to_schema(select_result.get_data(), schema_type=schema_type), count_result.scalar())

    def _raise_no_rows_found(self) -> NoReturn:
        msg = "No rows found"
        raise NotFoundError(msg)

    def _raise_no_rows_found_from_exception(self, e: ValueError) -> NoReturn:
        msg = "No rows found"
        raise NotFoundError(msg) from e

    def _raise_expected_one_row(self, data_len: int) -> NoReturn:
        msg = f"Expected exactly one row, found {data_len}"
        raise ValueError(msg)

    def _raise_expected_at_most_one_row(self, data_len: int) -> NoReturn:
        msg = f"Expected at most one row, found {data_len}"
        raise ValueError(msg)

    def _raise_row_no_columns(self) -> NoReturn:
        msg = "Row has no columns"
        raise ValueError(msg)

    def _raise_unexpected_row_type(self, row_type: type) -> NoReturn:
        msg = f"Unexpected row type: {row_type}"
        raise ValueError(msg)

    def _raise_cannot_extract_value_from_row_type(self, type_name: str) -> NoReturn:
        msg = f"Cannot extract value from row type {type_name}"
        raise TypeError(msg)
