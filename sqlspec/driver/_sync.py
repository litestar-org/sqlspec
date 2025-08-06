"""Synchronous driver protocol implementation."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from sqlspec.driver._common import CommonDriverAttributesMixin, ExecutionResult
from sqlspec.driver.mixins import SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.sql import SQL
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractContextManager

    from sqlspec.builder import QueryBuilder
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import Statement, StatementConfig
    from sqlspec.typing import ModelDTOT, ModelT, RowT, StatementParameters

logger = get_logger("sqlspec")

__all__ = ("SyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class SyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin):
    __slots__ = ()

    def dispatch_statement_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        This method orchestrates the common execution flow, delegating
        database-specific steps to abstract methods that concrete adapters
        must implement.

        All database operations are wrapped in the mandatory exception handler,
        ensuring consistent error handling across all drivers.

        Args:
            statement: The SQL statement to execute.
            connection: The database connection to use.

        Returns:
            The result of the SQL execution.
        """
        with self.handle_database_exceptions(), self.with_cursor(connection) as cursor:
            statement._ensure_processed()

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

        This method should return a context manager that yields a cursor.
        For sync drivers, this is typically implemented using @contextmanager
        or a custom context manager class.
        """

    @abstractmethod
    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """MANDATORY: Handle database-specific exceptions and wrap them appropriately.

        This context manager is the ONLY place where exceptions should be caught
        and wrapped in SQLSpec exceptions. All other layers (SQLTransformer,
        statement processing, etc.) should let exceptions bubble up naturally.

        Each driver MUST implement this to handle their specific database exceptions:
        - SQLGlot ParseError -> SQLSpecParseError
        - Database connection errors -> SQLSpecConnectionError
        - Constraint violations -> SQLSpecIntegrityError
        - Driver-specific errors -> SQLSpecDatabaseError

        Example implementation:
            @contextmanager
            def handle_database_exceptions(self):
                try:
                    yield
                except sqlglot.ParseError as e:
                    raise SQLSpecParseError(f"SQL parsing failed: {e}") from e
                except psycopg.Error as e:
                    raise SQLSpecDatabaseError(f"Database error: {e}") from e

        Returns:
            A context manager that wraps database operations with proper exception handling
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
        """Execute a SQL script (multiple statements).

        Default implementation splits script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration

        Returns:
            ExecutionResult with script execution data including statement counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        for stmt in statements:
            # Create individual statement for each script part
            single_stmt = statement.copy(statement=stmt, parameters=prepared_parameters)
            self._execute_statement(cursor, single_stmt)

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
        )

    @abstractmethod
    def _execute_many(self, cursor: Any, statement: "SQL") -> ExecutionResult:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement object with all necessary data and configuration
                      Use statement.sql and statement.parameters for driver data

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
                      Use statement.sql and statement.parameters for driver data

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

        # Create SQL object with is_many=True and execute_many parameters
        if isinstance(statement, SQL):
            # If already a SQL object, create a new one with is_many=True and the execute_many parameters
            sql_statement = SQL(statement._raw_sql, parameters, statement_config=config, is_many=True, **kwargs)
        else:
            # Create new SQL object directly with is_many=True (handles str, QueryBuilder via prepare_statement logic)
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
        script_config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=script_config, kwargs=kwargs)

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
    ) -> "Union[ModelT, RowT, dict[str, Any]]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelT, RowT, ModelDTOT]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
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
    ) -> "Optional[ModelT]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    def select_one_or_none(
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
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        data = result.get_data()
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        return cast("Optional[Union[ModelT, ModelDTOT]]", self.to_schema(data[0], schema_type=schema_type))

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
    ) -> "list[ModelT]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "Union[list[ModelT], list[ModelDTOT]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return all rows."""
        result = self.execute(statement, *parameters, statement_config=statement_config, **kwargs)
        return cast(
            "Union[list[ModelT], list[ModelDTOT]]",
            self.to_schema(cast("list[ModelT]", result.get_data()), schema_type=schema_type),
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

        Example:
            >>> data, total = driver.select_with_total(
            ...     "SELECT * FROM users WHERE active = ? LIMIT 10 OFFSET 20",
            ...     True,
            ... )
            >>> print(f"Page data: {len(data)} rows, Total: {total} rows")
        """
        sql_statement = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        count_result = self.dispatch_statement_execution(self._create_count_query(sql_statement), self.connection)
        select_result = self.execute(sql_statement)

        return (self.to_schema(select_result.get_data(), schema_type=schema_type), count_result.scalar())
