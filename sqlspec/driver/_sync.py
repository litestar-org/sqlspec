"""Synchronous driver protocol implementation."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from sqlglot import exp

from sqlspec.driver._common import CommonDriverAttributesMixin, create_execution_result
from sqlspec.driver.mixins import SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import ImproperConfigurationError, NotFoundError
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, Statement, StatementConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, ModelT, RowT, StatementParameters

logger = get_logger("sqlspec")

__all__ = ("SyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class SyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin):
    __slots__ = ()

    def _dispatch_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        This method orchestrates the common execution flow, delegating
        database-specific steps to abstract methods that concrete adapters
        must implement.

        The enhanced pattern captures the execution result tuple from _perform_execute
        and passes it directly to _build_result for clean data flow.

        Args:
            statement: The SQL statement to execute.
            connection: The database connection to use.

        Returns:
            The result of the SQL execution.
        """

        with self.with_cursor(connection) as cursor:
            execution_result = self._perform_execute(cursor, statement)
            return self._build_result(cursor, statement, execution_result)

    @abstractmethod
    def with_cursor(self, connection: Any) -> Any:
        """Create and return a context manager for cursor acquisition and cleanup.

        This method should return a context manager that yields a cursor.
        For sync drivers, this is typically implemented using @contextmanager
        or a custom context manager class.
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

    def _perform_execute(self, cursor: Any, statement: "SQL") -> "tuple[Any, Optional[int], Any]":
        """Enhanced sync execution logic with parameter integration and hook support.

        This method implements the enhanced execution pattern that:
        1. Calls _try_special_handling() hook first for database-specific operations
        2. Uses ParameterProcessor for centralized SQL compilation
        3. Routes to appropriate hook method based on statement type
        4. Returns execution results as standardized tuple

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement to execute

        Returns:
            Tuple of (cursor_result, rowcount_override, special_data)
        """
        # Step 1: Try special handling first (e.g., PostgreSQL COPY, bulk operations)
        special_result = self._try_special_handling(cursor, statement)
        if special_result is not None:
            return special_result

        # Step 2: Compile with driver's parameter style
        sql, params = self._get_compiled_sql(statement, self.statement_config)

        # Step 3: Route to appropriate hook method
        if statement.is_script:
            # Handle script execution
            if self.statement_config.parameter_config.needs_static_script_compilation:
                # Use static compilation for databases that don't support parameters in scripts
                static_sql = self._prepare_script_sql(statement)
                result = self._execute_script(cursor, static_sql, None, self.statement_config)
            else:
                # Prepare parameters for script execution
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
                result = self._execute_script(cursor, sql, prepared_params, self.statement_config)
            return create_execution_result(result)
        if statement.is_many:
            # Prepare parameters for executemany
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=True)
            result = self._execute_many(cursor, sql, prepared_params)
            return create_execution_result(result)
        # Prepare parameters for single execution
        prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
        result = self._execute_statement(cursor, sql, prepared_params)
        return create_execution_result(result)

    @abstractmethod
    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for database-specific special operations (e.g., PostgreSQL COPY, bulk operations).

        This method is called first in _perform_execute() to allow drivers to handle
        special operations that don't follow the standard SQL execution pattern.

        Args:
            cursor: Database cursor/connection object
            statement: SQL statement to analyze

        Returns:
            Tuple of (cursor_result, rowcount_override, special_data) if handled,
            None if standard execution should proceed
        """

    @abstractmethod
    def _execute_script(self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig") -> Any:
        """Execute a SQL script (multiple statements).

        Default implementation splits script and executes statements individually.
        Drivers can override for database-specific script execution methods.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL script
            prepared_params: Prepared parameters
            statement_config: Statement configuration for dialect information

        Returns:
            Driver-specific result
        """

    @abstractmethod
    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute SQL with multiple parameter sets (executemany).

        Must be implemented by each driver for database-specific executemany logic.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL statement
            prepared_params: List of prepared parameter sets

        Returns:
            Driver-specific result
        """

    @abstractmethod
    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute a single SQL statement.

        Must be implemented by each driver for database-specific execution logic.

        Args:
            cursor: Database cursor/connection object
            sql: Compiled SQL statement
            prepared_params: Prepared parameters

        Returns:
            Driver-specific result
        """

    # New abstract methods for data extraction
    @abstractmethod
    def _get_selected_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Returns:
            Tuple of (data_rows, column_names, row_count)
        """

    @abstractmethod
    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE.

        Returns:
            Number of affected rows
        """

    def _build_result(
        self, cursor: Any, statement: "SQL", execution_result: "tuple[Any, Optional[int], Any]"
    ) -> "SQLResult":
        """Build and return the result of the SQL execution.

        This method is now implemented in the base class using the
        abstract extraction methods.
        """
        _, rowcount_override, special_data = execution_result

        if statement.is_script:
            # Use rowcount override if provided, otherwise extract from cursor
            row_count = rowcount_override if rowcount_override is not None else self._get_row_count(cursor)
            # Count statements in the script
            sql, _ = statement.compile()
            statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
            statement_count = len([stmt for stmt in statements if stmt.strip()])
            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=row_count,
                operation_type="SCRIPT",
                total_statements=statement_count,
                successful_statements=statement_count,  # Assume all successful if no exception
                metadata=special_data or {"status_message": "OK"},
            )

        # Handle regular operations
        if statement.returns_rows():
            data, column_names, row_count = self._get_selected_data(cursor)
            return self._build_select_result_from_data(
                statement=statement, data=data, column_names=column_names, row_count=row_count
            )
        # Use rowcount override if provided, otherwise extract from cursor
        row_count = rowcount_override if rowcount_override is not None else self._get_row_count(cursor)
        return self._build_execute_result_from_data(statement=statement, row_count=row_count, metadata=special_data)

    def _build_select_result_from_data(
        self, statement: "SQL", data: "list[dict[str, Any]]", column_names: "list[str]", row_count: int
    ) -> "SQLResult":
        """Build SQLResult for SELECT operations from extracted data."""
        return SQLResult(
            statement=statement, data=data, column_names=column_names, rows_affected=row_count, operation_type="SELECT"
        )

    def _prepare_sql(
        self,
        statement: "Union[Statement, QueryBuilder]",
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "StatementConfig",
        **kwargs: Any,
    ) -> "SQL":
        """Build SQL statement from various input types.

        Ensures dialect is set and preserves existing state when rebuilding SQL objects.
        """
        if isinstance(statement, QueryBuilder):
            return statement.to_statement(config=config)
        if isinstance(statement, SQL):
            if parameters or kwargs:
                new_config = config
                if self.dialect and new_config and not new_config.dialect:
                    new_config = new_config.replace(dialect=self.dialect)
                return statement.copy(
                    parameters=(*statement._positional_params, *parameters)
                    if parameters
                    else statement._positional_params,
                    statement_config=new_config,
                    **kwargs,
                )
            if self.dialect and (
                not statement.statement_config.dialect or statement.statement_config.dialect != self.dialect
            ):
                new_config = statement.statement_config.replace(dialect=self.dialect)
                if statement.parameters:
                    return statement.copy(statement_config=new_config, dialect=self.dialect)
                return statement.copy(statement_config=new_config, dialect=self.dialect)
            return statement
        if self.dialect and config and not config.dialect:
            config = config.replace(dialect=self.dialect)
        return SQL(statement, *parameters, config=config, **kwargs)

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        sql_statement = self._prepare_sql(
            statement, *parameters, config=statement_config or self.statement_config, **kwargs
        )
        return self._dispatch_execution(statement=sql_statement, connection=self.connection)

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        statement_config: "Optional[StatementConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters.

        Parameters passed will be used as the batch execution sequence.
        """
        # For execute_many, we need to handle parameters specially to preserve structure
        if parameters and len(parameters) == 1 and isinstance(parameters[0], list):
            # Direct list of parameter sets - pass the full list to as_many
            sql_statement = self._prepare_sql(statement, config=statement_config or self.statement_config, **kwargs)
            return self._dispatch_execution(statement=sql_statement.as_many(parameters[0]), connection=self.connection)

        # Default behavior for other cases
        sql_statement = self._prepare_sql(
            statement, *parameters, config=statement_config or self.statement_config, **kwargs
        )
        return self._dispatch_execution(statement=sql_statement.as_many(), connection=self.connection)

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
        return self._dispatch_execution(
            statement=self._prepare_sql(
                statement, *parameters, config=statement_config or self.statement_config, **kwargs
            ).as_script(),
            connection=self.connection,
        )

    # Syntax Sugar Methods for Selecting Data Below:
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

    def select_one(  # type: ignore[misc]
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
            self.to_schema(cast("list[ModelT]", result.get_data()), schema_type=schema_type),  # type: ignore[arg-type]
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
        row = result.one()
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
        try:
            return row[0]
        except (TypeError, IndexError) as e:
            msg = f"Cannot extract value from row type {type(row).__name__}: {e}"
            raise TypeError(msg) from e

    def _create_count_query(self, original_sql: "SQL") -> "SQL":
        """Create a COUNT query from the original SQL statement.

        Transforms the original SELECT statement to count total rows while preserving
        WHERE, HAVING, and GROUP BY clauses but removing ORDER BY, LIMIT, and OFFSET.

        For queries with GROUP BY, wraps the query in a subquery to count groups correctly.
        """
        if not original_sql.expression:
            msg = "Cannot create COUNT query from empty SQL expression"
            raise ImproperConfigurationError(msg)
        expr = original_sql.expression.copy()

        if isinstance(expr, exp.Select):
            # Check if query has GROUP BY clause
            if expr.args.get("group"):
                # For GROUP BY queries, wrap in subquery and count rows
                # This counts the number of groups, not the total rows
                subquery = expr.subquery(alias="grouped_data")
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery)
            else:
                # Simple case: replace SELECT list with COUNT(*)
                count_expr = exp.select(exp.Count(this=exp.Star())).from_(
                    cast("exp.Expression", expr.args.get("from")), copy=False
                )
                if expr.args.get("where"):
                    count_expr = count_expr.where(cast("exp.Expression", expr.args.get("where")), copy=False)
                if expr.args.get("having"):
                    count_expr = count_expr.having(cast("exp.Expression", expr.args.get("having")), copy=False)

            # Remove ORDER BY, LIMIT, OFFSET - preserve WHERE, HAVING, GROUP BY
            count_expr.set("order", None)
            count_expr.set("limit", None)
            count_expr.set("offset", None)

            # Create new SQL with same parameters and config as original
            return SQL(count_expr, *original_sql._positional_params, config=original_sql.statement_config)

        # Handle other query types (UNION, etc.) - wrap in subquery
        subquery = cast("exp.Select", expr).subquery(alias="total_query")
        count_expr = exp.select(exp.Count(this=exp.Star())).from_(subquery)
        # Create new SQL with same parameters and config as original
        return SQL(count_expr, *original_sql._positional_params, config=original_sql.statement_config)

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
        sql_statement = self._prepare_sql(
            statement, *parameters, config=statement_config or self.statement_config, **kwargs
        )
        count_result = self._dispatch_execution(self._create_count_query(sql_statement), self.connection)
        select_result = self.execute(sql_statement)
        data = self.to_schema(select_result.get_data(), schema_type=schema_type)
        return (data, count_result.scalar())
