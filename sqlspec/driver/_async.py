"""Asynchronous driver protocol implementation."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver.context import set_current_driver
from sqlspec.driver.mixins import SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.typing import ModelDTOT, ModelT, RowT, StatementParameters

logger = get_logger("sqlspec")

__all__ = ("AsyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class AsyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin):
    __slots__ = ()

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
    async def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""

    # New abstract methods for data extraction
    @abstractmethod
    async def _extract_select_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Returns:
            Tuple of (data_rows, column_names, row_count)
        """

    @abstractmethod
    def _extract_execute_rowcount(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE.

        Returns:
            Number of affected rows
        """

    async def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution.

        This method is now implemented in the base class using the
        abstract extraction methods.
        """
        if self.returns_rows(statement.expression):
            data, column_names, row_count = await self._extract_select_data(cursor)
            return self._build_select_result_from_data(
                statement=statement, data=data, column_names=column_names, row_count=row_count
            )
        row_count = self._extract_execute_rowcount(cursor)
        return self._build_execute_result_from_data(statement=statement, row_count=row_count)

    def _build_select_result_from_data(
        self, statement: "SQL", data: "list[dict[str, Any]]", column_names: "list[str]", row_count: int
    ) -> "SQLResult":
        """Build SQLResult for SELECT operations from extracted data."""
        return SQLResult(
            statement=statement, data=data, column_names=column_names, rows_affected=row_count, operation_type="SELECT"
        )

    def _build_execute_result_from_data(
        self, statement: "SQL", row_count: int, metadata: "Optional[dict[str, Any]]" = None
    ) -> "SQLResult":
        """Build SQLResult for non-SELECT operations from extracted data."""
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=row_count,
            operation_type=self._determine_operation_type(statement),
            metadata=metadata or {"status_message": "OK"},
        )

    def _prepare_sql(
        self,
        statement: "Union[Statement, QueryBuilder]",
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "SQLConfig",
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
                    config=new_config,
                    **kwargs,
                )
            if self.dialect and (not statement._config.dialect or statement._config.dialect != self.dialect):
                new_config = statement._config.replace(dialect=self.dialect)
                if statement.parameters:
                    return statement.copy(config=new_config)
                return statement.copy(config=new_config)
            return statement
        if self.dialect and config and not config.dialect:
            config = config.replace(dialect=self.dialect)
        return SQL(statement, *parameters, config=config, **kwargs)

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        suppress_warnings: bool = False,
        **kwargs: Any,
    ) -> "SQLResult":
        sql_statement = self._prepare_sql(statement, *parameters, config=config or self.config, **kwargs)
        return await self._dispatch_execution(statement=sql_statement, connection=self.connection)

    async def _dispatch_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
        """Central execution dispatcher using the Template Method Pattern.

        This method orchestrates the common execution flow, delegating
        database-specific steps to abstract methods that concrete adapters
        must implement.

        The new pattern passes only (cursor, statement) to _perform_execute,
        allowing the driver implementation to handle compilation internally.
        This provides backward compatibility with drivers still using the
        old (cursor, sql, params, statement) signature.

        Args:
            statement: The SQL statement to execute.
            connection: The database connection to use.

        Returns:
            The result of the SQL execution.
        """

        # Set current driver in context for SQL compilation
        set_current_driver(self)
        try:
            async with self.with_cursor(connection) as cursor:
                await self._perform_execute(cursor, statement)
                return await self._build_result(cursor, statement)
        finally:
            # Clear driver context
            set_current_driver(None)

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        suppress_warnings: bool = False,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters.

        Parameters passed will be used as the batch execution sequence.
        """
        # For execute_many, we need to handle parameters specially to preserve structure
        if parameters and len(parameters) == 1 and isinstance(parameters[0], list):
            # Direct list of parameter sets - pass to as_many
            sql_statement = self._prepare_sql(statement, config=config or self.config, **kwargs)
            return await self._dispatch_execution(
                statement=sql_statement.as_many(parameters[0]), connection=self.connection
            )

        sql_statement = self._prepare_sql(statement, *parameters, config=config or self.config, **kwargs)

        # Mark for batch execution - as_many() will use the existing positional params
        return await self._dispatch_execution(statement=sql_statement.as_many(), connection=self.connection)

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        suppress_warnings: bool = False,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script.

        By default, validates each statement and logs warnings for dangerous
        operations. Use suppress_warnings=True for migrations and admin scripts.
        """
        script_config = config or self.config
        sql_statement = self._prepare_sql(statement, *parameters, config=script_config, **kwargs)

        return await self._dispatch_execution(statement=sql_statement.as_script(), connection=self.connection)

    # Syntax Sugar Methods for Selecting Data Below:
    @overload
    async def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    async def select_one(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelT, RowT, dict[str, Any]]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    async def select_one(  # type: ignore[misc]
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[ModelT, RowT,ModelDTOT]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = await self.execute(statement, *parameters, config=config, **kwargs)
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
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelT]": ...  # pyright: ignore[reportInvalidTypeVarUse]

    async def select_one_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[ModelT, ModelDTOT]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = await self.execute(statement, *parameters, config=config, **kwargs)
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
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    async def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelT]": ...  # pyright: ignore[reportInvalidTypeVarUse]
    async def select(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[list[ModelT], list[ModelDTOT]]":  # pyright: ignore[reportInvalidTypeVarUse]
        """Execute a select statement and return all rows."""
        result = await self.execute(statement, *parameters, config=config, **kwargs)
        return cast(
            "Union[list[ModelT], list[ModelDTOT]]",
            self.to_schema(cast("list[ModelT]", result.get_data()), schema_type=schema_type),  # type: ignore[arg-type]
        )

    async def select_value(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.
        """
        result = await self.execute(statement, *parameters, config=config, **kwargs)
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

    async def select_value_or_none(
        self,
        statement: "Union[Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.
        """
        result = await self.execute(statement, *parameters, config=config, **kwargs)
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
