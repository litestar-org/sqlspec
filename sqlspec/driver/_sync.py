"""Synchronous driver protocol implementation."""

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver.context import set_current_driver
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncAdapterCacheMixin, ToSchemaMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.builder import QueryBuilder, Select
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import ModelDTOT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.typing import StatementParameters

logger = get_logger("sqlspec")

__all__ = ("SyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class SyncDriverAdapterBase(CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin, SyncAdapterCacheMixin):
    __slots__ = ()

    def _dispatch_execution(self, statement: "SQL", connection: "Any") -> "SQLResult":
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
            with self.with_cursor(connection) as cursor:
                self._perform_execute(cursor, statement)
                return self._build_result(cursor, statement)
        finally:
            # Clear driver context
            set_current_driver(None)

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

    @abstractmethod
    def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""

    # New abstract methods for data extraction
    @abstractmethod
    def _extract_select_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
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

    def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution.

        This method is now implemented in the base class using the
        abstract extraction methods.
        """
        if self.returns_rows(statement.expression):
            data, column_names, row_count = self._extract_select_data(cursor)
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

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        suppress_warnings: bool = False,
        **kwargs: Any,
    ) -> "SQLResult":
        sql_statement = self._prepare_sql(statement, *parameters, config=config or self.config, **kwargs)
        return self._dispatch_execution(statement=sql_statement, connection=self.connection)

    def execute_many(
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
        sql_statement = self._prepare_sql(statement, *parameters, config=config or self.config, **kwargs)
        return self._dispatch_execution(statement=sql_statement.as_many(), connection=self.connection)

    def execute_script(
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
        return self._dispatch_execution(
            statement=self._prepare_sql(statement, *parameters, config=config or self.config, **kwargs).as_script(),
            connection=self.connection,
        )

    # Syntax Sugar Methods for Selecting Data Below:
    @overload
    def select_one(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    def select_one(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "dict[str,Any]": ...

    def select_one(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Execute a select statement and return exactly one row.

        Raises an exception if no rows or more than one row is returned.
        """
        result = self.execute(statement, *parameters, config=config, **kwargs)
        data = result.get_data()
        if not data:
            msg = "No rows found"
            raise NotFoundError(msg)
        if len(data) > 1:
            msg = f"Expected exactly one row, found {len(data)}"
            raise ValueError(msg)
        return self.to_schema(data[0], schema_type=schema_type) if schema_type else data[0]

    @overload
    def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...

    def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Execute a select statement and return at most one row.

        Returns None if no rows are found.
        Raises an exception if more than one row is returned.
        """
        result = self.execute(statement, *parameters, config=config, **kwargs)
        data = result.get_data()
        if not data:
            return None
        if len(data) > 1:
            msg = f"Expected at most one row, found {len(data)}"
            raise ValueError(msg)
        return self.to_schema(data[0], schema_type=schema_type)

    @overload
    def select(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    def select(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...

    def select(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Union[list[dict[str, Any]], list[ModelDTOT]]:
        """Execute a select statement and return all rows."""
        result = self.execute(statement, *parameters, config=config, **kwargs)
        return self.to_schema(result.get_data(), schema_type=schema_type)

    def select_value(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value.

        Expects exactly one row with one column.
        Raises an exception if no rows or more than one row/column is returned.
        """
        result = self.execute(statement, *parameters, config=config, **kwargs)
        data = result.get_data()
        if not data:
            msg = "No rows found"
            raise NotFoundError(msg)
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
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a select statement and return a single scalar value or None.

        Returns None if no rows are found.
        Expects at most one row with one column.
        Raises an exception if more than one row is returned.
        """
        result = self.execute(statement, *parameters, config=config, **kwargs)
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
