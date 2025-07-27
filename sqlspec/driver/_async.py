"""Asynchronous driver protocol implementation."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from mypy_extensions import mypyc_attr

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver.mixins import AsyncAdapterCacheMixin, SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import NotFoundError
from sqlspec.parameters import process_execute_many_parameters
from sqlspec.statement.builder import QueryBuilder, Select
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import is_dict_row, is_indexable_row

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import SQLResult
    from sqlspec.typing import ModelDTOT, StatementParameters

logger = get_logger("sqlspec")

__all__ = ("AsyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AsyncDriverAdapterBase(
    CommonDriverAttributesMixin, SQLTranslatorMixin, ToSchemaMixin, AsyncAdapterCacheMixin, ABC
):
    __slots__ = ()

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
                return SQL(
                    statement.statement,
                    *parameters,
                    config=new_config,
                    _existing_state={
                        "is_many": statement._is_many,
                        "is_script": statement._is_script,
                        "original_parameters": statement._original_parameters,
                        "filters": statement._filters,
                        "positional_params": statement._positional_params,
                        "named_params": statement._named_params,
                    },
                    **kwargs,
                )
            if self.dialect and (not statement._config.dialect or statement._config.dialect != self.dialect):
                new_config = statement._config.replace(dialect=self.dialect)
                if statement.parameters:
                    return SQL(
                        statement.statement,
                        parameters=statement.parameters,
                        config=new_config,
                        _existing_state={
                            "is_many": statement._is_many,
                            "is_script": statement._is_script,
                            "original_parameters": statement._original_parameters,
                            "filters": statement._filters,
                            "positional_params": statement._positional_params,
                            "named_params": statement._named_params,
                        },
                    )
                return SQL(
                    statement.statement,
                    config=new_config,
                    _existing_state={
                        "is_many": statement._is_many,
                        "is_script": statement._is_script,
                        "original_parameters": statement._original_parameters,
                        "filters": statement._filters,
                        "positional_params": statement._positional_params,
                        "named_params": statement._named_params,
                    },
                )
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
        from sqlspec.driver.context import set_current_driver

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

        Passes first parameter set through pipeline to enable literal extraction
        and consistent parameter processing.
        """
        filters, param_sequence = process_execute_many_parameters(parameters)

        bind_parameters = param_sequence[0] if param_sequence else None

        sql_statement = self._prepare_sql(statement, bind_parameters, *filters, config=config or self.config, **kwargs)

        return await self._dispatch_execution(
            statement=sql_statement.as_many(param_sequence), connection=self.connection
        )

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
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ModelDTOT": ...

    @overload
    async def select_one(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "dict[str,Any]": ...

    async def select_one(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str,Any], ModelDTOT]":
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
        return self.to_schema(data[0], schema_type=schema_type) if schema_type else data[0]

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[dict[str,Any]]": ...

    async def select_one_or_none(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str,Any], ModelDTOT]]":
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
        return self.to_schema(data[0], schema_type=schema_type) if schema_type else data[0]

    @overload
    async def select(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[ModelDTOT]": ...

    @overload
    async def select(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: None = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "list[dict[str,Any]]": ...  # pyright: ignore
    async def select(
        self,
        statement: "Union[Statement, Select]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[list[dict[str,Any]], list[ModelDTOT]]":  # pyright: ignore
        """Execute a select statement and return all rows."""
        result = await self.execute(statement, *parameters, config=config, **kwargs)
        return result.get_data()

    async def select_value(
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
            # Tuple or list-like row
            return row[0]
        # Try indexing - if it fails, we'll get a proper error
        try:
            return row[0]
        except (TypeError, IndexError) as e:
            msg = f"Cannot extract value from row type {type(row).__name__}: {e}"
            raise TypeError(msg) from e

    # Abstract methods that must be implemented by concrete adapters
    @abstractmethod
    def with_cursor(self, connection: Any) -> "AbstractAsyncContextManager[Any]":
        """Async context manager for cursor acquisition and cleanup."""

    @abstractmethod
    async def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin a database transaction."""

    @abstractmethod
    async def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback the current transaction."""

    @abstractmethod
    async def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit the current transaction."""

    @abstractmethod
    async def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        """Execute the SQL statement using the provided cursor."""

    @abstractmethod
    async def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution."""
