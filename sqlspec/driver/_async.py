"""Asynchronous driver protocol implementation."""

from abc import ABC, abstractmethod
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver.parameters import process_execute_many_parameters
from sqlspec.statement.builder import Delete, Insert, QueryBuilder, Select, Update
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import ConnectionT, DictRow, ModelDTOT, RowT, StatementParameters
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import can_convert_to_schema

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter

logger = get_logger("sqlspec")

__all__ = ("AsyncDriverAdapterProtocol",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class AsyncDriverAdapterProtocol(CommonDriverAttributesMixin[ConnectionT, RowT], ABC):
    __slots__ = ()

    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize async driver adapter.

        Args:
            connection: The database connection
            config: SQL statement configuration
            default_row_type: Default row type for results (DictRow, TupleRow, etc.)
        """
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    def _build_statement(
        self,
        statement: "Union[Statement, QueryBuilder[Any]]",
        *parameters: "Union[StatementParameters, StatementFilter]",
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQL":
        # Use driver's config if none provided
        _config = _config or self.config

        if isinstance(statement, QueryBuilder):
            return statement.to_statement(config=_config)
        # If statement is already a SQL object, handle additional parameters
        if isinstance(statement, SQL):
            if parameters or kwargs:
                new_config = _config
                if self.dialect and not new_config.dialect:
                    new_config = replace(new_config, dialect=self.dialect)
                # Use raw SQL if available to ensure proper parsing with dialect
                sql_source = statement._raw_sql or statement._statement
                # Preserve filters and state when creating new SQL object
                existing_state = {
                    "is_many": statement._is_many,
                    "is_script": statement._is_script,
                    "original_parameters": statement._original_parameters,
                    "filters": statement._filters,
                    "positional_params": statement._positional_params,
                    "named_params": statement._named_params,
                }
                return SQL(sql_source, *parameters, config=new_config, _existing_state=existing_state, **kwargs)
            # Even without additional parameters, ensure dialect is set
            if self.dialect and (not statement._config.dialect or statement._config.dialect != self.dialect):
                new_config = replace(statement._config, dialect=self.dialect)
                # Use raw SQL if available to ensure proper parsing with dialect
                sql_source = statement._raw_sql or statement._statement
                # Preserve parameters and state when creating new SQL object
                # Use the public parameters property which always has the right value
                existing_state = {
                    "is_many": statement._is_many,
                    "is_script": statement._is_script,
                    "original_parameters": statement._original_parameters,
                    "filters": statement._filters,
                    "positional_params": statement._positional_params,
                    "named_params": statement._named_params,
                }
                if statement.parameters:
                    return SQL(
                        sql_source, parameters=statement.parameters, config=new_config, _existing_state=existing_state
                    )
                return SQL(sql_source, config=new_config, _existing_state=existing_state)
            return statement
        new_config = _config
        if self.dialect and not new_config.dialect:
            new_config = replace(new_config, dialect=self.dialect)
        return SQL(statement, *parameters, config=new_config, **kwargs)

    @abstractmethod
    async def _execute_statement(
        self, statement: "SQL", connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> "SQLResult[RowT]":
        """Actual execution implementation by concrete drivers, using the raw connection.

        Returns SQLResult directly based on the statement type.
        """
        raise NotImplementedError

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
    ) -> "SQLResult[ModelDTOT]": ...

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
    ) -> "SQLResult[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[Insert, Update, Delete]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

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
    ) -> "SQLResult[ModelDTOT]": ...

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
    ) -> "SQLResult[RowT]": ...

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        sql_statement = self._build_statement(statement, *parameters, _config=_config or self.config, **kwargs)
        result = await self._execute_statement(
            statement=sql_statement, connection=self._connection(_connection), **kwargs
        )

        # If schema_type is provided and we have data, convert it
        if schema_type and result.data and can_convert_to_schema(self):
            converted_data = list(self.to_schema(data=result.data, schema_type=schema_type))
            return SQLResult[ModelDTOT](
                statement=result.statement,
                data=converted_data,
                column_names=result.column_names,
                rows_affected=result.rows_affected,
                operation_type=result.operation_type,
                last_inserted_id=result.last_inserted_id,
                execution_time=result.execution_time,
                metadata=result.metadata,
            )

        return result

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        _filters, param_sequence = process_execute_many_parameters(parameters)

        # For execute_many, disable transformations to prevent literal extraction
        # since the SQL already has placeholders for bulk operations
        many_config = _config or self.config
        if many_config.enable_transformations:
            from dataclasses import replace

            many_config = replace(many_config, enable_transformations=False)

        sql_statement = self._build_statement(statement, _config=many_config, **kwargs).as_many(param_sequence)

        return await self._execute_statement(
            statement=sql_statement, connection=self._connection(_connection), **kwargs
        )

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        script_config = _config or self.config
        if script_config.enable_validation:
            script_config = replace(script_config, enable_validation=False, strict_mode=False)

        sql_statement = self._build_statement(statement, *parameters, _config=script_config, **kwargs)
        sql_statement = sql_statement.as_script()
        return await self._execute_statement(
            statement=sql_statement, connection=self._connection(_connection), **kwargs
        )
