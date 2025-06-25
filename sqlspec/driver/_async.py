"""Asynchronous driver protocol implementation."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union, cast, overload

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.statement.builder import DeleteBuilder, InsertBuilder, QueryBuilder, SelectBuilder, UpdateBuilder
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import ConnectionT, DictRow, ModelDTOT, RowT, StatementParameters

if TYPE_CHECKING:
    from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict

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
        # If statement is already a SQL object, return it as-is
        if isinstance(statement, SQL):
            return statement
        return SQL(statement, *parameters, _dialect=self.dialect, _config=_config, **kwargs)

    @abstractmethod
    async def _execute_statement(
        self, statement: "SQL", connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> "Union[SelectResultDict, DMLResultDict, ScriptResultDict]":
        """Actual execution implementation by concrete drivers, using the raw connection.

        Returns one of the standardized result dictionaries based on the statement type.
        """
        raise NotImplementedError

    @abstractmethod
    async def _wrap_select_result(
        self,
        statement: "SQL",
        result: "SelectResultDict",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        raise NotImplementedError

    @abstractmethod
    async def _wrap_execute_result(
        self, statement: "SQL", result: "Union[DMLResultDict, ScriptResultDict]", **kwargs: Any
    ) -> "SQLResult[RowT]":
        raise NotImplementedError

    # Type-safe overloads based on the refactor plan pattern
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
    ) -> "SQLResult[ModelDTOT]": ...

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
    ) -> "SQLResult[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
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

        if self.returns_rows(sql_statement.expression):
            return await self._wrap_select_result(
                sql_statement, cast("SelectResultDict", result), schema_type=schema_type, **kwargs
            )
        return await self._wrap_execute_result(
            sql_statement, cast("Union[DMLResultDict, ScriptResultDict]", result), **kwargs
        )

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",  # QueryBuilder for DMLs will likely not return rows.
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        # Separate parameters from filters
        param_sequences = []
        filters = []
        for param in parameters:
            if isinstance(param, StatementFilter):
                filters.append(param)
            else:
                param_sequences.append(param)

        # Use first parameter as the sequence for execute_many
        param_sequence = param_sequences[0] if param_sequences else None
        # Convert tuple to list if needed
        if isinstance(param_sequence, tuple):
            param_sequence = list(param_sequence)
        # Ensure param_sequence is a list or None
        if param_sequence is not None and not isinstance(param_sequence, list):
            param_sequence = list(param_sequence) if hasattr(param_sequence, "__iter__") else None
        sql_statement = self._build_statement(statement, _config=_config or self.config, **kwargs)
        sql_statement = sql_statement.as_many(param_sequence)
        result = await self._execute_statement(
            statement=sql_statement,
            connection=self._connection(_connection),
            parameters=param_sequence,
            is_many=True,
            **kwargs,
        )
        return await self._wrap_execute_result(
            sql_statement, cast("Union[DMLResultDict, ScriptResultDict]", result), **kwargs
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
        param_values = []
        filters = []
        for param in parameters:
            if isinstance(param, StatementFilter):
                filters.append(param)
            else:
                param_values.append(param)

        # Use first parameter as the primary parameter value, or None if no parameters
        primary_params = param_values[0] if param_values else None

        script_config = _config or self.config
        if script_config.enable_validation:
            script_config = SQLConfig(
                enable_parsing=script_config.enable_parsing,
                enable_validation=False,
                enable_transformations=script_config.enable_transformations,
                enable_analysis=script_config.enable_analysis,
                strict_mode=False,
                cache_parsed_expression=script_config.cache_parsed_expression,
                parameter_converter=script_config.parameter_converter,
                parameter_validator=script_config.parameter_validator,
                analysis_cache_size=script_config.analysis_cache_size,
                allowed_parameter_styles=script_config.allowed_parameter_styles,
                target_parameter_style=script_config.target_parameter_style,
                allow_mixed_parameter_styles=script_config.allow_mixed_parameter_styles,
            )
        sql_statement = SQL(statement, primary_params, *filters, _dialect=self.dialect, _config=script_config, **kwargs)
        sql_statement = sql_statement.as_script()
        script_output = await self._execute_statement(
            statement=sql_statement, connection=self._connection(_connection), is_script=True, **kwargs
        )
        if isinstance(script_output, str):
            result = SQLResult[RowT](statement=sql_statement, data=[], operation_type="SCRIPT")
            result.total_statements = 1
            result.successful_statements = 1
            return result
        # Wrap the ScriptResultDict using the driver's wrapper
        return await self._wrap_execute_result(sql_statement, cast("ScriptResultDict", script_output), **kwargs)
