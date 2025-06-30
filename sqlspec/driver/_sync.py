"""Synchronous driver protocol implementation."""

from abc import ABC, abstractmethod
from dataclasses import replace
from typing import Any, Optional, Union, overload

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.statement.builder import DeleteBuilder, InsertBuilder, QueryBuilder, SelectBuilder, UpdateBuilder
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import ConnectionT, DictRow, ModelDTOT, RowT, StatementParameters
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import can_convert_to_schema

logger = get_logger("sqlspec")

__all__ = ("SyncDriverAdapterProtocol",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class SyncDriverAdapterProtocol(CommonDriverAttributesMixin[ConnectionT, RowT], ABC):
    __slots__ = ()

    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize sync driver adapter.

        Args:
            connection: The database connection
            config: SQL statement configuration
            default_row_type: Default row type for results (DictRow, TupleRow, etc.)
        """
        # Initialize CommonDriverAttributes part
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
                # Create a new SQL object with the same SQL but additional parameters
                # Create a new SQL object with additional parameters
                new_config = _config
                if self.dialect and not new_config.dialect:
                    new_config = replace(new_config, dialect=self.dialect)
                return SQL(statement, parameters=parameters or None, kwargs=kwargs or None, config=new_config)
            return statement
        # Create new SQL object
        new_config = _config
        if self.dialect and not new_config.dialect:
            new_config = replace(new_config, dialect=self.dialect)
        return SQL(statement, parameters=parameters or None, kwargs=kwargs or None, config=new_config)

    @abstractmethod
    def _execute_statement(
        self, statement: "SQL", connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> "SQLResult[RowT]":
        """Actual execution implementation by concrete drivers, using the raw connection.

        Returns SQLResult directly based on the statement type.
        """
        raise NotImplementedError

    @overload
    def execute(
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
    def execute(
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
    def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    def execute(
        self,
        statement: "Statement",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "type[ModelDTOT]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

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
    ) -> "SQLResult[RowT]": ...

    def execute(
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
        result = self._execute_statement(statement=sql_statement, connection=self._connection(_connection), **kwargs)

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

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
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
        sql_statement = self._build_statement(statement, _config=_config or self.config, **kwargs).as_many(
            param_sequence
        )

        return self._execute_statement(
            statement=sql_statement,
            connection=self._connection(_connection),
            parameters=param_sequence,
            is_many=True,
            **kwargs,
        )

    def execute_script(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        # Separate parameters from filters
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

        if filters:
            sql_statement = SQL(statement, parameters=primary_params or None, config=script_config, **kwargs)
            for filter_ in filters:
                sql_statement = sql_statement.filter(filter_)
        else:
            sql_statement = SQL(statement, parameters=primary_params or None, config=script_config, **kwargs)
        sql_statement = sql_statement.as_script()
        return self._execute_statement(
            statement=sql_statement, connection=self._connection(_connection), is_script=True, **kwargs
        )
