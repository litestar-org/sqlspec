"""Asynchronous driver protocol implementation."""

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
    cast,
    overload,
)

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver.mixins import AsyncInstrumentationMixin
from sqlspec.statement.builder import (
    DeleteBuilder,
    InsertBuilder,
    QueryBuilder,
    SelectBuilder,
    UpdateBuilder,
)
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import (
    ConnectionT,
    DictRow,
    ModelDTOT,
    RowT,
    SQLParameterType,
)
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import SQLResult

__all__ = ("AsyncDriverAdapterProtocol",)


EMPTY_FILTERS: "list[StatementFilter]" = []


class AsyncDriverAdapterProtocol(CommonDriverAttributesMixin[ConnectionT, RowT], AsyncInstrumentationMixin, ABC):
    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[Any]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize async driver adapter.

        Args:
            connection: The database connection
            config: SQL statement configuration
            instrumentation_config: Instrumentation configuration
            default_row_type: Default row type for results (DictRow, TupleRow, etc.)
        """
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
        )
        # AsyncInstrumentationMixin has no __init__

    def _build_statement(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        filters: "Optional[list[StatementFilter]]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        if isinstance(statement, SQL):
            return statement
        if isinstance(statement, QueryBuilder):
            return statement.to_statement(config=config or self.config)
        return SQL(statement, parameters, *filters or [], dialect=self.dialect, config=config or self.config)

    @abstractmethod
    async def _execute_statement(
        self,
        statement: "SQL",
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> Any:  # Raw driver result
        raise NotImplementedError

    @abstractmethod
    async def _wrap_select_result(
        self,
        statement: "SQL",
        result: Any,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        raise NotImplementedError

    @abstractmethod
    async def _wrap_execute_result(
        self,
        statement: "SQL",
        result: Any,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        raise NotImplementedError

    # Type-safe overloads based on the refactor plan pattern
    @overload
    async def execute(
        self,
        statement: "SelectBuilder",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "type[ModelDTOT]",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

    @overload
    async def execute(
        self,
        statement: "SelectBuilder",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: None = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[str, Any]",  # exp.Expression
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "type[ModelDTOT]",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

    @overload
    async def execute(
        self,
        statement: "Union[str, Any]",  # exp.Expression
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: None = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    async def execute(
        self,
        statement: "SQL",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "type[ModelDTOT]",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

    @overload
    async def execute(
        self,
        statement: "SQL",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: None = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        async with instrument_operation_async(self, "execute", "database"):
            sql_statement = self._build_statement(
                statement, parameters=parameters, filters=list(filters) or [], config=config or self.config
            )
            result = await self._execute_statement(
                statement=sql_statement,
                connection=self._connection(connection),
                **kwargs,
            )
            if self.returns_rows(sql_statement.expression):
                return await self._wrap_select_result(sql_statement, result, schema_type=schema_type, **kwargs)
            return await self._wrap_execute_result(sql_statement, result, **kwargs)

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",  # QueryBuilder for DMLs will likely not return rows.
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        async with instrument_operation_async(self, "execute_many", "database"):
            # For execute_many, don't pass the parameter sequence to _build_statement
            # to avoid individual parameter validation. Parse once without parameters.
            sql_statement = self._build_statement(
                statement, parameters=None, filters=list(filters) or [], config=config or self.config
            )
            # Mark the statement for batch execution with the parameter sequence
            sql_statement = sql_statement.as_many(parameters)
            result = await self._execute_statement(
                statement=sql_statement,
                connection=self._connection(connection),
                parameters=parameters,
                is_many=True,
                **kwargs,
            )
            return await self._wrap_execute_result(sql_statement, result, **kwargs)

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        async with instrument_operation_async(self, "execute_script", "database"):
            from sqlspec.statement.sql import SQLConfig

            script_config = config or self.config
            if script_config.enable_validation:
                script_config = SQLConfig(
                    enable_parsing=script_config.enable_parsing,
                    enable_validation=False,
                    enable_transformations=script_config.enable_transformations,
                    enable_analysis=script_config.enable_analysis,
                    strict_mode=False,
                    cache_parsed_expression=script_config.cache_parsed_expression,
                    processing_pipeline_components=[],
                    parameter_converter=script_config.parameter_converter,
                    parameter_validator=script_config.parameter_validator,
                    sqlglot_schema=script_config.sqlglot_schema,
                    analysis_cache_size=script_config.analysis_cache_size,
                )
            sql_statement = SQL(
                statement,
                parameters,
                *filters,
                dialect=self.dialect,
                config=script_config,
            )
            sql_statement = sql_statement.as_script()
            script_output = await self._execute_statement(
                statement=sql_statement,
                connection=self._connection(connection),
                is_script=True,
                **kwargs,
            )
            if isinstance(script_output, str):
                from sqlspec.statement.result import SQLResult

                result = SQLResult[RowT](
                    statement=sql_statement,
                    data=[],
                    operation_type="SCRIPT",
                )
                result.total_statements = 1
                result.successful_statements = 1
                return result
            return cast("SQLResult[RowT]", script_output)
