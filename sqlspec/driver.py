import logging
import time
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    Union,
    cast,
    overload,
)

from sqlglot import exp

from sqlspec.config import (
    AsyncDatabaseConfig,
    InstrumentationConfig,
)
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.builder import (
    DeleteBuilder,
    InsertBuilder,
    QueryBuilder,
    SelectBuilder,
    UpdateBuilder,
)
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import (  # pyright: ignore
    ConnectionT,
    Counter,  # pyright: ignore
    DictRow,
    Gauge,  # pyright: ignore
    Histogram,  # pyright: ignore
    ModelDTOT,  # pyright: ignore
    RowT,
    SQLParameterType,
    Status,  # pyright: ignore
    StatusCode,  # pyright: ignore
    T,
    Tracer,  # pyright: ignore
    trace,
)
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.parameters import ParameterStyle
    from sqlspec.statement.result import SQLResult


__all__ = (
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterProtocol",
    "AsyncInstrumentationMixin",
    "CommonDriverAttributes",
    "DriverAdapterProtocol",
    "SyncDriverAdapterProtocol",
    "SyncInstrumentationMixin",
)


logger = logging.getLogger("sqlspec")


class CommonDriverAttributes(ABC, Generic[ConnectionT, RowT]):
    """Enhanced common attributes and methods for driver adapters with instrumentation."""

    dialect: str
    """The SQL dialect supported by the underlying database driver."""
    parameter_style: "ParameterStyle"
    """The parameter style used by the driver."""
    connection: "ConnectionT"
    """The underlying database connection."""
    config: "SQLConfig"
    """Configuration for SQL statements."""
    instrumentation_config: "InstrumentationConfig"
    """Configuration for instrumentation."""
    default_row_type: "type[RowT]"
    """The default row type to use for results (DictRow, TupleRow, etc.)."""

    __supports_arrow__: "ClassVar[bool]" = False
    """Indicates if the driver supports Apache Arrow operations."""

    _tracer: "Optional[Tracer]" = None
    _query_counter: "Optional[Counter]" = None
    _error_counter: "Optional[Counter]" = None
    _latency_histogram: "Optional[Histogram]" = None
    _pool_latency_histogram: "Optional[Histogram]" = None
    _pool_connections_gauge: "Optional[Gauge]" = None

    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize with connection, config, instrumentation_config, and default_row_type.

        Args:
            connection: The database connection
            config: SQL statement configuration
            instrumentation_config: Instrumentation configuration
            default_row_type: Default row type for results (DictRow, TupleRow, etc.)
        """
        self.connection = connection
        self.config = config or SQLConfig()
        self.instrumentation_config = instrumentation_config or InstrumentationConfig()
        self.default_row_type = default_row_type or DictRow  # type: ignore[assignment]
        self._setup_instrumentation()

    def _setup_instrumentation(self) -> None:
        """Set up OpenTelemetry and Prometheus instrumentation."""
        if self.instrumentation_config.enable_opentelemetry:
            self._setup_opentelemetry()
        if self.instrumentation_config.enable_prometheus:
            self._setup_prometheus()

    def _setup_opentelemetry(self) -> None:
        """Set up OpenTelemetry tracer with proper service naming."""
        if trace is None:
            logger.warning("OpenTelemetry not installed, skipping OpenTelemetry setup.")
            return
        self._tracer = trace.get_tracer(
            self.instrumentation_config.service_name,
            # __version__ # Consider adding version here if available
        )

    def _setup_prometheus(self) -> None:  # pragma: no cover
        """Set up Prometheus metrics with proper labeling and semantic naming."""
        try:
            service_name = self.instrumentation_config.service_name
            custom_tag_keys = list(self.instrumentation_config.custom_tags.keys())

            # Database operation metrics
            self._query_counter = Counter(
                f"{service_name}_db_operations_total",
                "Total number of database operations executed",
                ["operation", "status", "db_system", *custom_tag_keys],
            )
            self._error_counter = Counter(
                f"{service_name}_db_errors_total",
                "Total number of database errors",
                ["operation", "error_type", "db_system", *custom_tag_keys],
            )
            self._latency_histogram = Histogram(
                f"{service_name}_db_operation_duration_seconds",
                "Database operation duration in seconds",
                ["operation", "db_system", *custom_tag_keys],
                buckets=self.instrumentation_config.prometheus_latency_buckets
                or [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10],  # Default buckets
            )

            # Connection pool metrics
            self._pool_latency_histogram = Histogram(
                f"{service_name}_db_pool_operation_duration_seconds",
                "Database connection pool operation duration in seconds",
                ["operation", "db_system", *custom_tag_keys],
                buckets=self.instrumentation_config.prometheus_latency_buckets
                or [0.001, 0.005, 0.01, 0.05, 0.1, 5, 10],  # Buckets for pool operations
            )
            self._pool_connections_gauge = Gauge(
                f"{service_name}_db_pool_connections",
                "Number of database connections in the pool by status",
                ["db_system", "status", *custom_tag_keys],
            )
        except (ImportError, AttributeError) as e:  # pragma: no cover
            logger.warning("Prometheus client not available or misconfigured, skipping Prometheus setup: %s", e)

    @abstractmethod
    def _get_placeholder_style(self) -> "ParameterStyle":
        """Return the parameter style for the driver (e.g., qmark, numeric)."""
        raise NotImplementedError  # pragma: no cover

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "ConnectionT":
        return connection or self.connection

    @staticmethod
    def returns_rows(expression: "Optional[exp.Expression]") -> bool:
        """Check if the SQL expression is expected to return rows.

        Args:
            expression: The SQL expression.

        Returns:
            True if the expression is a SELECT, VALUES, or WITH statement
            that is not a CTE definition.
        """
        if expression is None:
            return False
        if isinstance(
            expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma, exp.Command)
        ):  # Added more types including Command for SHOW/EXPLAIN statements
            return True
        if isinstance(expression, exp.With) and expression.expressions:
            # Check the final expression in the WITH clause
            return CommonDriverAttributes.returns_rows(expression.expressions[-1])
        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):  # Check for RETURNING
            return bool(expression.find(exp.Returning))
        return False

    @staticmethod
    def check_not_found(item_or_none: "Optional[T]" = None) -> "T":
        """Raise :exc:`sqlspec.exceptions.NotFoundError` if ``item_or_none`` is ``None``.

        Args:
            item_or_none: Item to be tested for existence.

        Raises:
            NotFoundError: If ``item_or_none`` is ``None``

        Returns:
            The item, if it exists.
        """
        if item_or_none is None:
            msg = "No result found when one was expected"
            raise NotFoundError(msg)
        return item_or_none


class SyncInstrumentationMixin(ABC):
    """Mixin providing synchronous instrumentation methods for sync drivers."""

    # These attributes are expected to be on the class that uses this mixin (from CommonDriverAttributes)
    instrumentation_config: "InstrumentationConfig"
    _tracer: "Optional[Tracer]"
    dialect: str
    _query_counter: "Optional[Counter]"
    _latency_histogram: "Optional[Histogram]"
    _error_counter: "Optional[Counter]"

    def instrument_sync_operation(
        self,
        operation_name: str,
        operation_type: str,
        custom_tags_from_decorator: dict[str, Any],
        func_to_execute: Any,
        original_self: Any,  # The instance of the adapter protocol
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Instrument a synchronous operation with OpenTelemetry and Prometheus."""
        start_time = time.monotonic()
        final_custom_tags = {**self.instrumentation_config.custom_tags, **custom_tags_from_decorator}

        if self.instrumentation_config.log_queries:
            logger.info("Starting %s operation", operation_name, extra={"operation_type": operation_type})

        span = None
        if self._tracer:
            span = self._tracer.start_span(operation_name)
            span.set_attribute("operation.type", operation_type)
            span.set_attribute("db.system", self.dialect)
            span.set_attribute("service.name", self.instrumentation_config.service_name)

            # Merge decorator tags with global custom tags
            for key, value in final_custom_tags.items():
                span.set_attribute(key, value)

        try:
            # func_to_execute is the original method (e.g., _execute_statement)
            # original_self is the instance of SyncDriverAdapterProtocol
            result = func_to_execute(original_self, *args, **kwargs)
            latency = time.monotonic() - start_time

            if self.instrumentation_config.log_runtime:
                logger.info(
                    "Completed %s in %.3fms",
                    operation_name,
                    latency * 1000,
                    extra={"operation_type": operation_type, "latency_ms": latency * 1000, "status": "success"},
                )

            # Update Prometheus metrics with proper labels
            if self._query_counter and operation_type == "database":
                self._query_counter.labels(
                    operation=operation_name, status="success", db_system=self.dialect, **final_custom_tags
                ).inc()
            if self._latency_histogram and operation_type == "database":
                self._latency_histogram.labels(
                    operation=operation_name, db_system=self.dialect, **final_custom_tags
                ).observe(latency)

            if span:
                span.set_attribute("duration_ms", latency * 1000)
                if hasattr(result, "get_affected_count"):  # Check on actual result
                    affected_count = result.get_affected_count()
                    span.set_attribute("db.rows_affected", affected_count)
                elif isinstance(result, (list, tuple)):  # Common raw results
                    rows_returned = len(result)
                    span.set_attribute("db.rows_returned", rows_returned)

                if StatusCode is not None and Status is not None:
                    span.set_status(Status(StatusCode.OK))
        except Exception as e:
            latency = time.monotonic() - start_time

            if self.instrumentation_config.log_queries:
                logger.exception(
                    "Error in %s after %.3fms",
                    operation_name,
                    latency * 1000,
                    extra={
                        "operation_type": operation_type,
                        "latency_ms": latency * 1000,
                        "status": "error",
                        "error_type": type(e).__name__,
                    },
                )

            if span:
                span.record_exception(e)
                if StatusCode is not None and Status is not None:
                    span.set_status(Status(StatusCode.ERROR, str(e)))

            if self._error_counter:
                self._error_counter.labels(
                    operation=operation_name, error_type=type(e).__name__, db_system=self.dialect, **final_custom_tags
                ).inc()
            raise
        else:
            return result
        finally:
            if span:
                span.end()


class AsyncInstrumentationMixin(ABC):
    """Mixin providing asynchronous instrumentation methods for async drivers."""

    instrumentation_config: "InstrumentationConfig"
    _tracer: "Optional[Tracer]"
    dialect: str
    _query_counter: "Optional[Counter]"
    _latency_histogram: "Optional[Histogram]"
    _error_counter: "Optional[Counter]"

    async def instrument_async_operation(
        self,
        operation_name: str,
        operation_type: str,
        custom_tags_from_decorator: dict[str, Any],
        func_to_execute: Any,
        original_self: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Instrument an asynchronous operation with OpenTelemetry and Prometheus."""
        start_time = time.monotonic()
        final_custom_tags = {**self.instrumentation_config.custom_tags, **custom_tags_from_decorator}

        if self.instrumentation_config.log_queries:
            logger.info("Starting %s operation", operation_name, extra={"operation_type": operation_type})

        span = None
        if self._tracer:
            span = self._tracer.start_span(operation_name)
            span.set_attribute("operation.type", operation_type)
            span.set_attribute("db.system", self.dialect)
            span.set_attribute("service.name", self.instrumentation_config.service_name)

            for key, value in final_custom_tags.items():
                span.set_attribute(key, value)

        try:
            result = await func_to_execute(original_self, *args, **kwargs)
            latency = time.monotonic() - start_time

            if self.instrumentation_config.log_runtime:
                logger.info(
                    "Completed %s in %.3fms",
                    operation_name,
                    latency * 1000,
                    extra={"operation_type": operation_type, "latency_ms": latency * 1000, "status": "success"},
                )

            if self._query_counter and operation_type == "database":
                self._query_counter.labels(
                    operation=operation_name, status="success", db_system=self.dialect, **final_custom_tags
                ).inc()
            if self._latency_histogram and operation_type == "database":
                self._latency_histogram.labels(
                    operation=operation_name, db_system=self.dialect, **final_custom_tags
                ).observe(latency)

            if span:
                span.set_attribute("duration_ms", latency * 1000)
                if hasattr(result, "get_affected_count"):
                    affected_count = result.get_affected_count()
                    span.set_attribute("db.rows_affected", affected_count)
                elif isinstance(result, (list, tuple)):
                    rows_returned = len(result)
                    span.set_attribute("db.rows_returned", rows_returned)

                if StatusCode is not None and Status is not None:
                    span.set_status(Status(StatusCode.OK))
        except Exception as e:
            latency = time.monotonic() - start_time

            if self.instrumentation_config.log_queries:
                logger.exception(
                    "Error in %s after %.3fms",
                    operation_name,
                    latency * 1000,
                    extra={
                        "operation_type": operation_type,
                        "latency_ms": latency * 1000,
                        "status": "error",
                        "error_type": type(e).__name__,
                    },
                )

            if span:
                span.record_exception(e)
                if StatusCode is not None and Status is not None:
                    span.set_status(Status(StatusCode.ERROR, str(e)))

            if self._error_counter:
                self._error_counter.labels(
                    operation=operation_name, error_type=type(e).__name__, db_system=self.dialect, **final_custom_tags
                ).inc()
            raise
        else:
            return result
        finally:
            if span:
                span.end()


EMPTY_FILTERS: "list[StatementFilter]" = []


class SyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT, RowT], SyncInstrumentationMixin, ABC):
    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        """Initialize sync driver adapter.

        Args:
            connection: The database connection
            config: SQL statement configuration
            instrumentation_config: Instrumentation configuration
            default_row_type: Default row type for results (DictRow, TupleRow, etc.)
        """
        # Initialize CommonDriverAttributes part
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
        )

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
    def _execute_statement(
        self,
        statement: "SQL",
        connection: "Optional[ConnectionT]" = None,
        **kwargs: Any,
    ) -> Any:  # Raw driver result
        """Actual execution implementation by concrete drivers, using the raw connection."""
        raise NotImplementedError

    @abstractmethod
    def _wrap_select_result(
        self,
        statement: "SQL",
        result: Any,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        raise NotImplementedError

    @abstractmethod
    def _wrap_execute_result(
        self,
        statement: "SQL",
        result: Any,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        raise NotImplementedError

    # Type-safe overloads based on the refactor plan pattern
    @overload
    def execute(
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
    def execute(
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
    def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    def execute(
        self,
        statement: "Union[str, exp.Expression]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "type[ModelDTOT]",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[ModelDTOT]": ...

    @overload
    def execute(
        self,
        statement: "Union[str, exp.Expression]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: None = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    @overload
    def execute(
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
    def execute(
        self,
        statement: "SQL",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: None = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]": ...

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        with instrument_operation(self, "execute", "database"):
            sql_statement = self._build_statement(
                statement, parameters, filters=list(filters) or [], config=config or self.config
            )
            result = self._execute_statement(
                statement=sql_statement,
                connection=self._connection(connection),
                **kwargs,
            )
            if CommonDriverAttributes.returns_rows(sql_statement.expression):
                return self._wrap_select_result(sql_statement, result, schema_type=schema_type, **kwargs)
            return self._wrap_execute_result(sql_statement, result, **kwargs)

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        with instrument_operation(self, "execute_many", "database"):
            # For execute_many, don't pass the parameter sequence to _build_statement
            # to avoid individual parameter validation. Parse once without parameters.
            sql_statement = self._build_statement(
                statement, parameters=None, filters=list(filters) or [], config=config or self.config
            )
            # Mark the statement for batch execution with the parameter sequence
            sql_statement = sql_statement.as_many(parameters)
            result = self._execute_statement(
                statement=sql_statement,
                connection=self._connection(connection),
                parameters=parameters,
                is_many=True,
                **kwargs,
            )
            return self._wrap_execute_result(sql_statement, result, **kwargs)

    def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult[RowT]":
        with instrument_operation(self, "execute_script", "database"):
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
            script_output = self._execute_statement(
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


class AsyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT, RowT], AsyncInstrumentationMixin, ABC):
    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
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
        statement: "Union[str, exp.Expression]",
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
        statement: "Union[str, exp.Expression]",
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
            if CommonDriverAttributes.returns_rows(sql_statement.expression):
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


DriverAdapterProtocol = Union[
    SyncDriverAdapterProtocol[ConnectionT, RowT], AsyncDriverAdapterProtocol[ConnectionT, RowT]
]
