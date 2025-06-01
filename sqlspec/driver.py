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

from psycopg.rows import TupleRow  # Assuming this is a common row type, adjust if needed
from sqlglot import exp
from typing_extensions import TypeVar

from sqlspec._typing import trace  # pyright: ignore
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
from sqlspec.typing import (
    ConnectionT,
    Counter,  # pyright: ignore
    DictRow,
    Gauge,  # pyright: ignore
    Histogram,  # pyright: ignore
    ModelDTOT,  # pyright: ignore
    SQLParameterType,
    Status,  # pyright: ignore
    StatusCode,  # pyright: ignore
    T,
    Tracer,  # pyright: ignore
)
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.parameters import ParameterStyle
    from sqlspec.statement.result import (
        ExecuteResult,
        SelectResult,
        StatementResult,
    )


StatementResultType = Union["StatementResult[dict[str, Any]]", "StatementResult[dict[str, T]]"]

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
DefaultRowT = TypeVar("DefaultRowT", bound=Union[DictRow, TupleRow], default=DictRow)  # pyright: ignore


class CommonDriverAttributes(ABC, Generic[ConnectionT, DefaultRowT]):
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
    default_row_type: "type[DefaultRowT]"
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
        default_row_type: "Optional[type[DefaultRowT]]" = None,
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
            logger.warning("OpenTelemetry not installed, skipping OpenTelemetry setup.")  # type: ignore[unreachable]
            return
        self._tracer = trace.get_tracer(  # type: ignore[assignment]
            self.instrumentation_config.service_name,
            # __version__ # Consider adding version here if available
        )

    def _setup_prometheus(self) -> None:  # pragma: no cover
        """Set up Prometheus metrics with proper labeling and semantic naming."""
        try:
            service_name = self.instrumentation_config.service_name
            custom_tag_keys = list(self.instrumentation_config.custom_tags.keys())

            # Database operation metrics
            self._query_counter = Counter(  # type: ignore[misc]
                f"{service_name}_db_operations_total",
                "Total number of database operations executed",
                ["operation", "status", "db_system", *custom_tag_keys],
            )
            self._error_counter = Counter(  # type: ignore[misc]
                f"{service_name}_db_errors_total",
                "Total number of database errors",
                ["operation", "error_type", "db_system", *custom_tag_keys],
            )
            self._latency_histogram = Histogram(  # type: ignore[misc]
                f"{service_name}_db_operation_duration_seconds",
                "Database operation duration in seconds",
                ["operation", "db_system", *custom_tag_keys],
                buckets=self.instrumentation_config.prometheus_latency_buckets
                or [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10],  # Default buckets
            )

            # Connection pool metrics
            self._pool_latency_histogram = Histogram(  # type: ignore[misc]
                f"{service_name}_db_pool_operation_duration_seconds",
                "Database connection pool operation duration in seconds",
                ["operation", "db_system", *custom_tag_keys],
                buckets=self.instrumentation_config.prometheus_latency_buckets
                or [0.001, 0.005, 0.01, 0.05, 0.1, 5, 10],  # Buckets for pool operations
            )
            self._pool_connections_gauge = Gauge(  # type: ignore[misc]
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
            expression, (exp.Select, exp.Values, exp.Table, exp.Show, exp.Describe, exp.Pragma)
        ):  # Added more types
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
            # func_to_execute is the original method (e.g., _execute_impl)
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


class SyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT, DefaultRowT], SyncInstrumentationMixin, ABC):
    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "Optional[type[DefaultRowT]]" = None,
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
        # SyncInstrumentationMixin has no __init__

    def _build_statement(
        self,
        statement_input: "Union[SQL, Statement, QueryBuilder[Any]]",
        config: "Optional[SQLConfig]",
        *filters: "StatementFilter",
    ) -> "SQL":
        sql_obj: SQL
        if isinstance(statement_input, SQL):
            sql_obj = statement_input
        elif isinstance(statement_input, QueryBuilder):
            sql_obj = statement_input.to_statement(config=config or self.config)
        else:  # str or exp.Expression
            sql_obj = SQL(statement_input, dialect=self.dialect, config=config or self.config)

        for sql_filter in filters:
            if sql_filter is not None and hasattr(sql_filter, "append_to_statement"):
                sql_obj = sql_filter.append_to_statement(sql_obj)
            elif sql_filter is not None:
                logger.warning(
                    "Filter object %s of type %s does not support append_to_statement.", sql_filter, type(sql_filter)
                )
        return sql_obj

    @abstractmethod
    def _execute_impl(
        self,
        statement: "SQL",
        parameters: "Optional[SQLParameterType]" = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:  # Raw driver result
        """Actual execution implementation by concrete drivers, using the raw connection."""
        raise NotImplementedError

    @abstractmethod
    def _wrap_select_result(
        self,
        statement: "SQL",
        raw_driver_result: Any,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]":
        raise NotImplementedError

    @abstractmethod
    def _wrap_execute_result(
        self,
        statement: "SQL",
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> "ExecuteResult":
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
    ) -> "SelectResult[ModelDTOT]": ...

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
    ) -> "SelectResult[dict[str, Any]]": ...

    @overload
    def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ExecuteResult": ...

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
    ) -> "SelectResult[ModelDTOT]": ...

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
    ) -> "SelectResult[dict[str, Any]]": ...

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
    ) -> "SelectResult[ModelDTOT]": ...

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
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult]": ...

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[ConnectionT]" = None,
        config_override: "Optional[SQLConfig]" = None,  # Renamed from 'config' to avoid clash
        **kwargs: Any,
    ) -> "Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]], ExecuteResult]":
        with instrument_operation(self, "execute", "database"):
            sql_statement = self._build_statement(statement, config_override, *filters)
            raw_driver_result = self._execute_impl(
                statement=sql_statement,
                parameters=parameters,
                connection=self._connection(connection),
                config=config_override or self.config,
                **kwargs,
            )
            if CommonDriverAttributes.returns_rows(sql_statement.expression):
                return self._wrap_select_result(sql_statement, raw_driver_result, schema_type=schema_type, **kwargs)
            return self._wrap_execute_result(sql_statement, raw_driver_result, **kwargs)

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[ExecuteResult]]",  # Typically for DML builders
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        # schema_type is usually not relevant for execute_many's primary result
        connection: "Optional[ConnectionT]" = None,
        config_override: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ExecuteResult":
        with instrument_operation(self, "execute_many", "database"):
            sql_statement = self._build_statement(statement, config_override, *filters)
            raw_driver_result = self._execute_impl(
                statement=sql_statement,
                parameters=parameters,
                connection=self._connection(connection),
                config=config_override or self.config,
                is_many=True,
                **kwargs,
            )
            return self._wrap_execute_result(sql_statement, raw_driver_result, **kwargs)

    def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config_override: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> str:
        with instrument_operation(self, "execute_script", "database"):
            script_output = self._execute_impl(
                statement=SQL(
                    statement,
                    parameters,
                    *filters,
                    dialect=self.dialect,
                    config=config_override or self.config,
                ),
                parameters=parameters,
                connection=self._connection(connection),
                config=config_override or self.config,
                is_script=True,
                **kwargs,
            )
            return cast("str", script_output)


class AsyncDriverAdapterProtocol(CommonDriverAttributes[ConnectionT, DefaultRowT], AsyncInstrumentationMixin, ABC):
    def __init__(
        self,
        connection: "ConnectionT",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "Optional[type[DefaultRowT]]" = None,
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
        statement_input: "Union[SQL, Statement, QueryBuilder[Any]]",
        config: "Optional[SQLConfig]",
        *filters: "StatementFilter",
    ) -> "SQL":
        sql_obj: SQL
        if isinstance(statement_input, SQL):
            sql_obj = statement_input
        elif isinstance(statement_input, QueryBuilder):
            sql_obj = statement_input.to_statement(config=config or self.config)
        else:  # str or exp.Expression
            sql_obj = SQL(statement_input, dialect=self.dialect, config=config or self.config)

        for sql_filter in filters:
            if sql_filter is not None and hasattr(sql_filter, "append_to_statement"):
                sql_obj = sql_filter.append_to_statement(sql_obj)
            elif sql_filter is not None:
                logger.warning(
                    "Filter object %s of type %s does not support append_to_statement.", sql_filter, type(sql_filter)
                )
        return sql_obj

    @abstractmethod
    async def _execute_impl(
        self,
        statement: "SQL",
        parameters: "Optional[SQLParameterType]" = None,
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:  # Raw driver result
        raise NotImplementedError

    @abstractmethod
    async def _wrap_select_result(
        self,
        statement: "SQL",
        raw_driver_result: Any,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]":
        raise NotImplementedError

    @abstractmethod
    async def _wrap_execute_result(
        self,
        statement: "SQL",
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> "ExecuteResult":
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
    ) -> "SelectResult[ModelDTOT]": ...

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
    ) -> "SelectResult[dict[str, Any]]": ...

    @overload
    async def execute(
        self,
        statement: "Union[InsertBuilder, UpdateBuilder, DeleteBuilder]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ExecuteResult": ...

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
    ) -> "SelectResult[ModelDTOT]": ...

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
    ) -> "SelectResult[dict[str, Any]]": ...

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
    ) -> "SelectResult[ModelDTOT]": ...

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
    ) -> "Union[SelectResult[dict[str, Any]], ExecuteResult]": ...

    async def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[Any]]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        connection: "Optional[ConnectionT]" = None,
        config_override: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]], ExecuteResult]":
        async with instrument_operation_async(self, "execute", "database"):
            sql_statement = self._build_statement(statement, config_override, *filters)
            raw_driver_result = await self._execute_impl(
                statement=sql_statement,
                parameters=parameters,
                connection=self._connection(connection),
                config=config_override or self.config,
                **kwargs,
            )
            if CommonDriverAttributes.returns_rows(sql_statement.expression):
                return await self._wrap_select_result(
                    sql_statement, raw_driver_result, schema_type=schema_type, **kwargs
                )
            return await self._wrap_execute_result(sql_statement, raw_driver_result, **kwargs)

    async def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder[ExecuteResult]]",
        parameters: "Optional[Sequence[SQLParameterType]]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config_override: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "ExecuteResult":
        async with instrument_operation_async(self, "execute_many", "database"):
            sql_statement = self._build_statement(statement, config_override, *filters)
            raw_driver_result = await self._execute_impl(
                statement=sql_statement,
                parameters=parameters,
                connection=self._connection(connection),
                config=config_override or self.config,
                is_many=True,
                **kwargs,
            )
            return await self._wrap_execute_result(sql_statement, raw_driver_result, **kwargs)

    async def execute_script(
        self,
        statement: "Union[str, SQL]",
        parameters: "Optional[SQLParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[ConnectionT]" = None,
        config_override: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> str:
        async with instrument_operation_async(self, "execute_script", "database"):
            script_output = await self._execute_impl(
                statement=SQL(
                    statement,
                    parameters,
                    *filters,
                    dialect=self.dialect,
                    config=config_override or self.config,
                ),
                parameters=parameters,
                connection=self._connection(connection),
                config=config_override or self.config,
                is_script=True,
                **kwargs,
            )
            return cast("str", script_output)


DriverAdapterProtocol = Union[
    SyncDriverAdapterProtocol[ConnectionT, DefaultRowT], AsyncDriverAdapterProtocol[ConnectionT, DefaultRowT]
]
