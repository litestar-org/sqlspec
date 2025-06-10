"""Common driver attributes and utilities."""

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
    Union,
)

from sqlglot import exp

from sqlspec.config import InstrumentationConfig
from sqlspec.exceptions import NotFoundError
from sqlspec.statement.sql import SQLConfig
from sqlspec.typing import (
    ConnectionT,
    Counter,
    DictRow,
    Gauge,
    Histogram,
    RowT,
    T,
    Tracer,
    trace,
)
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.statement.parameters import ParameterInfo, ParameterStyle

__all__ = ("CommonDriverAttributesMixin",)


logger = get_logger("driver")


class CommonDriverAttributesMixin(ABC, Generic[ConnectionT, RowT]):
    """Enhanced common attributes and methods for driver adapters with instrumentation."""

    dialect: "Any"  # DialectType
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
    """Indicates if the driver supports native Apache Arrow operations."""

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
            return CommonDriverAttributesMixin.returns_rows(expression.expressions[-1])
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

    def _convert_parameters_to_driver_format(
        self, sql: str, parameters: Any, target_style: "Optional[ParameterStyle]" = None
    ) -> Any:
        """Convert parameters from any format to the format expected by the driver.

        This method analyzes the SQL to understand what parameter style is used
        and converts the input parameters to match.

        Args:
            sql: The SQL string with placeholders
            parameters: The parameters in any format (dict, list, tuple, scalar)
            target_style: Optional override for the target parameter style

        Returns:
            Parameters in the format expected by the database driver
        """
        if parameters is None:
            return None

        from collections.abc import Mapping, Sequence

        from sqlspec.statement.parameters import ParameterStyle, ParameterValidator

        # Extract parameter info from the SQL
        validator = ParameterValidator()
        param_info_list = validator.extract_parameters(sql)

        if not param_info_list:
            # No parameters in SQL, return None
            return None

        # Determine the target style from the SQL if not provided
        if target_style is None:
            target_style = self._get_placeholder_style()

        # Analyze what format the driver expects based on the placeholder style
        driver_expects_dict = target_style in {
            "named_colon", "oracle_numeric", "named_at", "named_dollar", "pyformat_named"
        }

        # Handle single scalar parameter
        if (
            len(param_info_list) == 1
            and not isinstance(parameters, (dict, list, tuple, Mapping, Sequence))
        ):
            if driver_expects_dict:
                # Convert scalar to dict
                param_info = param_info_list[0]
                if param_info.name:
                    return {param_info.name: parameters}
                return {f"param_{param_info.ordinal}": parameters}
            # Return as single-element list for positional
            return [parameters]

        # Convert between dict and list/tuple formats
        if driver_expects_dict and not isinstance(parameters, (dict, Mapping)):
            # Convert positional to dict
            if isinstance(parameters, (list, tuple, Sequence)):
                result = {}
                for i, (param_info, value) in enumerate(zip(param_info_list, parameters)):
                    if param_info.name:
                        # Special handling for Oracle numeric style where name is the position (1, 2, etc.)
                        if param_info.style == ParameterStyle.ORACLE_NUMERIC and param_info.name.isdigit():
                            # Oracle uses string keys even for numeric placeholders
                            result[param_info.name] = value
                        else:
                            result[param_info.name] = value
                    else:
                        # Use param_N format for unnamed placeholders
                        result[f"param_{i}"] = value
                return result

        elif not driver_expects_dict and isinstance(parameters, (dict, Mapping)):
            # Convert dict to positional
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                # Already in param_N format, extract values in order
                result = []
                for i in range(len(param_info_list)):
                    key = f"param_{i}"
                    if key in parameters:
                        result.append(parameters[key])
                return result

            # Convert named dict to positional based on parameter order in SQL
            result = []
            for param_info in param_info_list:
                if param_info.name and param_info.name in parameters:
                    result.append(parameters[param_info.name])
                elif f"param_{param_info.ordinal}" in parameters:
                    result.append(parameters[f"param_{param_info.ordinal}"])
                else:
                    # Try to match by position if we have a simple dict
                    # This handles cases where SQL was converted from named to positional
                    # but parameters are still in named format
                    param_values = list(parameters.values())
                    if param_info.ordinal < len(param_values):
                        result.append(param_values[param_info.ordinal])
            return result if result else list(parameters.values())

        # Parameters are already in the expected format
        return parameters
