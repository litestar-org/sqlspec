"""Common driver attributes and utilities."""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    Optional,
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
    from sqlspec.statement.parameters import ParameterStyle

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

    def _convert_parameters_to_driver_format(  # noqa: C901
        self, sql: str, parameters: Any, target_style: "Optional[ParameterStyle]" = None
    ) -> Any:
        """Convert parameters to the format expected by the driver, but only when necessary.

        This method analyzes the SQL to understand what parameter style is used
        and only converts when there's a mismatch between provided parameters
        and what the driver expects.

        Args:
            sql: The SQL string with placeholders
            parameters: The parameters in any format (dict, list, tuple, scalar)
            target_style: Optional override for the target parameter style

        Returns:
            Parameters in the format expected by the database driver
        """
        if parameters is None:
            return None

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

        # Override target style based on what's actually in the SQL
        # This handles cases where the driver supports multiple styles
        if param_info_list:
            actual_styles = {p.style for p in param_info_list if p.style}
            if len(actual_styles) == 1:
                # All parameters use the same style - use that
                detected_style = actual_styles.pop()
                if detected_style != target_style:
                    # The SQL uses a different style than the driver default
                    target_style = detected_style

        # Analyze what format the driver expects based on the placeholder style
        driver_expects_dict = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.ORACLE_NUMERIC,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.PYFORMAT_NAMED,
        }

        # Check if parameters are already in the correct format
        params_are_dict = isinstance(parameters, (dict, Mapping))
        params_are_sequence = isinstance(parameters, (list, tuple, Sequence)) and not isinstance(
            parameters, (str, bytes)
        )

        # Single scalar parameter
        if len(param_info_list) == 1 and not params_are_dict and not params_are_sequence:
            if driver_expects_dict:
                # Convert scalar to dict
                param_info = param_info_list[0]
                if param_info.name:
                    return {param_info.name: parameters}
                return {f"param_{param_info.ordinal}": parameters}
            # Return as single-element list for positional
            return [parameters]

        # If formats match, check if conversion is still needed for special cases
        if driver_expects_dict and params_are_dict:
            # Special case: Oracle numeric style with named dict parameters
            if target_style == ParameterStyle.ORACLE_NUMERIC and all(
                p.name and p.name.isdigit() for p in param_info_list
            ):
                # If all parameters are numeric but named, convert to dict
                # SQL has numeric placeholders but params might have named keys
                # Only convert if keys don't match
                numeric_keys_expected = {p.name for p in param_info_list if p.name}
                if not numeric_keys_expected.issubset(parameters.keys()):
                    # Need to convert named keys to numeric positions
                    result = {}
                    param_values = list(parameters.values())
                    for param_info in param_info_list:
                        if param_info.name and param_info.ordinal < len(param_values):
                            result[param_info.name] = param_values[param_info.ordinal]
                    return result

            # Special case: Auto-generated param_N style when SQL expects specific names
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                # Check if SQL has different parameter names
                sql_param_names = {p.name for p in param_info_list if p.name}
                if sql_param_names and not any(name.startswith("param_") for name in sql_param_names):
                    # SQL has specific names, not param_N style - don't use these params as-is
                    # This likely indicates a mismatch in parameter generation
                    # For now, pass through and let validation catch it
                    pass

            # Otherwise, dict format matches - return as-is
            return parameters

        if not driver_expects_dict and params_are_sequence:
            # Formats match - return as-is
            return parameters

        # Formats don't match - need conversion
        if driver_expects_dict and params_are_sequence:
            # Convert positional to dict
            result = {}
            for i, (param_info, value) in enumerate(zip(param_info_list, parameters)):
                if param_info.name:
                    # Use the name from SQL
                    if param_info.style == ParameterStyle.ORACLE_NUMERIC and param_info.name.isdigit():
                        # Oracle uses string keys even for numeric placeholders
                        result[param_info.name] = value
                    else:
                        result[param_info.name] = value
                else:
                    # Use param_N format for unnamed placeholders
                    result[f"param_{i}"] = value
            return result

        if not driver_expects_dict and params_are_dict:
            # Convert dict to positional
            # First check if it's already in param_N format
            if all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                # Extract values in order
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
                    param_values = list(parameters.values())
                    if param_info.ordinal < len(param_values):
                        result.append(param_values[param_info.ordinal])
            return result or list(parameters.values())

        # This shouldn't happen, but return as-is
        return parameters

    def _split_script_statements(self, script: str) -> list[str]:
        """Split a SQL script into individual statements.

        This method uses a robust lexer-driven state machine to handle
        multi-statement scripts, including complex constructs like
        PL/SQL blocks, T-SQL batches, and nested blocks.

        Args:
            script: The SQL script to split

        Returns:
            A list of individual SQL statements

        Note:
            This is particularly useful for databases that don't natively
            support multi-statement execution (e.g., Oracle, some async drivers).
        """
        from sqlspec.statement.splitter import split_sql_script

        # Map database dialect names to splitter dialect names
        dialect_map = {
            "oracle": "oracle",
            "postgres": "postgresql",
            "postgresql": "postgresql",
            "mssql": "tsql",
            "tsql": "tsql",
            "sqlserver": "tsql",
            # Add more mappings as needed
        }

        # Get the dialect name
        dialect_name = str(self.dialect)
        splitter_dialect = dialect_map.get(dialect_name.lower(), dialect_name.lower())

        try:
            return split_sql_script(script, dialect=splitter_dialect)
        except ValueError as e:
            # Unsupported dialect, fall back to simple split
            logger.warning(
                "Dialect %s not supported by statement splitter, using simple split: %s", splitter_dialect, e
            )
            return self._simple_split_statements(script)

    def _simple_split_statements(self, script: str) -> list[str]:
        """Simple fallback splitting on semicolons (respects quotes and comments)."""
        statements = []
        current_statement = []

        in_single_quote = False
        in_double_quote = False

        i = 0
        while i < len(script):
            char = script[i]

            # Handle string literals
            if char == "'" and not in_double_quote:
                # Check for escaped quote
                if i + 1 < len(script) and script[i + 1] == "'":
                    current_statement.append(char)
                    current_statement.append("'")
                    i += 2
                    continue
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            current_statement.append(char)

            # Check for statement terminator
            if char == ";" and not in_single_quote and not in_double_quote:
                stmt = "".join(current_statement[:-1]).strip()  # Exclude semicolon
                if stmt:
                    statements.append(stmt + ";")  # Add semicolon back
                current_statement = []

            i += 1

        # Handle remaining content
        if current_statement:
            stmt = "".join(current_statement).strip()
            if stmt:
                statements.append(stmt)

        return statements
