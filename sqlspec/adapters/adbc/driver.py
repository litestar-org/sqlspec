# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import decimal
import logging
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlspec.adapters.adbc.pipeline_steps import adbc_null_transform_step
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._types import AdbcConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult


def create_adbc_statement_config(detected_dialect: str) -> StatementConfig:
    """Create ADBC statement configuration for a specific dialect."""
    default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
        detected_dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
    )

    # Get type coercion map for this dialect
    type_map = get_type_coercion_map(detected_dialect)

    parameter_config = ParameterStyleConfig(
        default_parameter_style=default_style,
        supported_parameter_styles=set(supported_styles),
        type_coercion_map=type_map,
        has_native_list_expansion=True,  # ADBC handles lists natively
        needs_static_script_compilation=True,  # ADBC requires static compilation for scripts
    )

    # Add PostgreSQL null transform step if needed
    custom_pipeline_steps = None
    if detected_dialect in {"postgres", "postgresql"}:
        try:
            from sqlspec.adapters.adbc.pipeline_steps import adbc_null_transform_step

            custom_pipeline_steps = [adbc_null_transform_step]
        except ImportError:
            pass

    return StatementConfig(
        dialect=detected_dialect, parameter_config=parameter_config, custom_pipeline_steps=custom_pipeline_steps
    )


def get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Get type coercion map for Arrow/ADBC type handling."""
    # Basic type mappings for Arrow/ADBC compatibility
    type_map = {
        # Handle None/NULL values explicitly
        type(None): lambda _: None,
        # Date/Time types - DO NOT convert to strings, PostgreSQL expects actual date/time objects
        datetime.datetime: lambda x: x,
        datetime.date: lambda x: x,
        datetime.time: lambda x: x,
        decimal.Decimal: float,
        bool: lambda x: x,
        int: lambda x: x,
        float: lambda x: x,
        str: lambda x: x,
        bytes: lambda x: x,
        list: lambda x: x,
        tuple: list,
        dict: lambda x: x,
    }

    # PostgreSQL-specific type handling
    if dialect == "postgres":
        # PostgreSQL arrays need special handling
        type_map[list] = lambda x: x if x is not None else []
        # PostgreSQL JSON types - convert dict to JSON string
        type_map[dict] = lambda x: to_json(x) if x is not None else None

    return type_map


__all__ = ("AdbcCursor", "AdbcDriver", "create_adbc_statement_config")

logger = logging.getLogger("sqlspec")


class AdbcCursor:
    """Context manager for ADBC cursor management."""

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Cursor] = None

    def __enter__(self) -> "Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()  # type: ignore[no-untyped-call]


DIALECT_PATTERNS = {
    "postgres": ["postgres", "postgresql"],
    "bigquery": ["bigquery"],
    "sqlite": ["sqlite", "flight", "flightsql"],
    "duckdb": ["duckdb"],
    "mysql": ["mysql"],
    "snowflake": ["snowflake"],
}

# Parameter style configurations
DIALECT_PARAMETER_STYLES = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NAMED_COLON]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
}


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC Sync Driver Adapter with modern architecture."""

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        # Detect dialect from connection before using it
        detected_dialect = self._get_dialect(connection)

        # Create default config if none provided
        if statement_config is None:
            statement_config = create_adbc_statement_config(detected_dialect)
        elif detected_dialect in {"postgres", "postgresql"} and not statement_config.custom_pipeline_steps:
            # If PostgreSQL config provided without null transform, we need to add it

            statement_config = statement_config.replace(custom_pipeline_steps=[adbc_null_transform_step])

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

        # Set dialect attribute for compatibility
        self.dialect = statement_config.dialect

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name."""
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            # Check against known patterns
            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    return dialect
        except Exception:
            logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Handle PostgreSQL rollback requirement after failed transaction."""
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")

    def _handle_postgres_empty_params(self, params: "Any") -> "Any":
        """Handle empty parameters for PostgreSQL to avoid struct type errors."""
        if self.dialect == "postgres" and isinstance(params, dict) and not params:
            return None
        return params

    def with_cursor(self, connection: "AdbcConnection") -> "AdbcCursor":
        return AdbcCursor(connection)

    # Remove override - default implementation should handle this correctly

    def _try_special_handling(self, cursor: "Cursor", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for ADBC-specific special operations.

        ADBC handles scripts by executing multiple statements sequentially.
        """
        if statement.is_script:
            try:
                sql, parameters = statement.compile()
                statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
                statement_count = len(statements)

                for stmt in statements:
                    if stmt.strip():
                        prepared_params = self.prepare_driver_parameters(
                            self._handle_postgres_empty_params(parameters), self.statement_config, is_many=False
                        )
                        cursor.execute(stmt, parameters=prepared_params)

                # Create ExecutionResult and build SQLResult directly
                execution_result = self.create_execution_result(
                    cursor,
                    statement_count=statement_count,
                    successful_statements=statement_count,  # Assume all successful if no exception
                    is_script_result=True,
                )
                return self.build_statement_result(statement, execution_result)

            except Exception as e:
                self._handle_postgres_rollback(cursor)
                raise e from e

        return None

    def _execute_many(self, cursor: "Cursor", sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """ADBC executemany implementation."""
        try:
            # ADBC requires at least one parameter set for executemany with parameterized queries
            # Use AST-based parameter detection instead of naive string searching
            if not prepared_params:
                cursor._rowcount = 0
                row_count = 0
            else:
                cursor.executemany(sql, prepared_params)
                # For executemany, get row count
                row_count = cursor.rowcount if cursor.rowcount is not None else -1

        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(
        self, cursor: "Cursor", sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """ADBC single execution."""
        try:
            # Handle PostgreSQL empty params and single param lists
            final_params = self._handle_postgres_empty_params(prepared_params)
            final_params = self._handle_single_param_list(sql, final_params)
            cursor.execute(sql, parameters=final_params)

        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            if fetched_data and isinstance(fetched_data[0], tuple):
                dict_data: list[dict[Any, Any]] = [dict(zip(column_names, row)) for row in fetched_data]
            else:
                dict_data = fetched_data  # type: ignore[assignment]

            return self.create_execution_result(
                cursor,
                selected_data=cast("list[dict[str, Any]]", dict_data),
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        # For non-SELECT operations, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else -1
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def begin(self) -> None:
        """Begin database transaction."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback database transaction."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("ROLLBACK")

    def commit(self) -> None:
        """Commit database transaction."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("COMMIT")

    def _prepare_driver_parameters(self, parameters: "Any") -> "Any":
        """Convert parameters to the format expected by ADBC driver.

        ADBC/Arrow requires special handling for certain types:
        - NULL values need to be handled properly
        - Date/time types may need conversion
        - Arrays and complex types need proper formatting

        Overrides base class to provide ADBC-specific type conversions.
        The base class handles dict->list conversion for positional parameter styles.
        """
        # Use base class for parameter structure conversion
        base_params = self.prepare_driver_parameters(parameters, self.statement_config, is_many=False)

        if not base_params:
            return []

        # Apply ADBC-specific type conversions
        if isinstance(base_params, list):
            return [self._convert_single_parameter(p) for p in base_params]
        if isinstance(base_params, dict):
            return {k: self._convert_single_parameter(v) for k, v in base_params.items()}
        return [self._convert_single_parameter(base_params)]

    def _convert_single_parameter(self, param: "Any") -> "Any":
        """Convert a single parameter to Arrow-compatible type.

        This handles special cases for Arrow/ADBC:
        - None values (Arrow 'na' type issues)
        - Date/time conversions
        - Decimal to float conversions
        - JSON/dict conversions
        """
        if param is None:
            # Keep None as is - ADBC should handle it
            return None
        if isinstance(param, decimal.Decimal):
            # Convert Decimal to float for Arrow compatibility
            return float(param)
        if isinstance(param, (datetime.date, datetime.time, datetime.datetime)):
            # Keep datetime objects as is - ADBC should handle them
            # The driver will convert them appropriately based on the column type
            return param
        if isinstance(param, (list, tuple)):
            # Arrays should be passed as lists
            return list(param) if isinstance(param, tuple) else param
        if isinstance(param, dict):
            # For JSON/JSONB types - convert to string if needed
            return to_json(param) if self.dialect == "postgres" else param
        # Keep other types as is
        return param

    def _convert_parameters_for_arrow(self, params: "list[Any]") -> "list[Any]":
        """Convert parameters to Arrow-compatible types.

        This handles special cases for Arrow/ADBC:
        - None values (Arrow 'na' type issues)
        - Date/time conversions
        - Decimal to float conversions
        """
        return [self._convert_single_parameter(param) for param in params]

    def _handle_single_param_list(self, sql: str, params: "list[Any]") -> "list[Any]":
        """Handle special case where a single-element list is passed for a single parameter.

        When we have a SQL query with one parameter placeholder and receive a list with
        one element that is itself a list, we need to determine if this is:
        1. A single parameter that should be an array (keep as is)
        2. A mistaken way of passing a single parameter (extract the value)

        For now, we'll check if the parameter count matches.
        """
        # Count parameter placeholders in SQL
        param_count = sql.count("$1") + sql.count("$2") + sql.count("?") + sql.count("%s")

        # If we have exactly one parameter placeholder and one parameter that is a list
        # and the list contains exactly one non-list element, extract it
        if (
            param_count == 1
            and len(params) == 1
            and isinstance(params[0], list)
            and len(params[0]) == 1
            and not isinstance(params[0][0], (list, tuple))
        ):
            return params[0]

        return params

    def _prepare_driver_parameters_many(self, parameters: "Any") -> "list[Any]":
        """Prepare parameters for execute_many operations.

        ADBC requires special handling for batch operations with Arrow.
        """
        if not parameters:
            return []

        # Convert each parameter set
        prepared = []
        for param_set in parameters:
            if isinstance(param_set, dict):
                # Named parameters - convert values
                values = list(param_set.values())
                prepared.append([self._convert_single_parameter(v) for v in values])
            elif isinstance(param_set, (list, tuple)):
                # Positional parameters
                prepared.append([self._convert_single_parameter(p) for p in param_set])
            else:
                # Single value parameter sets
                prepared.append([self._convert_single_parameter(param_set)])

        return prepared
