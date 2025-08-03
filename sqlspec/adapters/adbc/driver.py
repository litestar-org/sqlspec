# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import decimal
import logging
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlspec.adapters.adbc.pipeline_steps import adbc_null_parameter_pipeline_step
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


__all__ = ("AdbcCursor", "AdbcDriver", "get_adbc_statement_config")


def get_adbc_statement_config(detected_dialect: str) -> StatementConfig:
    """Create ADBC statement configuration with enhanced pipeline architecture."""
    logger.debug("Creating ADBC statement config for dialect: %s", detected_dialect)
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

    # Configure post-processing pipeline steps for PostgreSQL NULL handling
    post_process_steps = None
    if detected_dialect in {"postgres", "postgresql"}:
        post_process_steps = [adbc_null_parameter_pipeline_step]

    return StatementConfig(
        dialect=detected_dialect,
        # Enhanced pipeline steps with precise execution control
        pre_process_steps=None,  # No pre-processing needed for ADBC
        post_process_steps=post_process_steps,  # NULL handling after parameterization
        # Core pipeline configuration
        enable_parsing=True,
        enable_transformations=True,  # Enables parameterize_literals_step
        enable_validation=True,
        enable_caching=True,
        # Parameter processing configuration
        parameter_config=parameter_config,
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
        tuple: list,
        dict: lambda x: x,
    }

    # PostgreSQL-specific type handling
    if dialect == "postgres":
        # PostgreSQL JSON types - convert dict to JSON string
        type_map[dict] = lambda x: to_json(x) if x is not None else None

        # Note: PostgreSQL arrays are handled natively by ADBC driver
        # ADBC can pass Python lists directly to PostgreSQL without conversion
        # The has_native_list_expansion=True flag indicates this capability

    return type_map


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
            statement_config = get_adbc_statement_config(detected_dialect)

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

    # Note: ADBC type conversions are now handled by the centralized parameter system
    # via type_coercion_map in get_type_coercion_map() function above.
    # This eliminates the need for driver-specific parameter conversion overrides.

    def _handle_single_param_list(self, sql: str, params: "list[Any]") -> "list[Any]":
        """Handle special case where a single-element list/tuple is passed for a single parameter.

        When we have a SQL query with one parameter placeholder and receive a list with
        one element that is itself a list or tuple, we need to determine if this is:
        1. A single parameter that should be an array (keep as is)
        2. A mistaken way of passing a single parameter (extract the value)

        Uses AST-based parameter counting for reliability.
        """
        # Count parameter placeholders using AST parsing for accuracy
        try:
            import sqlglot

            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            # Count unique parameter placeholders in the AST
            param_placeholders = set()
            for node in parsed.walk():
                if isinstance(node, sqlglot.exp.Placeholder):
                    param_placeholders.add(node.this)
            param_count = len(param_placeholders)
        except Exception:
            # Fallback to string counting if AST parsing fails
            # This is still fragile but better than crashing
            param_count = sql.count("$1") + sql.count("$2") + sql.count("?") + sql.count("%s")

        # If we have exactly one parameter placeholder and one parameter that is a list or tuple
        # and the list/tuple contains exactly one non-list/non-tuple element, extract it
        if (
            param_count == 1
            and len(params) == 1
            and isinstance(params[0], (list, tuple))
            and len(params[0]) == 1
            and not isinstance(params[0][0], (list, tuple))
        ):
            return list(params[0])

        return params
