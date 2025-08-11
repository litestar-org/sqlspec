"""ADBC driver implementation for Arrow Database Connectivity.

This module provides ADBC driver integration with support for:
- Multi-dialect database connections through ADBC
- Arrow-native data handling with type coercion
- Parameter style conversion for different database backends
- Transaction management with proper error handling
"""

import contextlib
import datetime
import decimal
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlglot import exp

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import MissingDependencyError, SQLParsingError, SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._types import AdbcConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.driver import ExecutionResult

__all__ = ("AdbcCursor", "AdbcDriver", "AdbcExceptionHandler", "get_adbc_statement_config")

logger = get_logger("adapters.adbc")

DIALECT_PATTERNS = {
    "postgres": ["postgres", "postgresql"],
    "bigquery": ["bigquery"],
    "sqlite": ["sqlite", "flight", "flightsql"],
    "duckdb": ["duckdb"],
    "mysql": ["mysql"],
    "snowflake": ["snowflake"],
}

DIALECT_PARAMETER_STYLES = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NAMED_COLON]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
}


def _adbc_ast_transformer(expression: Any, parameters: Any) -> tuple[Any, Any]:
    """ADBC-specific AST transformer for NULL parameter handling.

    For PostgreSQL, this transformer replaces NULL parameter placeholders with NULL literals
    in the AST to prevent Arrow from inferring 'na' types which cause binding errors.

    The transformer:
    1. Detects None parameters in the parameter list
    2. Replaces corresponding placeholders in the AST with NULL literals
    3. Removes the None parameters from the list
    4. Renumbers remaining placeholders to maintain correct mapping

    Args:
        expression: SQLGlot AST expression
        parameters: Parameter values that may contain None

    Returns:
        Tuple of (modified_expression, cleaned_parameters)
    """
    if not parameters:
        return expression, parameters

    # Detect NULL parameter positions
    null_positions = set()
    if isinstance(parameters, (list, tuple)):
        for i, param in enumerate(parameters):
            if param is None:
                null_positions.add(i)
    elif isinstance(parameters, dict):
        for key, param in parameters.items():
            if param is None:
                try:
                    if isinstance(key, str) and key.lstrip("$").isdigit():
                        param_num = int(key.lstrip("$"))
                        null_positions.add(param_num - 1)
                except ValueError:
                    pass

    if not null_positions:
        return expression, parameters

    # Track position for QMARK-style placeholders
    qmark_position = [0]

    def transform_node(node: Any) -> Any:
        """Transform parameter nodes to NULL literals and renumber remaining ones."""
        # Handle QMARK-style placeholders (?, ?, ?)
        if isinstance(node, exp.Placeholder) and (not hasattr(node, "this") or node.this is None):
            current_pos = qmark_position[0]
            qmark_position[0] += 1

            if current_pos in null_positions:
                return exp.Null()
            # Don't renumber QMARK placeholders - they stay as ?
            return node

        # Handle PostgreSQL-style placeholders ($1, $2, etc.)
        if isinstance(node, exp.Placeholder) and hasattr(node, "this") and node.this is not None:
            try:
                param_str = str(node.this).lstrip("$")
                param_num = int(param_str)
                param_index = param_num - 1  # Convert to 0-based

                if param_index in null_positions:
                    return exp.Null()
                # Renumber placeholder to account for removed NULLs
                nulls_before = sum(1 for idx in null_positions if idx < param_index)
                new_param_num = param_num - nulls_before
                return exp.Placeholder(this=f"${new_param_num}")
            except (ValueError, AttributeError):
                pass

        # Handle generic parameter nodes
        if isinstance(node, exp.Parameter) and hasattr(node, "this"):
            try:
                param_str = str(node.this)
                param_num = int(param_str)
                param_index = param_num - 1  # Convert to 0-based

                if param_index in null_positions:
                    return exp.Null()
                # Renumber parameter to account for removed NULLs
                nulls_before = sum(1 for idx in null_positions if idx < param_index)
                new_param_num = param_num - nulls_before
                return exp.Parameter(this=str(new_param_num))
            except (ValueError, AttributeError):
                pass

        return node

    # Transform the AST
    modified_expression = expression.transform(transform_node)

    # Remove NULL parameters from the parameter list
    cleaned_params: Any
    if isinstance(parameters, (list, tuple)):
        cleaned_params = [p for i, p in enumerate(parameters) if i not in null_positions]
    elif isinstance(parameters, dict):
        cleaned_params_dict = {}
        new_num = 1
        for val in parameters.values():
            if val is not None:
                cleaned_params_dict[str(new_num)] = val
                new_num += 1
        cleaned_params = cleaned_params_dict
    else:
        cleaned_params = parameters

    return modified_expression, cleaned_params


def get_adbc_statement_config(detected_dialect: str) -> StatementConfig:
    """Create ADBC statement configuration for the specified dialect."""
    default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
        detected_dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
    )

    type_map = get_type_coercion_map(detected_dialect)

    sqlglot_dialect = "postgres" if detected_dialect == "postgresql" else detected_dialect

    parameter_config = ParameterStyleConfig(
        default_parameter_style=default_style,
        supported_parameter_styles=set(supported_styles),
        default_execution_parameter_style=default_style,
        supported_execution_parameter_styles=set(supported_styles),
        type_coercion_map=type_map,
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        preserve_parameter_format=True,
        ast_transformer=_adbc_ast_transformer if detected_dialect in {"postgres", "postgresql"} else None,
    )

    return StatementConfig(
        dialect=sqlglot_dialect,
        parameter_config=parameter_config,
        enable_parsing=True,
        enable_validation=True,
        enable_caching=True,
        enable_parameter_type_wrapping=True,
    )


def _convert_array_for_postgres_adbc(value: Any) -> Any:
    """Convert array values for PostgreSQL ADBC compatibility."""
    if isinstance(value, tuple):
        return list(value)
    return value


def get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Get type coercion map for Arrow/ADBC type handling."""
    type_map = {
        datetime.datetime: lambda x: x,
        datetime.date: lambda x: x,
        datetime.time: lambda x: x,
        decimal.Decimal: float,
        bool: lambda x: x,
        int: lambda x: x,
        float: lambda x: x,
        str: lambda x: x,
        bytes: lambda x: x,
        tuple: _convert_array_for_postgres_adbc,
        list: _convert_array_for_postgres_adbc,
        dict: lambda x: x,
    }

    if dialect in {"postgres", "postgresql"}:
        type_map[dict] = lambda x: to_json(x) if x is not None else None

    return type_map


class AdbcCursor:
    """Context manager for ADBC cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Cursor] = None

    def __enter__(self) -> "Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()  # type: ignore[no-untyped-call]


class AdbcExceptionHandler:
    """Custom sync context manager for handling ADBC database exceptions."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        try:
            from adbc_driver_manager.dbapi import DatabaseError, IntegrityError, OperationalError, ProgrammingError

            if issubclass(exc_type, IntegrityError):
                e = exc_val
                msg = f"ADBC integrity constraint violation: {e}"
                raise SQLSpecError(msg) from e
            if issubclass(exc_type, ProgrammingError):
                e = exc_val
                error_msg = str(e).lower()
                if "syntax" in error_msg or "parse" in error_msg:
                    msg = f"ADBC SQL syntax error: {e}"
                    raise SQLParsingError(msg) from e
                msg = f"ADBC programming error: {e}"
                raise SQLSpecError(msg) from e
            if issubclass(exc_type, OperationalError):
                e = exc_val
                msg = f"ADBC operational error: {e}"
                raise SQLSpecError(msg) from e
            if issubclass(exc_type, DatabaseError):
                e = exc_val
                msg = f"ADBC database error: {e}"
                raise SQLSpecError(msg) from e
        except ImportError:
            pass
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected database operation error: {e}"
            raise SQLSpecError(msg) from e


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC driver for Arrow Database Connectivity.

    Provides database connectivity through ADBC with support for:
    - Multi-database dialect support with automatic detection
    - Arrow-native data handling with type coercion
    - Parameter style conversion for different backends
    - Transaction management with proper error handling
    """

    __slots__ = ("_detected_dialect", "dialect")

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        self._detected_dialect = self._get_dialect(connection)

        if statement_config is None:
            cache_config = get_cache_config()
            base_config = get_adbc_statement_config(self._detected_dialect)
            enhanced_config = base_config.replace(
                enable_caching=cache_config.compiled_cache_enabled, enable_parsing=True, enable_validation=True
            )
            statement_config = enhanced_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.dialect = statement_config.dialect

    @staticmethod
    def _ensure_pyarrow_installed() -> None:
        """Ensure PyArrow is installed for Arrow operations."""
        from sqlspec.typing import PYARROW_INSTALLED

        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="arrow")

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Detect database dialect from ADBC connection information."""
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    logger.debug("ADBC dialect detected: %s (from %s/%s)", dialect, vendor_name, driver_name)
                    return dialect
        except Exception as e:
            logger.debug("ADBC dialect detection failed: %s", e)

        logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Execute rollback for PostgreSQL after transaction failure."""
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")
                logger.debug("PostgreSQL rollback executed after ADBC transaction failure")

    def _handle_postgres_empty_parameters(self, parameters: Any) -> Any:
        """Process empty parameters for PostgreSQL compatibility."""
        if self.dialect == "postgres" and isinstance(parameters, dict) and not parameters:
            return None
        return parameters

    def with_cursor(self, connection: "AdbcConnection") -> "AdbcCursor":
        """Create context manager for ADBC cursor."""
        return AdbcCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return AdbcExceptionHandler()

    def _try_special_handling(self, cursor: "Cursor", statement: SQL) -> "Optional[SQLResult]":
        """Handle ADBC-specific operations.

        Args:
            cursor: ADBC cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        _ = (cursor, statement)
        return None

    def _execute_many(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using batch processing."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        try:
            if not prepared_parameters:
                cursor._rowcount = 0
                row_count = 0
            elif isinstance(prepared_parameters, list) and prepared_parameters:
                processed_params = []
                for param_set in prepared_parameters:
                    postgres_compatible = self._handle_postgres_empty_parameters(param_set)
                    formatted_params = self.prepare_driver_parameters(
                        postgres_compatible, self.statement_config, is_many=False
                    )
                    processed_params.append(formatted_params)

                cursor.executemany(sql, processed_params)
                row_count = cursor.rowcount if cursor.rowcount is not None else -1
            else:
                cursor.executemany(sql, prepared_parameters)
                row_count = cursor.rowcount if cursor.rowcount is not None else -1

        except Exception:
            self._handle_postgres_rollback(cursor)
            logger.exception("ADBC executemany failed")
            raise

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement with ADBC-specific data handling."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        try:
            postgres_compatible_params = self._handle_postgres_empty_parameters(prepared_parameters)
            cursor.execute(sql, parameters=postgres_compatible_params)

        except Exception:
            self._handle_postgres_rollback(cursor)
            raise

        if statement.returns_rows():
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

        row_count = cursor.rowcount if cursor.rowcount is not None else -1
        return self.create_execution_result(cursor, rowcount_override=row_count)

    def _execute_script(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with ADBC-specific transaction handling."""
        if statement.is_script:
            sql = statement._raw_sql
            prepared_parameters: list[Any] = []
        else:
            sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_rowcount = 0

        try:
            for stmt in statements:
                if prepared_parameters:
                    postgres_compatible_params = self._handle_postgres_empty_parameters(prepared_parameters)
                    cursor.execute(stmt, parameters=postgres_compatible_params)
                else:
                    cursor.execute(stmt)
                successful_count += 1
                if cursor.rowcount is not None:
                    last_rowcount = cursor.rowcount
        except Exception:
            self._handle_postgres_rollback(cursor)
            logger.exception("ADBC script execution failed")
            raise

        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=successful_count,
            rowcount_override=last_rowcount,
            is_script_result=True,
        )

    def begin(self) -> None:
        """Begin database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("BEGIN")
        except Exception as e:
            msg = f"Failed to begin ADBC transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("ROLLBACK")
        except Exception as e:
            msg = f"Failed to rollback ADBC transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("COMMIT")
        except Exception as e:
            msg = f"Failed to commit ADBC transaction: {e}"
            raise SQLSpecError(msg) from e
