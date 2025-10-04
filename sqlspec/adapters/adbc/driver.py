"""ADBC driver implementation for Arrow Database Connectivity.

Provides database connectivity through ADBC with support for multiple
database dialects, parameter style conversion, and transaction management.
"""

import contextlib
import datetime
import decimal
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlglot import exp

from sqlspec.adapters.adbc.data_dictionary import AdbcDataDictionary
from sqlspec.adapters.adbc.type_converter import ADBCTypeConverter
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    MissingDependencyError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import Empty
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._types import AdbcConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase

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
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
}


def _count_placeholders(expression: Any) -> int:
    """Count the number of unique parameter placeholders in a SQLGlot expression.

    For PostgreSQL ($1, $2) style: counts highest numbered parameter (e.g., $1, $1, $2 = 2)
    For QMARK (?) style: counts total occurrences (each ? is a separate parameter)
    For named (:name) style: counts unique parameter names

    Args:
        expression: SQLGlot AST expression

    Returns:
        Number of unique parameter placeholders expected
    """
    numeric_params = set()  # For $1, $2 style
    qmark_count = 0  # For ? style
    named_params = set()  # For :name style

    def count_node(node: Any) -> Any:
        nonlocal qmark_count
        if isinstance(node, exp.Parameter):
            # PostgreSQL style: $1, $2, etc.
            param_str = str(node)
            if param_str.startswith("$") and param_str[1:].isdigit():
                numeric_params.add(int(param_str[1:]))
            elif ":" in param_str:
                # Named parameter: :name
                named_params.add(param_str)
            else:
                # Other parameter formats
                named_params.add(param_str)
        elif isinstance(node, exp.Placeholder):
            # QMARK style: ?
            qmark_count += 1
        return node

    expression.transform(count_node)

    # Return the appropriate count based on parameter style detected
    if numeric_params:
        # PostgreSQL style: return highest numbered parameter
        return max(numeric_params)
    if named_params:
        # Named parameters: return count of unique names
        return len(named_params)
    # QMARK style: return total count
    return qmark_count


def _is_execute_many_parameters(parameters: Any) -> bool:
    """Check if parameters are in execute_many format (list/tuple of lists/tuples)."""
    return isinstance(parameters, (list, tuple)) and len(parameters) > 0 and isinstance(parameters[0], (list, tuple))


def _validate_parameter_counts(expression: Any, parameters: Any, dialect: str) -> None:
    """Validate parameter count against placeholder count in SQL."""
    placeholder_count = _count_placeholders(expression)
    is_execute_many = _is_execute_many_parameters(parameters)

    if is_execute_many:
        # For execute_many, validate each inner parameter set
        for i, param_set in enumerate(parameters):
            param_count = len(param_set) if isinstance(param_set, (list, tuple)) else 0
            if param_count != placeholder_count:
                msg = f"Parameter count mismatch in set {i}: {param_count} parameters provided but {placeholder_count} placeholders in SQL (dialect: {dialect})"
                raise SQLSpecError(msg)
    else:
        # For single execution, validate the parameter set directly
        param_count = (
            len(parameters)
            if isinstance(parameters, (list, tuple))
            else len(parameters)
            if isinstance(parameters, dict)
            else 0
        )

        if param_count != placeholder_count:
            msg = f"Parameter count mismatch: {param_count} parameters provided but {placeholder_count} placeholders in SQL (dialect: {dialect})"
            raise SQLSpecError(msg)


def _find_null_positions(parameters: Any) -> set[int]:
    """Find positions of None values in parameters for single execution."""
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
    return null_positions


def _adbc_ast_transformer(expression: Any, parameters: Any, dialect: str = "postgres") -> tuple[Any, Any]:
    """Transform AST to handle NULL parameters.

    Replaces NULL parameter placeholders with NULL literals in the AST
    to prevent Arrow from inferring 'na' types which cause binding errors.
    Validates parameter count before transformation.

    Args:
        expression: SQLGlot AST expression parsed with proper dialect
        parameters: Parameter values that may contain None
        dialect: SQLGlot dialect used for parsing (default: "postgres")

    Returns:
        Tuple of (modified_expression, cleaned_parameters)
    """
    if not parameters:
        return expression, parameters

    # Validate parameter count before transformation
    _validate_parameter_counts(expression, parameters, dialect)

    # For execute_many operations, skip AST transformation as different parameter
    # sets may have None values in different positions, making transformation complex
    if _is_execute_many_parameters(parameters):
        return expression, parameters

    # Find positions of None values for single execution
    null_positions = _find_null_positions(parameters)
    if not null_positions:
        return expression, parameters

    qmark_position = [0]

    def transform_node(node: Any) -> Any:
        if isinstance(node, exp.Placeholder) and (not hasattr(node, "this") or node.this is None):
            current_pos = qmark_position[0]
            qmark_position[0] += 1

            if current_pos in null_positions:
                return exp.Null()

            return node

        if isinstance(node, exp.Placeholder) and hasattr(node, "this") and node.this is not None:
            try:
                param_str = str(node.this).lstrip("$")
                param_num = int(param_str)
                param_index = param_num - 1

                if param_index in null_positions:
                    return exp.Null()

                nulls_before = sum(1 for idx in null_positions if idx < param_index)
                new_param_num = param_num - nulls_before
                return exp.Placeholder(this=f"${new_param_num}")
            except (ValueError, AttributeError):
                pass

        if isinstance(node, exp.Parameter) and hasattr(node, "this"):
            try:
                param_str = str(node.this)
                param_num = int(param_str)
                param_index = param_num - 1

                if param_index in null_positions:
                    return exp.Null()

                nulls_before = sum(1 for idx in null_positions if idx < param_index)
                new_param_num = param_num - nulls_before
                return exp.Parameter(this=str(new_param_num))
            except (ValueError, AttributeError):
                pass

        return node

    modified_expression = expression.transform(transform_node)

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
    """Create statement configuration for the specified dialect."""
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
    """Convert array values for PostgreSQL compatibility.

    Args:
        value: Value to convert

    Returns:
        Converted value (tuples become lists)
    """
    if isinstance(value, tuple):
        return list(value)
    return value


def get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Get type coercion map for Arrow type handling with dialect-aware type conversion.

    Args:
        dialect: Database dialect name

    Returns:
        Mapping of Python types to conversion functions
    """
    tc = ADBCTypeConverter(dialect)

    return {
        datetime.datetime: lambda x: x,
        datetime.date: lambda x: x,
        datetime.time: lambda x: x,
        decimal.Decimal: float,
        bool: lambda x: x,
        int: lambda x: x,
        float: lambda x: x,
        str: tc.convert_if_detected,
        bytes: lambda x: x,
        tuple: _convert_array_for_postgres_adbc,
        list: _convert_array_for_postgres_adbc,
        dict: lambda x: x,
    }


class AdbcCursor:
    """Context manager for cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AdbcConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Cursor] = None

    def __enter__(self) -> "Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()  # type: ignore[no-untyped-call]


class AdbcExceptionHandler:
    """Context manager for handling ADBC database exceptions.

    ADBC propagates underlying database errors. Exception mapping
    depends on the specific ADBC driver being used.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        self._map_adbc_exception(exc_val)

    def _map_adbc_exception(self, e: Any) -> None:
        """Map ADBC exception to SQLSpec exception.

        ADBC drivers may expose SQLSTATE codes or driver-specific codes.

        Args:
            e: ADBC exception instance
        """
        sqlstate = getattr(e, "sqlstate", None)

        if sqlstate:
            self._map_sqlstate_exception(e, sqlstate)
        else:
            self._map_message_based_exception(e)

    def _map_sqlstate_exception(self, e: Any, sqlstate: str) -> None:
        """Map SQLSTATE code to exception.

        Args:
            e: Exception instance
            sqlstate: SQLSTATE error code
        """
        if sqlstate == "23505":
            self._raise_unique_violation(e)
        elif sqlstate == "23503":
            self._raise_foreign_key_violation(e)
        elif sqlstate == "23502":
            self._raise_not_null_violation(e)
        elif sqlstate == "23514":
            self._raise_check_violation(e)
        elif sqlstate.startswith("23"):
            self._raise_integrity_error(e)
        elif sqlstate.startswith("42"):
            self._raise_parsing_error(e)
        elif sqlstate.startswith("08"):
            self._raise_connection_error(e)
        elif sqlstate.startswith("40"):
            self._raise_transaction_error(e)
        elif sqlstate.startswith("22"):
            self._raise_data_error(e)
        else:
            self._raise_generic_error(e)

    def _map_message_based_exception(self, e: Any) -> None:
        """Map exception using message-based detection.

        Args:
            e: Exception instance
        """
        error_msg = str(e).lower()

        if "unique" in error_msg or "duplicate" in error_msg:
            self._raise_unique_violation(e)
        elif "foreign key" in error_msg:
            self._raise_foreign_key_violation(e)
        elif "not null" in error_msg or "null value" in error_msg:
            self._raise_not_null_violation(e)
        elif "check constraint" in error_msg:
            self._raise_check_violation(e)
        elif "constraint" in error_msg:
            self._raise_integrity_error(e)
        elif "syntax" in error_msg:
            self._raise_parsing_error(e)
        elif "connection" in error_msg or "connect" in error_msg:
            self._raise_connection_error(e)
        else:
            self._raise_generic_error(e)

    def _raise_unique_violation(self, e: Any) -> None:
        msg = f"ADBC unique constraint violation: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any) -> None:
        msg = f"ADBC foreign key constraint violation: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any) -> None:
        msg = f"ADBC not-null constraint violation: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any) -> None:
        msg = f"ADBC check constraint violation: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any) -> None:
        msg = f"ADBC integrity constraint violation: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any) -> None:
        msg = f"ADBC SQL parsing error: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any) -> None:
        msg = f"ADBC connection error: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any) -> None:
        msg = f"ADBC transaction error: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any) -> None:
        msg = f"ADBC data error: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"ADBC database error: {e}"
        raise SQLSpecError(msg) from e


class AdbcDriver(SyncDriverAdapterBase):
    """ADBC driver for Arrow Database Connectivity.

    Provides database connectivity through ADBC with support for multiple
    database dialects, parameter style conversion, and transaction management.
    """

    __slots__ = ("_data_dictionary", "_detected_dialect", "dialect")

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
            statement_config = base_config.replace(
                enable_caching=cache_config.compiled_cache_enabled, enable_parsing=True, enable_validation=True
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self.dialect = statement_config.dialect
        self._data_dictionary: Optional[SyncDataDictionaryBase] = None

    @staticmethod
    def _ensure_pyarrow_installed() -> None:
        """Ensure PyArrow is installed.

        Raises:
            MissingDependencyError: If PyArrow is not installed
        """
        from sqlspec.typing import PYARROW_INSTALLED

        if not PYARROW_INSTALLED:
            raise MissingDependencyError(package="pyarrow", install_package="arrow")

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Detect database dialect from connection information.

        Args:
            connection: ADBC connection

        Returns:
            Detected dialect name (defaults to 'postgres')
        """
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    logger.debug("Dialect detected: %s (from %s/%s)", dialect, vendor_name, driver_name)
                    return dialect
        except Exception as e:
            logger.debug("Dialect detection failed: %s", e)

        logger.warning("Could not determine dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Execute rollback for PostgreSQL after transaction failure.

        Args:
            cursor: Database cursor
        """
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")
                logger.debug("PostgreSQL rollback executed after transaction failure")

    def _handle_postgres_empty_parameters(self, parameters: Any) -> Any:
        """Process empty parameters for PostgreSQL compatibility.

        Args:
            parameters: Parameter values

        Returns:
            Processed parameters
        """
        if self.dialect == "postgres" and isinstance(parameters, dict) and not parameters:
            return None
        return parameters

    def prepare_driver_parameters(
        self,
        parameters: Any,
        statement_config: "StatementConfig",
        is_many: bool = False,
        prepared_statement: Optional[Any] = None,
    ) -> Any:
        """Prepare parameters with cast-aware type coercion for ADBC.

        For PostgreSQL, applies cast-aware parameter processing using metadata from the compiled statement.
        This allows proper handling of JSONB casts and other type conversions.

        Args:
            parameters: Parameters in any format
            statement_config: Statement configuration
            is_many: Whether this is for execute_many operation
            prepared_statement: Prepared statement containing the original SQL statement

        Returns:
            Parameters with cast-aware type coercion applied
        """
        if prepared_statement and self.dialect in {"postgres", "postgresql"} and not is_many:
            parameter_casts = self._get_parameter_casts(prepared_statement)
            postgres_compatible = self._handle_postgres_empty_parameters(parameters)
            return self._prepare_parameters_with_casts(postgres_compatible, parameter_casts, statement_config)

        return super().prepare_driver_parameters(parameters, statement_config, is_many, prepared_statement)

    def _get_parameter_casts(self, statement: SQL) -> "dict[int, str]":
        """Get parameter cast metadata from compiled statement.

        Args:
            statement: SQL statement with compiled metadata

        Returns:
            Dict mapping parameter positions to cast types
        """

        processed_state = statement.get_processed_state()
        if processed_state is not Empty:
            return processed_state.parameter_casts or {}
        return {}

    def _prepare_parameters_with_casts(
        self, parameters: Any, parameter_casts: "dict[int, str]", statement_config: "StatementConfig"
    ) -> Any:
        """Prepare parameters with cast-aware type coercion.

        Uses type coercion map for non-dict types and dialect-aware dict handling.

        Args:
            parameters: Parameter values (list, tuple, or scalar)
            parameter_casts: Mapping of parameter positions to cast types
            statement_config: Statement configuration for type coercion

        Returns:
            Parameters with cast-aware type coercion applied
        """
        from sqlspec._serialization import encode_json

        if isinstance(parameters, (list, tuple)):
            result: list[Any] = []
            for idx, param in enumerate(parameters, start=1):  # pyright: ignore
                cast_type = parameter_casts.get(idx, "").upper()
                if cast_type in {"JSON", "JSONB", "TYPE.JSON", "TYPE.JSONB"}:
                    if isinstance(param, dict):
                        result.append(encode_json(param))
                    else:
                        result.append(param)
                elif isinstance(param, dict):
                    result.append(ADBCTypeConverter(self.dialect).convert_dict(param))  # type: ignore[arg-type]
                else:
                    if statement_config.parameter_config.type_coercion_map:
                        for type_check, converter in statement_config.parameter_config.type_coercion_map.items():
                            if type_check is not dict and isinstance(param, type_check):
                                param = converter(param)
                                break
                    result.append(param)
            return tuple(result) if isinstance(parameters, tuple) else result
        return parameters

    def with_cursor(self, connection: "AdbcConnection") -> "AdbcCursor":
        """Create context manager for cursor.

        Args:
            connection: Database connection

        Returns:
            Cursor context manager
        """
        return AdbcCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler context manager
        """
        return AdbcExceptionHandler()

    def _try_special_handling(self, cursor: "Cursor", statement: SQL) -> "Optional[SQLResult]":
        """Handle special operations.

        Args:
            cursor: Database cursor
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        _ = (cursor, statement)
        return None

    def _execute_many(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = self._get_parameter_casts(statement)

        try:
            if not prepared_parameters:
                cursor._rowcount = 0  # pyright: ignore[reportPrivateUsage]
                row_count = 0
            elif isinstance(prepared_parameters, (list, tuple)) and prepared_parameters:
                processed_params = []
                for param_set in prepared_parameters:
                    postgres_compatible = self._handle_postgres_empty_parameters(param_set)

                    if self.dialect in {"postgres", "postgresql"}:
                        # For postgres, always use cast-aware parameter preparation
                        formatted_params = self._prepare_parameters_with_casts(
                            postgres_compatible, parameter_casts, self.statement_config
                        )
                    else:
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
            raise

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: "Cursor", statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            Execution result with data or row count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        parameter_casts = self._get_parameter_casts(statement)

        try:
            postgres_compatible_params = self._handle_postgres_empty_parameters(prepared_parameters)

            if self.dialect in {"postgres", "postgresql"}:
                formatted_params = self._prepare_parameters_with_casts(
                    postgres_compatible_params, parameter_casts, self.statement_config
                )
                cursor.execute(sql, parameters=formatted_params)
            else:
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
        """Execute SQL script containing multiple statements.

        Args:
            cursor: Database cursor
            statement: SQL script to execute

        Returns:
            Execution result with statement counts
        """
        if statement.is_script:
            sql = statement.raw_sql
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
            logger.exception("Script execution failed")
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
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("ROLLBACK")
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit database transaction."""
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("COMMIT")
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AdbcDataDictionary()
        return self._data_dictionary
