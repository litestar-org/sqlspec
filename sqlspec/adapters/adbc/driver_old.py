# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import decimal
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

import sqlglot

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Generator

    from adbc_driver_manager.dbapi import Cursor

    from sqlspec.adapters.adbc._types import AdbcConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.driver._common import ExecutionResult


__all__ = ("AdbcCursor", "AdbcDriver", "get_adbc_statement_config")


def get_adbc_statement_config(detected_dialect: str) -> StatementConfig:
    """Create ADBC statement configuration for the specified dialect."""
    default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
        detected_dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
    )

    type_map = get_type_coercion_map(detected_dialect)

    parameter_config = ParameterStyleConfig(
        default_parameter_style=default_style,
        supported_parameter_styles=set(supported_styles),
        type_coercion_map=type_map,
        has_native_list_expansion=True,
        needs_static_script_compilation=True,
        remove_null_parameters=True,  # ADBC cannot handle NULL parameters
    )

    post_process_steps = None

    return StatementConfig(
        dialect=detected_dialect,
        pre_process_steps=None,
        post_process_steps=post_process_steps,
        enable_parsing=True,
        enable_transformations=True,
        enable_validation=True,
        enable_caching=True,
        parameter_config=parameter_config,
    )


def get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Get type coercion map for Arrow/ADBC type handling."""
    type_map = {
        # NOTE: NoneType is excluded from type map to force NULL handling at SQL level
        # ADBC cannot handle NULL parameters in parameter arrays - they must be
        # replaced with literal NULL in SQL and removed from parameter list
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

    if dialect == "postgres":
        type_map[dict] = lambda x: to_json(x) if x is not None else None

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
    """ADBC synchronous driver adapter for Arrow Database Connectivity."""

    def __init__(
        self,
        connection: "AdbcConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        detected_dialect = self._get_dialect(connection)

        if statement_config is None:
            statement_config = get_adbc_statement_config(detected_dialect)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

        self.dialect = statement_config.dialect

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Detect database dialect from ADBC connection information."""
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            for dialect, patterns in DIALECT_PATTERNS.items():
                if any(pattern in vendor_name or pattern in driver_name for pattern in patterns):
                    return dialect
        except Exception:
            logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    def _handle_postgres_rollback(self, cursor: "Cursor") -> None:
        """Execute rollback for PostgreSQL after transaction failure."""
        if self.dialect == "postgres":
            with contextlib.suppress(Exception):
                cursor.execute("ROLLBACK")

    def _handle_postgres_empty_parameters(self, parameters: "Any") -> "Any":
        """Process empty parameters for PostgreSQL compatibility."""
        if self.dialect == "postgres" and isinstance(parameters, dict) and not parameters:
            return None
        return parameters

    def with_cursor(self, connection: "AdbcConnection") -> "AdbcCursor":
        return AdbcCursor(connection)

    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle ADBC-specific exceptions and wrap them appropriately."""
        return cast("Generator[None, None, None]", self._handle_database_exceptions_impl())

    @contextmanager
    def _handle_database_exceptions_impl(self) -> "Generator[None, None, None]":
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except Exception as e:
            # Handle ADBC-specific errors
            if "adbc" in str(e).lower() or "arrow" in str(e).lower():
                msg = f"ADBC database error: {e}"
                raise SQLSpecError(msg) from e
            elif "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            else:
                msg = f"Unexpected database error: {e}"
                raise SQLSpecError(msg) from e

    def _try_special_handling(self, cursor: "Cursor", statement: "SQL") -> "Optional[SQLResult]":
        """Handle ADBC-specific operations including script execution."""
        if statement.is_script:
            try:
                sql, parameters = statement.compile()
                statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
                statement_count = len(statements)

                for stmt in statements:
                    if stmt.strip():
                        prepared_parameters = self.prepare_driver_parameters(
                            self._handle_postgres_empty_parameters(parameters), self.statement_config, is_many=False
                        )
                        cursor.execute(stmt, parameters=prepared_parameters)

                execution_result = self.create_execution_result(
                    cursor,
                    statement_count=statement_count,
                    successful_statements=statement_count,
                    is_script_result=True,
                )
                return self.build_statement_result(statement, execution_result)

            except Exception as e:
                self._handle_postgres_rollback(cursor)
                raise e from e

        return None

    def _execute_many(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """ADBC executemany implementation."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        try:
            if not prepared_parameters:
                cursor._rowcount = 0
                row_count = 0
            else:
                if isinstance(prepared_parameters, list) and prepared_parameters:
                    processed_params = []
                    for param_set in prepared_parameters:
                        postgres_compatible = self._handle_postgres_empty_parameters(param_set)
                        formatted_params = self.prepare_driver_parameters(
                            postgres_compatible, self.statement_config, is_many=False
                        )
                        processed_params.append(formatted_params)

                    cursor.executemany(sql, processed_params)
                else:
                    cursor.executemany(sql, prepared_parameters)

                row_count = cursor.rowcount if cursor.rowcount is not None else -1

        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: "Cursor", statement: "SQL") -> "ExecutionResult":
        """ADBC single execution."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        try:
            postgres_compatible_params = self._handle_postgres_empty_parameters(prepared_parameters)
            parameters = self.prepare_driver_parameters(
                postgres_compatible_params, self.statement_config, is_many=False
            )
            final_parameters = self._handle_single_param_list(sql, parameters)
            cursor.execute(sql, parameters=final_parameters)

        except Exception as e:
            self._handle_postgres_rollback(cursor)
            raise e from e

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

    def _handle_single_param_list(self, sql: str, parameters: "list[Any]") -> "list[Any]":
        """Handle single parameter list edge cases for ADBC compatibility."""
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            param_placeholders = set()
            for node in parsed.walk():
                if isinstance(node, sqlglot.exp.Placeholder):
                    param_placeholders.add(node.this)
            param_count = len(param_placeholders)
        except Exception:
            param_count = sql.count("$1") + sql.count("$2") + sql.count("?") + sql.count("%s")

        if (
            param_count == 1
            and len(parameters) == 1
            and isinstance(parameters[0], (list, tuple))
            and len(parameters[0]) == 1
            and not isinstance(parameters[0][0], (list, tuple))
        ):
            return list(parameters[0])

        return parameters
