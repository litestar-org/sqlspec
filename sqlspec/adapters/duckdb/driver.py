# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

import duckdb
from sqlglot import exp

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.adapters.duckdb._types import DuckDBConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

duckdb_statement_config = StatementConfig(
    dialect="duckdb",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        default_execution_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
        supported_execution_parameter_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_DOLLAR,
        },
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=True,
    ),
)

__all__ = ("DuckDBCursor", "DuckDBDriver", "duckdb_statement_config")


class DuckDBCursor:
    """Context manager for DuckDB cursor management."""

    def __init__(self, connection: "DuckDBConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Any] = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class DuckDBDriver(SyncDriverAdapterBase):
    """DuckDB Sync Driver Adapter. Clean hook-based implementation with no state management."""

    dialect = "duckdb"

    def __init__(
        self,
        connection: "DuckDBConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or duckdb_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: "DuckDBConnection") -> "DuckDBCursor":
        return DuckDBCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "Any") -> "Optional[SQLResult]":
        """Hook for DuckDB-specific special operations.

        DuckDB doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        DuckDB supports parameters in individual statements.
        """
        sql = statement.sql
        prepared_parameters = statement.parameters
        statement_config = statement.statement_config
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)

        last_result = None
        for stmt in statements:
            last_result = cursor.execute(stmt, prepared_parameters or ())

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
        )

    def _is_modifying_operation(self, statement: "SQL") -> bool:
        """Check if the SQL statement is a modifying operation (INSERT/UPDATE/DELETE)."""

        expression = statement.expression
        if expression and isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return True

        sql_upper = statement._raw_sql.strip().upper()
        return sql_upper.startswith(("INSERT", "UPDATE", "DELETE"))

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """DuckDB executemany with accurate row counting."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        if prepared_parameters:
            cursor.executemany(sql, prepared_parameters)
            if self._is_modifying_operation(statement):
                row_count = len(prepared_parameters)
            else:
                try:
                    result = cursor.fetchone()
                    row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
                except Exception:
                    row_count = max(cursor.rowcount, 0) or 0
        else:
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """DuckDB single execution."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            if fetched_data and isinstance(fetched_data[0], tuple):
                dict_data = [dict(zip(column_names, row)) for row in fetched_data]
            else:
                dict_data = fetched_data

            return self.create_execution_result(
                cursor,
                selected_data=dict_data,
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        try:
            result = cursor.fetchone()
            row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
        except Exception:
            row_count = max(cursor.rowcount, 0) or 0

        return self.create_execution_result(cursor, rowcount_override=row_count)

    def begin(self) -> None:
        """Begin a database transaction."""
        self.connection.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()

    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle DuckDB-specific exceptions and wrap them appropriately."""
        return cast("Generator[None, None, None]", self._handle_database_exceptions_impl())

    @contextmanager
    def _handle_database_exceptions_impl(self) -> "Generator[None, None, None]":
        """Implementation of database exception handling."""
        try:
            yield
        except duckdb.Error as e:
            msg = f"DuckDB database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
