# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb._types import DuckDBConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

# Shared DuckDB statement configuration
duckdb_statement_config = StatementConfig(
    dialect="duckdb",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
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
        if statement_config is None:
            # Use DuckDB defaults
            statement_config = duckdb_statement_config
        else:
            # Ensure provided config uses DuckDB-compatible parameter configuration
            # Preserve other settings but replace parameter_config with DuckDB requirements
            statement_config = statement_config.replace(
                dialect=statement_config.dialect or "duckdb", parameter_config=duckdb_statement_config.parameter_config
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "DuckDBConnection") -> "DuckDBCursor":
        return DuckDBCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "Any") -> "Optional[SQLResult]":
        """Hook for DuckDB-specific special operations.

        DuckDB doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    def _execute_script(
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig", statement: "SQL"
    ) -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        DuckDB supports parameters in individual statements.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)

        last_result = None
        for stmt in statements:
            last_result = cursor.execute(stmt, prepared_params or ())

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
        )

    def _is_modifying_operation(self, statement: "SQL") -> bool:
        """Check if the SQL statement is a modifying operation (INSERT/UPDATE/DELETE)."""

        # Try to get the expression from the statement
        expression = statement.expression
        if expression and isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return True

        # Fallback: check the raw SQL text for INSERT/UPDATE/DELETE keywords
        sql_upper = statement._raw_sql.strip().upper()
        return sql_upper.startswith(("INSERT", "UPDATE", "DELETE"))

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """DuckDB executemany with accurate row counting."""
        if prepared_params:
            cursor.executemany(sql, prepared_params)
            # DuckDB's cursor.rowcount is unreliable for executemany
            # Use explicit count for INSERT/UPDATE/DELETE operations
            if self._is_modifying_operation(statement):
                row_count = len(prepared_params)  # Explicit accurate count
            else:
                # For non-modifying operations, try to get row count from cursor
                try:
                    result = cursor.fetchone()
                    row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
                except Exception:
                    row_count = max(cursor.rowcount, 0) or 0
        else:
            # Empty parameter set - no operation performed
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """DuckDB single execution."""
        cursor.execute(sql, prepared_params or ())

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
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

        # For non-SELECT operations, get row count
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
