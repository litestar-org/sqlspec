# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb._types import DuckDBConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult

# Shared DuckDB statement configuration
duckdb_statement_config = StatementConfig(
    dialect="duckdb",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=True,  # DuckDB requires static compilation for scripts
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
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig"
    ) -> "ExecutionResult":
        """Execute a SQL script (multiple statements).

        DuckDB can handle multiple statements in a single execute call.
        """
        try:
            if prepared_params:
                cursor.execute(sql, prepared_params)
            else:
                cursor.execute(sql)
        except Exception as e:
            raise e from e

        # Count statements for the result
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)

        # Get row count if available
        try:
            row_count = self._get_row_count(cursor)
        except Exception:
            row_count = None

        return self.create_execution_result(
            cursor,
            statement_count=statement_count,
            successful_statements=statement_count,  # Assume all successful if no exception
            rowcount_override=row_count,
            is_script_result=True,
        )

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> "ExecutionResult":
        """DuckDB executemany with accurate row counting."""
        if prepared_params:
            cursor.executemany(sql, prepared_params)
            # DuckDB's cursor.rowcount is unreliable for executemany
            # Use explicit count for INSERT/UPDATE/DELETE operations
            sql_upper = sql.strip().upper()
            if sql_upper.startswith(("INSERT", "UPDATE", "DELETE")):
                row_count = len(prepared_params)  # Explicit accurate count
            else:
                # For non-modifying operations, try to get row count from cursor
                try:
                    row_count = self._get_row_count(cursor)
                except Exception:
                    row_count = None
        else:
            # Empty parameter set - no operation performed
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> "ExecutionResult":
        """DuckDB single execution."""
        cursor.execute(sql, prepared_params or ())

        # Get row count if available
        try:
            row_count = self._get_row_count(cursor)
        except Exception:
            row_count = None

        return self.create_execution_result(cursor, rowcount_override=row_count)

    def _get_selected_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data

        return dict_data, column_names, len(dict_data)

    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        try:
            result = cursor.fetchone()
            return int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
        except Exception:
            return max(cursor.rowcount, 0) or 0

    def begin(self) -> None:
        """Begin a database transaction."""
        self.connection.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()
