# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig

if TYPE_CHECKING:
    from sqlspec.adapters.duckdb._types import DuckDBConnection


__all__ = ("DuckDBCursor", "DuckDBDriver")


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
        # Set default DuckDB-specific configuration
        if statement_config is None:
            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK,
                supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC},
                type_coercion_map={},
                has_native_list_expansion=True,
                needs_static_script_compilation=True,  # DuckDB requires static compilation for scripts
            )
            statement_config = StatementConfig(dialect="duckdb", parameter_config=parameter_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "DuckDBConnection") -> "DuckDBCursor":
        return DuckDBCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "Any") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for DuckDB-specific special operations.

        DuckDB doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.
        """
        return None

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """DuckDB executemany with accurate row counting."""
        if prepared_params:
            cursor.executemany(sql, prepared_params)
            # DuckDB's cursor.rowcount is unreliable for executemany
            # Return explicit count for INSERT operations
            if sql.strip().upper().startswith("INSERT"):
                return len(prepared_params)  # Explicit accurate count
        else:
            # Empty parameter set - no operation performed
            return 0

        # For non-INSERT operations, return cursor
        return cursor

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """DuckDB single execution."""
        cursor.execute(sql, prepared_params or ())
        return cursor

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
            return max(cursor.rowcount, 0) if hasattr(cursor, "rowcount") else 0

    def _build_result(self, cursor: Any, statement: "Any", execution_result: "tuple[Any, Optional[int], Any]") -> "Any":
        """Build result with DuckDB-specific handling for executemany row counting."""
        cursor_result, _, _ = execution_result

        # For executemany operations, use explicit row count from _execute_many
        if statement.is_many and isinstance(cursor_result, int):
            return self._build_execute_result_from_data(statement=statement, row_count=cursor_result)

        # Use base class implementation for all other cases
        return super()._build_result(cursor, statement, execution_result)

    def begin(self) -> None:
        """Begin a database transaction."""
        self.connection.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()
