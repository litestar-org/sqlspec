# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle

if TYPE_CHECKING:
    from typing import Optional

    from sqlspec.adapters.duckdb._types import DuckDBConnection
    from sqlspec.statement.sql import SQL, StatementConfig


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
    """DuckDB Sync Driver Adapter with modern architecture."""

    def __init__(
        self,
        connection: "DuckDBConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        from sqlspec.statement.sql import StatementConfig

        if statement_config is None:
            statement_config = StatementConfig(
                dialect="duckdb",
                supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC},
                default_parameter_style=ParameterStyle.QMARK,
                type_coercion_map={},
                has_native_list_expansion=True,
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._execution_state: dict[str, Optional[int]] = {"executemany_count": None}

    def with_cursor(self, connection: "DuckDBConnection") -> "DuckDBCursor":
        return DuckDBCursor(connection)

    def _perform_execute(self, cursor: "DuckDBConnection", statement: "SQL") -> None:
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            # DuckDB doesn't have a dedicated executescript method, so split and execute
            statements = self.split_script_statements(sql, self.statement_config, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    cursor.execute(stmt)
        else:
            # Enable intelligent parameter conversion - DuckDB supports both QMARK and NUMERIC
            sql, params = self._get_compiled_sql(statement, self.statement_config)

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=True)

                # DuckDB requires non-empty parameter sets for executemany
                if prepared_params:
                    cursor.executemany(sql, prepared_params)
                    # Store the parameter count for accurate row count reporting
                    # DuckDB's rowcount after executemany only reports the last batch
                    if sql.strip().upper().startswith("INSERT"):
                        self._execution_state["executemany_count"] = len(prepared_params)
                    else:
                        self._execution_state["executemany_count"] = None
                else:
                    # Empty parameter set - no operation performed
                    # Set executemany_count to 0 to indicate no rows were affected
                    self._execution_state["executemany_count"] = 0
            else:
                prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)
                cursor.execute(sql, prepared_params)
                self._execution_state["executemany_count"] = None

    def _extract_select_data(self, cursor: "DuckDBConnection") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data

        return dict_data, column_names, len(dict_data)

    def _extract_execute_rowcount(self, cursor: "DuckDBConnection") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        executemany_count = self._execution_state.get("executemany_count")
        if executemany_count is not None:
            self._execution_state["executemany_count"] = None
            return executemany_count

        try:
            result = cursor.fetchone()
            return int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
        except Exception:
            return max(cursor.rowcount, 0) if hasattr(cursor, "rowcount") else 0

    def begin(self) -> None:
        """Begin a database transaction."""
        self.connection.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()
