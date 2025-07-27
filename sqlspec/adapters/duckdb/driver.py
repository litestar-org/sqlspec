# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from contextlib import contextmanager
from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any, Optional

    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.statement.sql import SQL, SQLConfig


__all__ = ("DuckDBConnection", "DuckDBDriver")

if TYPE_CHECKING:
    DuckDBConnection: TypeAlias = DuckDBPyConnection
else:
    DuckDBConnection = DuckDBPyConnection

logger = get_logger("adapters.duckdb")


class DuckDBDriver(SyncDriverAdapterBase):
    """DuckDB Sync Driver Adapter with modern architecture."""

    dialect: "DialectType" = "duckdb"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[
            ParameterStyle.QMARK,     # ?
            ParameterStyle.NUMERIC,   # $1, $2
        ],
        default_parameter_style=ParameterStyle.QMARK,
        type_coercion_map={},
        has_native_list_expansion=True,  # DuckDB handles lists natively
    )

    def __init__(self, connection: "DuckDBConnection", config: "Optional[SQLConfig]" = None) -> None:  # noqa: FA100
        super().__init__(connection=connection, config=config)

    @contextmanager
    def with_cursor(self, connection: "DuckDBConnection") -> "Generator[DuckDBConnection, None, None]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _perform_execute(self, cursor: "DuckDBConnection", statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            cursor.execute(sql, prepared_params or [])

    def _build_result(self, cursor: "DuckDBConnection", statement: "SQL") -> "SQLResult":
        if self.returns_rows(statement.expression):
            return self._build_select_result(cursor, statement)
        return self._build_modify_result(cursor, statement)

    def _build_select_result(self, result: "Any", statement: "SQL") -> "SQLResult":
        """Build SQLResult for SELECT operations."""
        fetched_data = result.fetchall()
        column_names = [col[0] for col in result.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data

        return SQLResult(
            statement=statement,
            data=dict_data,
            column_names=column_names,
            rows_affected=len(dict_data),
            operation_type="SELECT",
        )

    def _build_modify_result(self, cursor: "Any", statement: "SQL") -> "SQLResult":
        """Build SQLResult for non-SELECT operations (INSERT, UPDATE, DELETE)."""
        try:
            result = cursor.fetchone()
            rows_affected = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
        except Exception:
            rows_affected = max(cursor.rowcount, 0)

        operation_type = self._determine_operation_type(statement)

        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=rows_affected,
            operation_type=operation_type,
            metadata={"status_message": "OK"},
        )

    def begin(self, connection: "Optional[Any]" = None) -> None:
        """Begin a database transaction."""
        conn = connection or self.connection
        conn.execute("BEGIN TRANSACTION")

    def rollback(self, connection: "Optional[Any]" = None) -> None:
        """Rollback the current transaction."""
        conn = connection or self.connection
        conn.rollback()

    def commit(self, connection: "Optional[Any]" = None) -> None:
        """Commit the current transaction."""
        conn = connection or self.connection
        conn.commit()
