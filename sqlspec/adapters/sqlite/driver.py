# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    import sqlite3

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.sqlite._types import SqliteConnection

__all__ = ("SqliteCursor", "SqliteDriver")


class SqliteCursor:
    """Context manager for SQLite cursor management."""

    def __init__(self, connection: "SqliteConnection") -> None:
        self.connection = connection
        self.cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self) -> "sqlite3.Cursor":
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class SqliteDriver(SyncDriverAdapterBase):
    """Reference implementation for a synchronous SQLite driver."""

    dialect: "DialectType" = "sqlite"

    def __init__(self, connection: "SqliteConnection", statement_config: "Optional[StatementConfig]" = None, driver_features: "Optional[dict[str, Any]]" = None) -> None:
        # Set default sqlite-specific configuration
        if statement_config is None:
            statement_config = StatementConfig(
                dialect="sqlite",
                supported_parameter_styles={ParameterStyle.QMARK},
                default_parameter_style=ParameterStyle.QMARK,
                type_coercion_map={
                    bool: int,
                    datetime.datetime: lambda v: v.isoformat(),
                    Decimal: str,
                    dict: to_json,
                    list: to_json,
                    tuple: lambda v: to_json(list(v)),
                },
                has_native_list_expansion=False,
                needs_static_script_compilation=True,
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "SqliteConnection") -> "SqliteCursor":
        return SqliteCursor(connection)

    def _execute_script(
        self, cursor: "sqlite3.Cursor", sql: str, prepared_params: Optional[Any], statement_config: "StatementConfig"
    ) -> None:
        """Execute SQL script using SQLite's native executescript (parameters embedded as static values)."""
        # SQLite's executescript doesn't support parameters, so we use static compilation
        # The base class _perform_execute will have already prepared static SQL for scripts
        # prepared_params will be None for static compilation
        cursor.executescript(sql)

    def _execute_many(self, cursor: "sqlite3.Cursor", sql: str, prepared_params: Any) -> None:
        """Execute SQL with multiple parameter sets using SQLite executemany."""
        cursor.executemany(sql, prepared_params)

    def _execute_statement(self, cursor: "sqlite3.Cursor", sql: str, prepared_params: Any) -> None:
        """Execute single SQL statement using SQLite execute."""
        cursor.execute(sql, prepared_params or ())

    def begin(self) -> None:
        """Begin a database transaction."""
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()

    def _extract_select_data(self, cursor: "sqlite3.Cursor") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col[0] for col in cursor.description or []]
        data = [dict(zip(column_names, row)) for row in fetched_data]
        return data, column_names, len(data)

    def _extract_execute_rowcount(self, cursor: "sqlite3.Cursor") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount or 0
