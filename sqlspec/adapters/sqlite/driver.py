# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.adapters.sqlite._types import SqliteConnection
    from sqlspec.driver import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

__all__ = ("SqliteCursor", "SqliteDriver", "sqlite_statement_config")

sqlite_statement_config = StatementConfig(
    dialect="sqlite",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
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
    ),
)


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

    dialect = "sqlite"

    def __init__(
        self,
        connection: "SqliteConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or sqlite_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: "SqliteConnection") -> "SqliteCursor":
        return SqliteCursor(connection)

    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle SQLite-specific exceptions and wrap them appropriately."""
        return cast("Generator[None, None, None]", self._handle_database_exceptions_impl())

    @contextmanager
    def _handle_database_exceptions_impl(self) -> "Generator[None, None, None]":
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except sqlite3.Error as e:
            msg = f"SQLite database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e

    def _try_special_handling(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for SQLite-specific special operations.

        SQLite doesn't have special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution
        """
        return None

    def _execute_script(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script using SQLite's native executescript (parameters embedded as static values)."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or ())

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
        )

    def _execute_many(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using SQLite executemany."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        cursor.executemany(sql, prepared_parameters)
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount or 0, is_many_result=True)

    def _execute_statement(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement using SQLite execute."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]
            data = [dict(zip(column_names, row)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )
        return self.create_execution_result(cursor, rowcount_override=cursor.rowcount or 0)

    def begin(self) -> None:
        """Begin a database transaction."""
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.connection.commit()
