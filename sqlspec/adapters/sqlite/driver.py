# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import sqlite3
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from sqlspec.driver import SyncDriverAdapterBase

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

__all__ = ("SqliteConnection", "SqliteDriver")

logger = get_logger("adapters.sqlite")

if TYPE_CHECKING:
    SqliteConnection: TypeAlias = sqlite3.Connection
else:
    # Direct assignment for mypyc runtime
    SqliteConnection = sqlite3.Connection


class _SqliteCursorManager:
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
    default_parameter_style: "ClassVar[str]" = "qmark"
    parameter_config: DriverParameterConfig

    def __init__(self, connection: "SqliteConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=[ParameterStyle.QMARK],
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
        )

    def with_cursor(self, connection: "SqliteConnection") -> "_SqliteCursorManager":
        return _SqliteCursorManager(connection)

    def _perform_execute(self, cursor: "sqlite3.Cursor", statement: "SQL") -> None:
        if statement.is_script:
            # Scripts use STATIC compilation to transpile parameters automatically
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            cursor.executescript(sql)
        else:
            # Regular execution - let intelligent conversion handle parameter style
            # Since SQLite only supports QMARK, conversion will happen automatically when needed
            sql, params = statement.compile()

            if statement.is_many:
                # For execute_many, params is already a list of parameter sets
                prepared_params = self._prepare_driver_parameters_many(params) if params else []
                cursor.executemany(sql, prepared_params)
            else:
                # Prepare parameters for driver consumption
                prepared_params = self._prepare_driver_parameters(params)
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
        return cursor.rowcount if cursor.rowcount is not None else 0
