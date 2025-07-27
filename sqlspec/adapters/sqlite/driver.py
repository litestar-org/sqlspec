# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import contextlib
import datetime
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from sqlspec.driver import SyncDriverAdapterBase

if TYPE_CHECKING:
    from collections.abc import Iterator

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


class SqliteDriver(SyncDriverAdapterBase):
    """Reference implementation for a synchronous SQLite driver."""

    dialect: "DialectType" = "sqlite"
    default_parameter_style: "ClassVar[str]" = "qmark"
    parameter_config: ClassVar[DriverParameterConfig] = DriverParameterConfig(
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

    def __init__(self, connection: "SqliteConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    @contextmanager
    def with_cursor(self, connection: "SqliteConnection") -> "Iterator[sqlite3.Cursor]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _perform_execute(self, cursor: "sqlite3.Cursor", statement: "SQL") -> None:
        # Compile with driver's parameter style
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_script:
            # Scripts don't support parameters
            cursor.executescript(sql)
        elif statement.is_many:
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
        # Convert Row objects to dicts
        data = [dict(zip(column_names, row)) for row in fetched_data]
        return data, column_names, len(data)

    def _extract_execute_rowcount(self, cursor: "sqlite3.Cursor") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0
