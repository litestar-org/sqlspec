import contextlib
import csv
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from typing_extensions import TypeAlias

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT, is_dict_with_field
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("SqliteConnection", "SqliteDriver")

logger = get_logger("adapters.sqlite")

SqliteConnection: TypeAlias = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterProtocol[SqliteConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """SQLite Sync Driver Adapter with Arrow/Parquet export support.

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
    """

    __slots__ = ()

    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK

    def __init__(
        self,
        connection: "SqliteConnection",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = dict[str, Any],
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    # SQLite-specific type coercion overrides
    def _coerce_boolean(self, value: Any) -> Any:
        """SQLite stores booleans as integers (0/1)."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """SQLite stores decimals as strings to preserve precision."""
        if isinstance(value, str):
            return value  # Already a string
        from decimal import Decimal

        if isinstance(value, Decimal):
            return str(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """SQLite stores JSON as strings (requires JSON1 extension)."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """SQLite doesn't have native arrays - store as JSON strings."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

    @staticmethod
    @contextmanager
    def _get_cursor(connection: SqliteConnection) -> Iterator[sqlite3.Cursor]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_statement(
        self, statement: SQL, connection: Optional[SqliteConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        # Determine if we need to convert parameter style
        detected_styles = {p.style for p in statement.parameter_info}
        target_style = self.default_parameter_style

        # Check if any detected style is not supported
        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            # Convert to default style if we have unsupported styles
            target_style = self.default_parameter_style
        elif len(detected_styles) > 1:
            # Mixed styles detected - use default style for consistency
            target_style = self.default_parameter_style
        elif detected_styles:
            # Single style detected - use it if supported
            single_style = next(iter(detected_styles))
            if single_style in self.supported_parameter_styles:
                target_style = single_style
            else:
                target_style = self.default_parameter_style

        if statement.is_many:
            sql, params = statement.compile(placeholder_style=target_style)
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)

        # Process parameters through type coercion
        params = self._process_parameters(params)

        # SQLite expects tuples for positional parameters
        if isinstance(params, list):
            params = tuple(params)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[SqliteConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        """Execute a single statement with parameters."""
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            # SQLite expects tuple or dict parameters
            if parameters is not None and not isinstance(parameters, (tuple, list, dict)):
                # Convert scalar to tuple
                parameters = (parameters,)
            cursor.execute(sql, parameters or ())
            if self.returns_rows(statement.expression):
                fetched_data: list[sqlite3.Row] = cursor.fetchall()
                return {
                    "data": fetched_data,
                    "column_names": [col[0] for col in cursor.description or []],
                    "rows_affected": len(fetched_data),
                }
            return {"rows_affected": cursor.rowcount, "status_message": "OK"}

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[SqliteConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        """Execute a statement many times with a list of parameter tuples."""
        conn = self._connection(connection)
        if param_list:
            param_list = self._process_parameters(param_list)

        # Convert parameter list to proper format for executemany
        formatted_params: list[tuple[Any, ...]] = []
        if param_list and isinstance(param_list, list):
            for param_set in cast("list[Union[list, tuple]]", param_list):
                if isinstance(param_set, (list, tuple)):
                    formatted_params.append(tuple(param_set))
                elif param_set is None:
                    formatted_params.append(())
                else:
                    formatted_params.append((param_set,))

        with self._get_cursor(conn) as cursor:
            cursor.executemany(sql, formatted_params)
            return {"rows_affected": cursor.rowcount, "status_message": "OK"}

    def _execute_script(
        self, script: str, connection: Optional[SqliteConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        """Execute a script on the SQLite connection."""
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.executescript(script)
        # executescript doesn't auto-commit in some cases
        conn.commit()
        result: ScriptResultDict = {"statements_executed": -1, "status_message": "SCRIPT EXECUTED"}
        return result

    def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Database-specific bulk load implementation."""
        if format != "csv":
            msg = f"SQLite driver only supports CSV for bulk loading, not {format}."
            raise NotImplementedError(msg)

        conn = self._connection(None)
        with self._get_cursor(conn) as cursor:
            if mode == "replace":
                cursor.execute(f"DELETE FROM {table_name}")

            with Path(file_path).open(encoding="utf-8") as f:
                reader = csv.reader(f, **options)
                header = next(reader)  # Skip header
                placeholders = ", ".join("?" for _ in header)
                sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

                # executemany is efficient for bulk inserts
                data_iter = list(reader)  # Read all data into memory
                cursor.executemany(sql, data_iter)
                return cursor.rowcount

    def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        rows_as_dicts = [dict(row) for row in result["data"]]
        if schema_type:
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(self.to_schema(data=rows_as_dicts, schema_type=schema_type)),
                column_names=result["column_names"],
                rows_affected=result["rows_affected"],
                operation_type="SELECT",
            )

        return SQLResult[RowT](
            statement=statement,
            data=rows_as_dicts,
            column_names=result["column_names"],
            rows_affected=result["rows_affected"],
            operation_type="SELECT",
        )

    def _wrap_execute_result(
        self, statement: SQL, result: Union[DMLResultDict, ScriptResultDict], **kwargs: Any
    ) -> SQLResult[RowT]:
        if is_dict_with_field(result, "statements_executed"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type="SCRIPT",
                metadata={
                    "status_message": result.get("status_message", ""),
                    "statements_executed": result.get("statements_executed", -1),
                },
            )
        return SQLResult[RowT](
            statement=statement,
            data=[],
            rows_affected=cast("int", result.get("rows_affected", -1)),
            operation_type=statement.expression.key.upper() if statement.expression else "UNKNOWN",
            metadata={"status_message": result.get("status_message", "")},
        )
