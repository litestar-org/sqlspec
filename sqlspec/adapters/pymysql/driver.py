"""PyMySQL MySQL driver implementation."""

import os
import tempfile
from collections.abc import Sized
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.adapters.pymysql._typing import (
    PYMYSQL_INSERT_VALUES_PATTERN,
    PyMysqlCursor,
    PyMysqlFieldType,
    PyMysqlMySQLError,
    PyMysqlServerStatus,
    PyMysqlSessionContext,
)
from sqlspec.adapters.pymysql.core import (
    PymysqlStreamSource,
    build_insert_statement,
    build_load_data_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    encode_records_for_local_infile,
    escape_literal_percent,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    normalize_lastrowid,
    resolve_many_rowcount,
    resolve_row_plan,
    resolve_rowcount,
)
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import supports_json_type

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.pymysql._typing import PyMysqlConnection
    from sqlspec.core import SQL, SQLResult, StatementConfig
    from sqlspec.driver import CachedQuery, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("PyMysqlCursor", "PyMysqlDriver", "PyMysqlExceptionHandler", "PyMysqlSessionContext")

logger = get_logger("sqlspec.adapters.pymysql")

json_type_value = PyMysqlFieldType.JSON if supports_json_type(PyMysqlFieldType) else None
PYMYSQL_JSON_TYPE_CODES: Final[set[int]] = {json_type_value} if json_type_value is not None else set()

_MYSQL_TYPE_CODE_TOKENS: Final[dict[int, str]] = {
    0: "decimal",
    1: "int32",
    2: "int32",
    3: "int64",
    4: "float32",
    5: "float64",
    7: "timestamp",
    8: "int64",
    10: "date",
    11: "time",
    12: "timestamp",
    246: "decimal",
    252: "binary",
    253: "string",
    254: "string",
}


class PyMysqlExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling PyMySQL exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, PyMysqlMySQLError):
            result = create_mapped_exception(exc_val, logger=logger)
            if result is True:
                return True
            self.pending_exception = cast("Exception", result)
            return True
        return False


class PyMysqlDriver(SyncDriverAdapterBase):
    """MySQL/MariaDB database driver using PyMySQL."""

    __slots__ = ("_data_dictionary", "_json_deserializer", "_json_serializer")
    dialect = "mysql"

    def __init__(
        self,
        connection: "PyMysqlConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PyMysqlDataDictionary | None = None
        features = driver_features or {}
        self._json_deserializer: Callable[[Any], Any] = cast(
            "Callable[[Any], Any]", features.get("json_deserializer", from_json)
        )
        self._json_serializer: Callable[[Any], str] = cast(
            "Callable[[Any], str]", features.get("json_serializer", to_json)
        )

    def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        if (
            "%" in cached.compiled_sql
            and escape_literal_percent(
                cached.compiled_sql, params or (None,), self.statement_config.parameter_validator
            )
            != cached.compiled_sql
        ):
            statement = self._cached_statement(
                sql, params, cached, params, params_are_simple=True, compiled_sql=cached.compiled_sql
            )
            return self._execute_cached_statement(statement)
        return super()._execute_cache_hit(sql, params, cached)

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            description = cursor.description or None
            row_plan = resolve_row_plan(description, PYMYSQL_JSON_TYPE_CODES)
            rows, column_names, row_format = collect_rows(
                fetched_data, row_plan, self._json_deserializer, logger=logger
            )
            column_types = _resolve_column_types(description)

            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=column_names,
                column_types=column_types,
                data_row_count=len(rows),
                is_select_result=True,
                row_format=row_format,
            )

        affected_rows = resolve_rowcount(cursor)
        last_id = normalize_lastrowid(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_id)

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        match = PYMYSQL_INSERT_VALUES_PATTERN.match(sql)
        if match:
            sql = (
                escape_literal_percent(match.group(1), prepared_parameters, self.statement_config.parameter_validator)
                + sql[match.end(1) :]
            )
        else:
            sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        parameter_count = len(prepared_parameters) if isinstance(prepared_parameters, Sized) else None
        cursor.executemany(sql, prepared_parameters)

        affected_rows = resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def begin(self) -> None:
        try:
            self.connection.begin()
        except PyMysqlMySQLError as exc:
            msg = f"Failed to begin MySQL transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def commit(self) -> None:
        try:
            self.connection.commit()
        except PyMysqlMySQLError as exc:
            msg = f"Failed to commit MySQL transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except PyMysqlMySQLError as exc:
            msg = f"Failed to rollback MySQL transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: "PyMysqlConnection") -> "PyMysqlCursor":
        return PyMysqlCursor(connection)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native PyMySQL row stream backed by an unbuffered ``SSCursor``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)
        return SyncRowStream(PymysqlStreamSource(self, sql, prepared_parameters, chunk_size, PYMYSQL_JSON_TYPE_CODES))

    def handle_database_exceptions(self) -> "PyMysqlExceptionHandler":
        return PyMysqlExceptionHandler()

    def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        pipeline = self._storage_pipeline()
        telemetry_payload = self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = f"TRUNCATE TABLE {format_identifier(table)}"
            exc_handler = self.handle_database_exceptions()
            with exc_handler, self.with_cursor(self.connection) as cursor:
                cursor.execute(statement)
            if exc_handler.pending_exception is not None:
                raise exc_handler.pending_exception from None

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            needs_preparation = self._arrow_rows_need_preparation(arrow_table)
            use_infile = bool(self.driver_features.get("enable_local_infile_bulk_load")) and not needs_preparation
            if use_infile:
                payload = encode_records_for_local_infile(records)
                fd, tmp_name = tempfile.mkstemp(suffix=".tsv")
                try:
                    with os.fdopen(fd, "wb") as tmp:
                        tmp.write(payload)
                    load_sql = build_load_data_statement(table, columns)
                    exc_handler = self.handle_database_exceptions()
                    with exc_handler, self.with_cursor(self.connection) as cursor:
                        cursor.execute(load_sql, (tmp_name,))
                    if exc_handler.pending_exception is not None:
                        raise exc_handler.pending_exception from None
                finally:
                    Path(tmp_name).unlink(missing_ok=True)
            else:
                insert_sql = build_insert_statement(table, columns)
                prepared_records = (
                    self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                    if needs_preparation
                    else records
                )
                exc_handler = self.handle_database_exceptions()
                with exc_handler, self.with_cursor(self.connection) as cursor:
                    cursor.executemany(insert_sql, cast("Any", prepared_records))
                if exc_handler.pending_exception is not None:
                    raise exc_handler.pending_exception from None

        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    @property
    def data_dictionary(self) -> "PyMysqlDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = PyMysqlDataDictionary()
        return self._data_dictionary

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect PyMySQL rows for the direct execution path."""
        description = cursor.description or None
        row_plan = resolve_row_plan(description, PYMYSQL_JSON_TYPE_CODES)
        rows, column_names, _row_format = collect_rows(fetched, row_plan, self._json_deserializer, logger=logger)
        return rows, column_names, len(rows)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from PyMySQL cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        try:
            return bool(self.connection.server_status & PyMysqlServerStatus.SERVER_STATUS_IN_TRANS)
        except Exception:
            return False


register_driver_profile("pymysql", driver_profile)


def _resolve_column_types(description: Any) -> "dict[str, str] | None":
    """Map MySQL cursor column FIELD_TYPE codes to neutral Arrow type tokens.

    Returns ``None`` when the cursor has no description or reports no
    recognizable type codes.
    """
    if not description:
        return None
    column_types: dict[str, str] = {}
    for col in description:
        token = _MYSQL_TYPE_CODE_TOKENS.get(col[1])
        if token is not None:
            column_types[col[0]] = token
    return column_types or None
