"""PyMySQL MySQL driver implementation."""

from typing import TYPE_CHECKING, Any, Final, cast

import pymysql
from pymysql.constants import FIELD_TYPE

from sqlspec.adapters.pymysql.core import (
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    detect_json_columns,
    driver_profile,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    normalize_lastrowid,
    resolve_rowcount,
)
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.type_guards import supports_json_type

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.pymysql._typing import PyMysqlConnection
    from sqlspec.core import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.pymysql._typing import PyMysqlSessionContext

__all__ = ("PyMysqlCursor", "PyMysqlDriver", "PyMysqlExceptionHandler", "PyMysqlSessionContext")

logger = get_logger("sqlspec.adapters.pymysql")

json_type_value = FIELD_TYPE.JSON if supports_json_type(FIELD_TYPE) else None
PYMYSQL_JSON_TYPE_CODES: Final[set[int]] = {json_type_value} if json_type_value is not None else set()


class PyMysqlCursor:
    """Context manager for PyMySQL cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PyMysqlConnection") -> None:
        self.connection = connection
        self.cursor: Any | None = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class PyMysqlExceptionHandler:
    """Context manager for handling PyMySQL exceptions."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "PyMysqlExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, pymysql.MySQLError):
            result = create_mapped_exception(exc_val, logger=logger)
            if result is True:
                return True
            self.pending_exception = cast("Exception", result)
            return True
        return False


class PyMysqlDriver(SyncDriverAdapterBase):
    """MySQL/MariaDB database driver using PyMySQL."""

    __slots__ = ("_data_dictionary",)
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

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            fetched_rows = list(fetched_data) if fetched_data else None
            description = list(cursor.description) if cursor.description else None
            json_indexes = detect_json_columns(cursor, PYMYSQL_JSON_TYPE_CODES)
            deserializer = cast("Callable[[Any], Any]", self.driver_features.get("json_deserializer", from_json))
            rows, column_names = collect_rows(fetched_rows, description, json_indexes, deserializer, logger=logger)

            return self.create_execution_result(
                cursor, selected_data=rows, column_names=column_names, data_row_count=len(rows), is_select_result=True
            )

        affected_rows = resolve_rowcount(cursor)
        last_id = normalize_lastrowid(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_id)

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        cursor.executemany(sql, prepared_parameters)

        affected_rows = len(prepared_parameters)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
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
            with PyMysqlCursor(self.connection) as cursor:
                cursor.execute("BEGIN")
        except pymysql.MySQLError as exc:
            msg = f"Failed to begin MySQL transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def commit(self) -> None:
        try:
            self.connection.commit()
        except pymysql.MySQLError as exc:
            msg = f"Failed to commit MySQL transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except pymysql.MySQLError as exc:
            msg = f"Failed to rollback MySQL transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: "PyMysqlConnection") -> "PyMysqlCursor":
        return PyMysqlCursor(connection)

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
        telemetry_payload = self._write_result_to_storage_sync(
            arrow_result, destination, format_hint=format_hint, pipeline=pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

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
            with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                cursor.execute(statement)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = build_insert_statement(table, columns)
            with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                cursor.executemany(insert_sql, records)

        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    @property
    def data_dictionary(self) -> "PyMysqlDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = PyMysqlDataDictionary()
        return self._data_dictionary

    def _connection_in_transaction(self) -> bool:
        get_autocommit = getattr(self.connection, "get_autocommit", None)
        if callable(get_autocommit):
            return not bool(get_autocommit())
        autocommit = getattr(self.connection, "autocommit", None)
        if autocommit is not None:
            try:
                return not bool(autocommit)
            except Exception:
                return False
        return False


register_driver_profile("pymysql", driver_profile)
