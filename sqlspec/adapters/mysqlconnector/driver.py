"""MysqlConnector MySQL driver implementation.

Provides MySQL/MariaDB connectivity with parameter style conversion,
type coercion, error handling, and transaction management.
"""

from typing import TYPE_CHECKING, Any, Final, cast

import mysql.connector
from mysql.connector.constants import FieldType

from sqlspec.adapters.mysqlconnector.core import (
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
from sqlspec.adapters.mysqlconnector.data_dictionary import (
    MysqlConnectorAsyncDataDictionary,
    MysqlConnectorSyncDataDictionary,
)
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.type_guards import supports_json_type

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorAsyncConnection, MysqlConnectorSyncConnection
    from sqlspec.core import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorAsyncSessionContext, MysqlConnectorSyncSessionContext

__all__ = (
    "MysqlConnectorAsyncCursor",
    "MysqlConnectorAsyncDriver",
    "MysqlConnectorAsyncExceptionHandler",
    "MysqlConnectorAsyncSessionContext",
    "MysqlConnectorSyncCursor",
    "MysqlConnectorSyncDriver",
    "MysqlConnectorSyncExceptionHandler",
    "MysqlConnectorSyncSessionContext",
)

logger = get_logger("sqlspec.adapters.mysqlconnector")

json_type_value = FieldType.JSON if supports_json_type(FieldType) else None
MYSQLCONNECTOR_JSON_TYPE_CODES: Final[set[int]] = {json_type_value} if json_type_value is not None else set()


class MysqlConnectorSyncCursor:
    """Context manager for mysql-connector sync cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "MysqlConnectorSyncConnection") -> None:
        self.connection = connection
        self.cursor: Any | None = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor(dictionary=True)
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class MysqlConnectorSyncExceptionHandler:
    """Context manager for handling mysql-connector sync exceptions."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "MysqlConnectorSyncExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, mysql.connector.Error):
            result = create_mapped_exception(exc_val, logger=logger)
            if result is True:
                return True
            self.pending_exception = cast("Exception", result)
            return True
        return False


class MysqlConnectorSyncDriver(SyncDriverAdapterBase):
    """MySQL/MariaDB database driver using mysql-connector sync library."""

    __slots__ = ("_data_dictionary",)
    dialect = "mysql"

    def __init__(
        self,
        connection: "MysqlConnectorSyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: MysqlConnectorSyncDataDictionary | None = None

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            fetched_rows = list(fetched_data) if fetched_data else None
            description = list(cursor.description) if cursor.description else None
            json_indexes = detect_json_columns(cursor, MYSQLCONNECTOR_JSON_TYPE_CODES)
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
            with MysqlConnectorSyncCursor(self.connection) as cursor:
                cursor.execute("BEGIN")
        except mysql.connector.Error as e:
            msg = f"Failed to begin MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        try:
            self.connection.commit()
        except mysql.connector.Error as e:
            msg = f"Failed to commit MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except mysql.connector.Error as e:
            msg = f"Failed to rollback MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    def with_cursor(self, connection: "MysqlConnectorSyncConnection") -> "MysqlConnectorSyncCursor":
        return MysqlConnectorSyncCursor(connection)

    def handle_database_exceptions(self) -> "MysqlConnectorSyncExceptionHandler":
        return MysqlConnectorSyncExceptionHandler()

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
    def data_dictionary(self) -> "MysqlConnectorSyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = MysqlConnectorSyncDataDictionary()
        return self._data_dictionary

    def _connection_in_transaction(self) -> bool:
        autocommit = getattr(self.connection, "autocommit", None)
        if autocommit is not None:
            try:
                return not bool(autocommit)
            except Exception:
                return False
        return False


class MysqlConnectorAsyncCursor:
    """Async context manager for mysql-connector async cursor operations."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "MysqlConnectorAsyncConnection") -> None:
        self.connection = connection
        self.cursor: Any | None = None

    async def __aenter__(self) -> Any:
        self.cursor = await self.connection.cursor(dictionary=True)
        return self.cursor

    async def __aexit__(self, *_: Any) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class MysqlConnectorAsyncExceptionHandler:
    """Async context manager for handling mysql-connector exceptions."""

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "MysqlConnectorAsyncExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, mysql.connector.Error):
            result = create_mapped_exception(exc_val, logger=logger)
            if result is True:
                return True
            self.pending_exception = cast("Exception", result)
            return True
        return False


class MysqlConnectorAsyncDriver(AsyncDriverAdapterBase):
    """MySQL/MariaDB database driver using mysql-connector async library."""

    __slots__ = ("_data_dictionary",)
    dialect = "mysql"

    def __init__(
        self,
        connection: "MysqlConnectorAsyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: MysqlConnectorAsyncDataDictionary | None = None

    async def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            fetched_rows = list(fetched_data) if fetched_data else None
            description = list(cursor.description) if cursor.description else None
            json_indexes = detect_json_columns(cursor, MYSQLCONNECTOR_JSON_TYPE_CODES)
            deserializer = cast("Callable[[Any], Any]", self.driver_features.get("json_deserializer", from_json))
            rows, column_names = collect_rows(fetched_rows, description, json_indexes, deserializer, logger=logger)

            return self.create_execution_result(
                cursor, selected_data=rows, column_names=column_names, data_row_count=len(rows), is_select_result=True
            )

        affected_rows = resolve_rowcount(cursor)
        last_id = normalize_lastrowid(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_id)

    async def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        await cursor.executemany(sql, prepared_parameters)

        affected_rows = len(prepared_parameters)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def begin(self) -> None:
        try:
            async with MysqlConnectorAsyncCursor(self.connection) as cursor:
                await cursor.execute("BEGIN")
        except mysql.connector.Error as e:
            msg = f"Failed to begin MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        try:
            await self.connection.commit()
        except mysql.connector.Error as e:
            msg = f"Failed to commit MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        try:
            await self.connection.rollback()
        except mysql.connector.Error as e:
            msg = f"Failed to rollback MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    def with_cursor(self, connection: "MysqlConnectorAsyncConnection") -> "MysqlConnectorAsyncCursor":
        return MysqlConnectorAsyncCursor(connection)

    def handle_database_exceptions(self) -> "MysqlConnectorAsyncExceptionHandler":
        return MysqlConnectorAsyncExceptionHandler()

    async def select_to_storage(
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
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_result_to_storage_async(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
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
            async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                await cursor.execute(statement)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = build_insert_statement(table, columns)
            async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                await cursor.executemany(insert_sql, records)

        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    @property
    def data_dictionary(self) -> "MysqlConnectorAsyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = MysqlConnectorAsyncDataDictionary()
        return self._data_dictionary

    def _connection_in_transaction(self) -> bool:
        in_tx = getattr(self.connection, "in_transaction", None)
        if in_tx is not None:
            try:
                return bool(in_tx)
            except Exception:
                return False
        return False


register_driver_profile("mysql-connector", driver_profile)
