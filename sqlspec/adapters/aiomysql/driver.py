"""aiomysql MySQL driver implementation.

Provides MySQL/MariaDB connectivity with parameter style conversion,
type coercion, error handling, and transaction management.

aiomysql is built on top of PyMySQL, so error classes come from pymysql.err
rather than a driver-specific error module.
"""

import asyncio
import os
import tempfile
from collections.abc import Sized
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from sqlspec.adapters.aiomysql._typing import (
    AIOMYSQL_INSERT_VALUES_PATTERN,
    AiomysqlCursor,
    AiomysqlFieldType,
    AiomysqlPymysqlError,
    AiomysqlPymysqlMySQLError,
    AiomysqlSessionContext,
)
from sqlspec.adapters.aiomysql.core import (
    AiomysqlStreamSource,
    build_insert_statement,
    build_load_data_statement,
    can_use_local_infile_records,
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
from sqlspec.adapters.aiomysql.data_dictionary import AiomysqlDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase, AsyncRowStream, BaseAsyncExceptionHandler
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.type_guards import supports_json_type

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlspec.adapters.aiomysql._typing import AiomysqlConnection
    from sqlspec.core import SQL, SQLResult, StatementConfig
    from sqlspec.driver import CachedQuery, ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("AiomysqlCursor", "AiomysqlDriver", "AiomysqlExceptionHandler", "AiomysqlSessionContext")

logger = get_logger("sqlspec.adapters.aiomysql")

json_type_value = (
    AiomysqlFieldType.JSON if AiomysqlFieldType is not None and supports_json_type(AiomysqlFieldType) else None
)
AIOMYSQL_JSON_TYPE_CODES: Final[set[int]] = {json_type_value} if json_type_value is not None else set()

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


class AiomysqlExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for handling aiomysql (MySQL) database exceptions.

    Maps MySQL error codes and SQLSTATE values from ``pymysql.err.Error`` to
    SQLSpec exceptions stored on ``pending_exception``.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, AiomysqlPymysqlError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class AiomysqlDriver(AsyncDriverAdapterBase):
    """MySQL/MariaDB database driver using aiomysql client library.

    Implements asynchronous database operations for MySQL and MariaDB servers
    with support for parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ("_data_dictionary", "_json_deserializer")
    dialect = "mysql"

    def __init__(
        self,
        connection: "AiomysqlConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AiomysqlDataDictionary | None = None
        features = driver_features or {}
        self._json_deserializer: Callable[[Any], Any] = cast(
            "Callable[[Any], Any]", features.get("json_deserializer", from_json)
        )

    @property
    def data_dictionary(self) -> "AiomysqlDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AiomysqlDataDictionary()
        return self._data_dictionary

    async def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: aiomysql cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult: Statement execution results with data or row counts
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)
        await cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        description = getattr(cursor, "description", None) or None
        if statement.returns_rows() or description is not None:
            fetched_data = await cursor.fetchall()
            row_plan = resolve_row_plan(description, AIOMYSQL_JSON_TYPE_CODES)
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

    async def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL statement with multiple parameter sets.

        Args:
            cursor: aiomysql cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult: Batch execution results
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        match = AIOMYSQL_INSERT_VALUES_PATTERN.match(sql)
        if match:
            sql = (
                escape_literal_percent(match.group(1), prepared_parameters, self.statement_config.parameter_validator)
                + sql[match.end(1) :]
            )
        else:
            sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        parameter_count = len(prepared_parameters) if isinstance(prepared_parameters, Sized) else None
        await cursor.executemany(sql, prepared_parameters)

        affected_rows = resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: aiomysql cursor object
            statement: SQL script to execute

        Returns:
            ExecutionResult: Script execution results with statement count
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor
        normalized_params = normalize_execute_parameters(prepared_parameters)

        for stmt in statements:
            stmt_sql = (
                escape_literal_percent(stmt, prepared_parameters, self.statement_config.parameter_validator)
                if normalized_params is not None
                else stmt
            )
            await cursor.execute(stmt_sql, normalized_params)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def begin(self) -> None:
        """Begin a database transaction.

        Raises:
            SQLSpecError: If transaction initialization fails
        """
        try:
            await self.connection.begin()
        except AiomysqlPymysqlMySQLError as e:
            raise create_mapped_exception(e) from e

    async def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If transaction commit fails
        """
        try:
            await self.connection.commit()
        except AiomysqlPymysqlMySQLError as e:
            raise create_mapped_exception(e) from e

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If transaction rollback fails
        """
        try:
            await self.connection.rollback()
        except AiomysqlPymysqlMySQLError as e:
            raise create_mapped_exception(e) from e

    def with_cursor(self, connection: "AiomysqlConnection") -> "AiomysqlCursor":
        """Create cursor context manager for the connection.

        Args:
            connection: aiomysql database connection

        Returns:
            AiomysqlCursor: Context manager for cursor operations
        """
        return AiomysqlCursor(connection)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native aiomysql row stream backed by an unbuffered ``SSCursor``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)
        return AsyncRowStream(
            AiomysqlStreamSource(self, sql, prepared_parameters, chunk_size, AIOMYSQL_JSON_TYPE_CODES)
        )

    def handle_database_exceptions(self) -> "AiomysqlExceptionHandler":
        """Provide exception handling context manager.

        Returns:
            AiomysqlExceptionHandler: Context manager for aiomysql exception handling
        """
        return AiomysqlExceptionHandler()

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
        """Execute a query and stream Arrow-formatted results into storage."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into MySQL using batched inserts."""

        self._require_capability("arrow_import_enabled")
        if stage_table or conflict_keys or drop_indexes:
            return await self._execute_staged_load_async(
                table,
                source,
                raw_loader=self.load_from_arrow,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
            )
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = f"TRUNCATE TABLE {format_identifier(table)}"
            exc_handler = self.handle_database_exceptions()
            await self._run_with_exception_handler(exc_handler, self._execute_cursor_statement, statement)
            self._check_pending_exception(exc_handler)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            needs_preparation = self._arrow_rows_need_preparation(arrow_table)
            use_infile = (
                bool(self.driver_features.get("enable_local_infile_bulk_load"))
                and not needs_preparation
                and can_use_local_infile_records(arrow_table, records)
            )
            if use_infile:
                payload = encode_records_for_local_infile(records)
                fd, tmp_name = tempfile.mkstemp(suffix=".tsv")
                try:
                    with os.fdopen(fd, "wb") as tmp:
                        tmp.write(payload)
                    load_sql = build_load_data_statement(table, columns)
                    exc_handler = self.handle_database_exceptions()
                    await self._run_with_exception_handler(
                        exc_handler, self._execute_cursor_statement, load_sql, (tmp_name,)
                    )
                    self._check_pending_exception(exc_handler)
                finally:
                    await asyncio.to_thread(Path(tmp_name).unlink, True)
            else:
                insert_sql = build_insert_statement(table, columns)
                prepared_records = (
                    self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                    if needs_preparation
                    else records
                )
                exc_handler = self.handle_database_exceptions()
                await self._run_with_exception_handler(
                    exc_handler, self._executemany_cursor_statement, insert_sql, prepared_records
                )
                self._check_pending_exception(exc_handler)

        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage into MySQL."""

        return await super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect aiomysql rows for the direct execution path."""
        description = cursor.description or None
        row_plan = resolve_row_plan(description, AIOMYSQL_JSON_TYPE_CODES)
        rows, column_names, _row_format = collect_rows(fetched, row_plan, self._json_deserializer, logger=logger)
        return rows, column_names, len(rows)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from aiomysql cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    async def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        statement = self._cached_statement(
            sql, params, cached, params, params_are_simple=True, compiled_sql=cached.compiled_sql
        )
        return await self._execute_cached_statement(statement)

    async def _execute_cursor_statement(self, statement: str, parameters: Any = None) -> None:
        async with self.with_cursor(self.connection) as cursor:
            if parameters is None:
                await cursor.execute(statement)
            else:
                await cursor.execute(statement, parameters)

    async def _executemany_cursor_statement(self, statement: str, parameters: Any) -> None:
        async with self.with_cursor(self.connection) as cursor:
            await cursor.executemany(statement, parameters)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True when the server reports an open transaction.
        """
        return bool(self.connection.get_transaction_status())


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


register_driver_profile("aiomysql", driver_profile)
