"""AIOSQLite driver implementation for async SQLite operations."""

import asyncio
import contextlib
import inspect
import secrets
from typing import TYPE_CHECKING, Any, Literal, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.aiosqlite._typing import AiosqliteCursor, AiosqliteRawCursor, AiosqliteSessionContext
from sqlspec.adapters.aiosqlite._typing import aiosqlite_module as aiosqlite
from sqlspec.adapters.aiosqlite._typing import aiosqlite_sqlite_module as sqlite3
from sqlspec.adapters.aiosqlite.core import (
    AiosqliteStreamSource,
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    end_transaction,
    execute_and_resolve_metadata,
    execute_fetchall_with_metadata,
    execute_many_on_worker_thread,
    format_identifier,
    normalize_execute_parameters,
    resolve_rowcount,
    run_on_worker_thread,
)
from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.core.result import DMLResult
from sqlspec.driver import AsyncDriverAdapterBase, AsyncRowStream, BaseAsyncExceptionHandler
from sqlspec.exceptions import DeadlockError
from sqlspec.utils.type_guards import resolve_row_format

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection
    from sqlspec.core import SQL, SQLResult, StatementConfig
    from sqlspec.core.compiler import OperationType
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = (
    "AiosqliteCursor",
    "AiosqliteDriver",
    "AiosqliteExceptionHandler",
    "AiosqliteRawCursor",
    "AiosqliteSessionContext",
)

_execute_and_resolve_metadata = execute_and_resolve_metadata
_execute_fetchall_with_metadata = execute_fetchall_with_metadata
random = secrets.SystemRandom()


@mypyc_attr(allow_interpreted_subclasses=True)
class AiosqliteExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager for handling aiosqlite database exceptions.

    Maps SQLite extended result codes to SQLSpec exceptions stored on
    ``pending_exception``.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        _ = exc_type
        if isinstance(exc_val, (aiosqlite.Error, sqlite3.Error)):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


@mypyc_attr(allow_interpreted_subclasses=True)
class AiosqliteDriver(AsyncDriverAdapterBase):
    """AIOSQLite driver for async SQLite database operations."""

    __slots__ = ("_data_dictionary", "_rowid_target_cache")
    dialect = "sqlite"

    def __init__(
        self,
        connection: "AiosqliteConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AiosqliteDataDictionary | None = None
        self._rowid_target_cache: dict[tuple[str | None, str], bool] = {}

    @property
    def data_dictionary(self) -> "AiosqliteDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AiosqliteDataDictionary()
        return self._data_dictionary

    async def dispatch_execute(self, cursor: "AiosqliteRawCursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement."""
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        self._invalidate_rowid_target_cache(statement.operation_type)
        normalized_parameters = normalize_execute_parameters(prepared_parameters)

        if statement.returns_rows():
            fetched_data, description, _affected_rows, last_inserted_id = await run_on_worker_thread(
                self.connection,
                _execute_fetchall_with_metadata,
                self.connection,
                sql,
                normalized_parameters,
                statement.operation_type,
                statement._peek_expression(),
                self._rowid_target_cache,
            )
            data, column_names, row_count = collect_rows(fetched_data, description)
            row_format = resolve_row_format(data)

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=row_count,
                is_select_result=True,
                row_format=row_format,
                last_inserted_id=last_inserted_id,
            )

        affected_rows, last_inserted_id = await run_on_worker_thread(
            self.connection,
            _execute_and_resolve_metadata,
            self.connection,
            sql,
            normalized_parameters,
            statement.operation_type,
            statement._peek_expression(),
            self._rowid_target_cache,
        )
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_inserted_id)

    async def dispatch_execute_many(self, cursor: "AiosqliteRawCursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets."""
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        self._invalidate_rowid_target_cache(statement.operation_type)
        affected_rows = await run_on_worker_thread(
            self.connection, execute_many_on_worker_thread, self.connection, sql, prepared_parameters
        )
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "AiosqliteRawCursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script."""
        self._rowid_target_cache.clear()
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        try:
            for stmt in statements:
                await cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
                successful_count += 1
        finally:
            self._rowid_target_cache.clear()

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def begin(self, mode: "Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE'] | None" = None) -> None:
        """Begin a database transaction.

        Args:
            mode: Transaction locking mode (DEFERRED, IMMEDIATE, EXCLUSIVE).
                Falls back to ``driver_features['default_transaction_mode']``,
                or ``IMMEDIATE`` when neither is set.
        """
        transaction_mode = (
            mode if mode is not None else self.driver_features.get("default_transaction_mode", "IMMEDIATE")
        )
        stmt = f"BEGIN {transaction_mode}" if transaction_mode else "BEGIN"
        if transaction_mode is not None and transaction_mode not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
            msg = "Transaction mode must be DEFERRED, IMMEDIATE, or EXCLUSIVE"
            raise ValueError(msg)
        try:
            if not await _check_in_transaction(self.connection):
                cursor = await self.connection.execute(stmt)
                if cursor is not None:
                    with contextlib.suppress(Exception):
                        close_result = cursor.close()
                        if inspect.isawaitable(close_result):
                            await close_result
        except (aiosqlite.Error, sqlite3.Error) as e:
            await _retry_begin_with_backoff(self.connection, e, statement=stmt)

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await end_transaction(self.connection, commit=True)
        except (aiosqlite.Error, sqlite3.Error) as e:
            raise create_mapped_exception(e) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await end_transaction(self.connection, commit=False)
            self._rowid_target_cache.clear()
        except (aiosqlite.Error, sqlite3.Error) as e:
            self._rowid_target_cache.clear()
            raise create_mapped_exception(e) from e

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        """Create async context manager for AIOSQLite cursor."""
        return AiosqliteCursor(connection)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        """Return a native aiosqlite row stream backed by chunked ``fetchmany``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return AsyncRowStream(AiosqliteStreamSource(self, sql, prepared_parameters, chunk_size))

    def handle_database_exceptions(self) -> "AiosqliteExceptionHandler":
        """Handle AIOSQLite-specific exceptions."""
        return AiosqliteExceptionHandler()

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
        """Execute a query and stream Arrow results into storage."""

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
        batch_size: int = 10000,
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
        """Load Arrow data into SQLite using batched inserts."""
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)
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
        columns = arrow_table.column_names
        insert_sql = build_insert_statement(table, columns)
        needs_prep = self._arrow_rows_need_preparation(arrow_table)

        owns_transaction = not await _check_in_transaction(self.connection)
        try:
            if owns_transaction:
                await self.begin()
            if overwrite:
                statement = f"DELETE FROM {format_identifier(table)}"
                async with self.with_cursor(self.connection) as cursor:
                    await cursor.execute(statement)
            for batch in arrow_table.to_batches(max_chunksize=batch_size):
                pydict = batch.to_pydict()
                records = list(zip(*(pydict[col] for col in columns), strict=False))
                if records:
                    prepared_records = (
                        self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                        if needs_prep
                        else records
                    )
                    async with self.with_cursor(self.connection) as cursor:
                        await cursor.executemany(insert_sql, cast("Any", prepared_records))
            if owns_transaction:
                await self.commit()
        except BaseException as exc:
            if owns_transaction:
                await self.rollback()
            if isinstance(exc, (aiosqlite.Error, sqlite3.Error)):
                raise create_mapped_exception(exc) from exc
            raise

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
        """Load staged artifacts from storage into SQLite."""

        return await super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect aiosqlite rows for the direct execution path."""
        return collect_rows(fetched, cursor.description)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from aiosqlite cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    async def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: Any
    ) -> "SQLResult":
        """Execute cached queries through the async cursor fast path."""
        normalized_parameters = normalize_execute_parameters(params)
        exc_handler = self.handle_database_exceptions()
        result: SQLResult | None = None
        self._invalidate_rowid_target_cache(cached.operation_type)
        async with exc_handler:
            if cached.operation_profile.returns_rows:
                fetched_data, description, _affected_rows, last_inserted_id = await run_on_worker_thread(
                    self.connection,
                    _execute_fetchall_with_metadata,
                    self.connection,
                    cached.compiled_sql,
                    normalized_parameters,
                    cached.operation_type,
                    cached.processed_state.parsed_expression,
                    self._rowid_target_cache,
                )
                data, column_names, row_count = collect_rows(fetched_data, description)
                result = self.build_cached_select_result(
                    sql,
                    params,
                    cached,
                    selected_data=data,
                    column_names=column_names,
                    data_row_count=row_count,
                    row_format=resolve_row_format(data),
                    last_inserted_id=last_inserted_id,
                )
            else:
                affected_rows, last_inserted_id = await run_on_worker_thread(
                    self.connection,
                    _execute_and_resolve_metadata,
                    self.connection,
                    cached.compiled_sql,
                    normalized_parameters,
                    cached.operation_type,
                    cached.processed_state.parsed_expression,
                    self._rowid_target_cache,
                )
                result = DMLResult(cached.operation_type, affected_rows, last_inserted_id)

        self._check_pending_exception(exc_handler)
        assert result is not None
        return result

    def _invalidate_rowid_target_cache(self, operation_type: "OperationType") -> None:
        if operation_type not in {"SELECT", "INSERT", "UPDATE", "DELETE"}:
            self._rowid_target_cache.clear()

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)


def _read_in_transaction_on_worker(raw_conn: Any) -> bool:
    return bool(getattr(raw_conn, "in_transaction", False))


async def _check_in_transaction(connection: "AiosqliteConnection") -> bool:
    raw_conn = getattr(connection, "_conn", None)
    if isinstance(raw_conn, sqlite3.Connection) and callable(getattr(connection, "_execute", None)):
        return await run_on_worker_thread(connection, _read_in_transaction_on_worker, raw_conn)
    return bool(getattr(connection, "in_transaction", False))


def _is_busy_or_locked_error(error: BaseException) -> bool:
    code = getattr(error, "sqlite_errorcode", None)
    if isinstance(code, int) and code in {5, 6}:
        return True
    message = str(error).lower()
    return "locked" in message or "busy" in message


async def _retry_begin_with_backoff(
    connection: "AiosqliteConnection",
    initial_error: "aiosqlite.Error | sqlite3.Error",
    max_retries: int = 3,
    *,
    statement: str = "BEGIN IMMEDIATE",
) -> None:
    """Retry transaction start after SQLite reports a busy or locked connection.

    Args:
        connection: Aiosqlite connection used to retry the transaction start.
        initial_error: Error raised by the first transaction-start attempt.
        max_retries: Maximum number of retry attempts.
        statement: SQL statement used to start the transaction.

    Raises:
        SQLSpecError: If a non-transient error occurs or every retry attempt fails.
    """
    if not _is_busy_or_locked_error(initial_error):
        raise create_mapped_exception(initial_error) from initial_error

    for attempt in range(max_retries):
        delay = 0.01 * (2**attempt) + random.uniform(0, 0.01)
        await asyncio.sleep(delay)
        try:
            cursor = await connection.execute(statement)
            if cursor is not None:
                with contextlib.suppress(Exception):
                    close_result = cursor.close()
                    if inspect.isawaitable(close_result):
                        await close_result
        except (aiosqlite.Error, sqlite3.Error) as exc:
            if not _is_busy_or_locked_error(exc):
                raise create_mapped_exception(exc) from exc
            if attempt == max_retries - 1:
                break
        else:
            return
    msg = f"Failed to begin transaction after retries: {initial_error}"
    raise DeadlockError(msg) from initial_error


register_driver_profile("aiosqlite", driver_profile)
