"""AIOSQLite driver implementation for async SQLite operations."""

import asyncio
import contextlib
import random
import sqlite3
from typing import TYPE_CHECKING, Any, cast

import aiosqlite

from sqlspec.adapters.aiosqlite.core import (
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_rowcount,
)
from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection
    from sqlspec.core import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.aiosqlite._typing import AiosqliteSessionContext

__all__ = ("AiosqliteCursor", "AiosqliteDriver", "AiosqliteExceptionHandler", "AiosqliteSessionContext")

SQLITE_CONSTRAINT_UNIQUE_CODE = 2067
SQLITE_CONSTRAINT_FOREIGNKEY_CODE = 787
SQLITE_CONSTRAINT_NOTNULL_CODE = 1811
SQLITE_CONSTRAINT_CHECK_CODE = 531
SQLITE_CONSTRAINT_CODE = 19
SQLITE_CANTOPEN_CODE = 14
SQLITE_IOERR_CODE = 10
SQLITE_MISMATCH_CODE = 20


class AiosqliteCursor:
    """Async context manager for AIOSQLite cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection
        self.cursor: aiosqlite.Cursor | None = None

    async def __aenter__(self) -> "aiosqlite.Cursor":
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if exc_type is not None:
            return
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class AiosqliteExceptionHandler:
    """Async context manager for handling aiosqlite database exceptions.

    Maps SQLite extended result codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "AiosqliteExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_val is None:
            return False
        if isinstance(exc_val, (aiosqlite.Error, sqlite3.Error)):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class AiosqliteDriver(AsyncDriverAdapterBase):
    """AIOSQLite driver for async SQLite database operations."""

    __slots__ = ("_data_dictionary",)
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

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()

            # aiosqlite returns Iterable[Row], core helper expects Iterable[Any]
            # Use cast to satisfy mypy and pyright
            data, column_names, row_count = collect_rows(cast("list[Any]", fetched_data), cursor.description)

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=row_count, is_select_result=True
            )

        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def dispatch_execute_many(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        await cursor.executemany(sql, normalize_execute_many_parameters(prepared_parameters))

        affected_rows = resolve_rowcount(cursor)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script."""
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

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            if not self.connection.in_transaction:
                await self.connection.execute("BEGIN IMMEDIATE")
        except aiosqlite.Error as e:
            max_retries = 3
            for attempt in range(max_retries):
                delay = 0.01 * (2**attempt) + random.uniform(0, 0.01)  # noqa: S311
                await asyncio.sleep(delay)
                try:
                    await self.connection.execute("BEGIN IMMEDIATE")
                except aiosqlite.Error:
                    if attempt == max_retries - 1:
                        break
                else:
                    return
            msg = f"Failed to begin transaction after retries: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.commit()
        except aiosqlite.Error as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.rollback()
        except aiosqlite.Error as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        """Create async context manager for AIOSQLite cursor."""
        return AiosqliteCursor(connection)

    def handle_database_exceptions(self) -> "AiosqliteExceptionHandler":
        """Handle AIOSQLite-specific exceptions."""
        return AiosqliteExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

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
        """Load Arrow data into SQLite using batched inserts."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = f"DELETE FROM {format_identifier(table)}"
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
        """Load staged artifacts from storage into SQLite."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "AiosqliteDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AiosqliteDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)


register_driver_profile("aiosqlite", driver_profile)
