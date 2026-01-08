"""AIOSQLite driver implementation for async SQLite operations."""

import asyncio
import contextlib
import random
import sqlite3
from typing import TYPE_CHECKING, Any, cast

import aiosqlite

from sqlspec.adapters.aiosqlite.core import (
    aiosqlite_statement_config,
    build_aiosqlite_profile,
    build_sqlite_insert_statement,
    format_sqlite_identifier,
    process_sqlite_result,
)
from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.type_guards import has_sqlite_error

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite._typing import AiosqliteConnection
    from sqlspec.core import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._async import AsyncDataDictionaryBase
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.aiosqlite._typing import AiosqliteSessionContext

__all__ = (
    "AiosqliteCursor",
    "AiosqliteDriver",
    "AiosqliteExceptionHandler",
    "AiosqliteSessionContext",
    "aiosqlite_statement_config",
)

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
            try:
                self._map_sqlite_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
            return False
        return False

    def _map_sqlite_exception(self, e: BaseException) -> None:
        """Map SQLite exception to SQLSpec exception.

        Args:
            e: aiosqlite.Error instance

        Raises:
            Specific SQLSpec exception based on error code
        """
        exc: BaseException = e
        if has_sqlite_error(e):
            error_code = e.sqlite_errorcode
            error_name = e.sqlite_errorname
        else:
            error_code = None
            error_name = None
        error_msg = str(exc).lower()

        if "locked" in error_msg:
            msg = f"AIOSQLite database locked: {exc}. Consider enabling WAL mode or reducing concurrency."
            raise SQLSpecError(msg) from exc

        if not error_code:
            if "unique constraint" in error_msg:
                self._raise_unique_violation(e, 0)
            elif "foreign key constraint" in error_msg:
                self._raise_foreign_key_violation(e, 0)
            elif "not null constraint" in error_msg:
                self._raise_not_null_violation(e, 0)
            elif "check constraint" in error_msg:
                self._raise_check_violation(e, 0)
            elif "syntax" in error_msg:
                self._raise_parsing_error(e, None)
            else:
                self._raise_generic_error(e)
            return

        if error_code == SQLITE_CONSTRAINT_UNIQUE_CODE or error_name == "SQLITE_CONSTRAINT_UNIQUE":
            self._raise_unique_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_FOREIGNKEY_CODE or error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_NOTNULL_CODE or error_name == "SQLITE_CONSTRAINT_NOTNULL":
            self._raise_not_null_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_CHECK_CODE or error_name == "SQLITE_CONSTRAINT_CHECK":
            self._raise_check_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_CODE or error_name == "SQLITE_CONSTRAINT":
            self._raise_integrity_error(e, error_code)
        elif error_code == SQLITE_CANTOPEN_CODE or error_name == "SQLITE_CANTOPEN":
            self._raise_connection_error(e, error_code)
        elif error_code == SQLITE_IOERR_CODE or error_name == "SQLITE_IOERR":
            self._raise_operational_error(e, error_code)
        elif error_code == SQLITE_MISMATCH_CODE or error_name == "SQLITE_MISMATCH":
            self._raise_data_error(e, error_code)
        elif error_code == 1 or "syntax" in error_msg:
            self._raise_parsing_error(e, error_code)
        else:
            self._raise_generic_error(e)

    def _raise_unique_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite unique constraint violation [code {code}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite foreign key constraint violation [code {code}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite not-null constraint violation [code {code}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite check constraint violation [code {code}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: int) -> None:
        msg = f"SQLite integrity constraint violation [code {code}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[code {code}]" if code else ""
        msg = f"SQLite SQL syntax error {code_str}: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: int) -> None:
        msg = f"SQLite connection error [code {code}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_operational_error(self, e: Any, code: int) -> None:
        msg = f"SQLite operational error [code {code}]: {e}"
        raise OperationalError(msg) from e

    def _raise_data_error(self, e: Any, code: int) -> None:
        msg = f"SQLite data error [code {code}]: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"SQLite database error: {e}"
        raise SQLSpecError(msg) from e


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
            cache_config = get_cache_config()
            statement_config = aiosqlite_statement_config.replace(enable_caching=cache_config.compiled_cache_enabled)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncDataDictionaryBase[Any] | None = None

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        """Create async context manager for AIOSQLite cursor."""
        return AiosqliteCursor(connection)

    def handle_database_exceptions(self) -> "AiosqliteExceptionHandler":
        """Handle AIOSQLite-specific exceptions."""
        return AiosqliteExceptionHandler()

    async def _execute_script(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()

            # aiosqlite returns Iterable[Row], core helper expects Iterable[Any]
            # Use cast to satisfy mypy and pyright
            data, column_names, row_count = process_sqlite_result(cast("list[Any]", fetched_data), cursor.description)

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=row_count, is_select_result=True
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

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
            await self._truncate_table_async(table)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = build_sqlite_insert_statement(table, columns)
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

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.rollback()
        except aiosqlite.Error as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.commit()
        except aiosqlite.Error as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)

    async def _truncate_table_async(self, table: str) -> None:
        statement = f"DELETE FROM {format_sqlite_identifier(table)}"
        async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
            await cursor.execute(statement)

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase[Any]":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = cast("AsyncDataDictionaryBase[Any]", AiosqliteDataDictionary())
        return self._data_dictionary


_AIOSQLITE_PROFILE = build_aiosqlite_profile()

register_driver_profile("aiosqlite", _AIOSQLITE_PROFILE)
