"""AIOSQLite driver implementation for async SQLite operations."""

import asyncio
import contextlib
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import aiosqlite

from sqlspec.core import (
    ArrowResult,
    DriverParameterProfile,
    ParameterStyle,
    build_statement_config_from_profile,
    get_cache_config,
    register_driver_profile,
)
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
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_time_iso_converter
from sqlspec.utils.type_guards import has_sqlite_error

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.aiosqlite._types import AiosqliteConnection
    from sqlspec.core import SQL, SQLResult, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._async import AsyncDataDictionaryBase
    from sqlspec.storage import (
        AsyncStoragePipeline,
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
    )

__all__ = ("AiosqliteCursor", "AiosqliteDriver", "AiosqliteExceptionHandler", "aiosqlite_statement_config")

SQLITE_CONSTRAINT_UNIQUE_CODE = 2067
SQLITE_CONSTRAINT_FOREIGNKEY_CODE = 787
SQLITE_CONSTRAINT_NOTNULL_CODE = 1811
SQLITE_CONSTRAINT_CHECK_CODE = 531
SQLITE_CONSTRAINT_CODE = 19
SQLITE_CANTOPEN_CODE = 14
SQLITE_IOERR_CODE = 10
SQLITE_MISMATCH_CODE = 20
_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


class AiosqliteCursor:
    """Async context manager for AIOSQLite cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AiosqliteConnection") -> None:
        self.connection = connection
        self.cursor: aiosqlite.Cursor | None = None

    async def __aenter__(self) -> "aiosqlite.Cursor":
        self.cursor = await self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: Any) -> None:
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                await self.cursor.close()


class AiosqliteExceptionHandler:
    """Async context manager for handling aiosqlite database exceptions.

    Maps SQLite extended result codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return
        if issubclass(exc_type, aiosqlite.Error):
            self._map_sqlite_exception(exc_val)

    def _map_sqlite_exception(self, e: Any) -> None:
        """Map SQLite exception to SQLSpec exception.

        Args:
            e: aiosqlite.Error instance

        Raises:
            Specific SQLSpec exception based on error code
        """
        if has_sqlite_error(e):
            error_code = e.sqlite_errorcode
            error_name = e.sqlite_errorname
        else:
            error_code = None
            error_name = None
        error_msg = str(e).lower()

        if "locked" in error_msg:
            msg = f"AIOSQLite database locked: {e}. Consider enabling WAL mode or reducing concurrency."
            raise SQLSpecError(msg) from e

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
        self._data_dictionary: AsyncDataDictionaryBase | None = None

    def with_cursor(self, connection: "AiosqliteConnection") -> "AiosqliteCursor":
        """Create async context manager for AIOSQLite cursor."""
        return AiosqliteCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle AIOSQLite-specific exceptions."""
        return AiosqliteExceptionHandler()

    async def _try_special_handling(self, cursor: "aiosqlite.Cursor", statement: "SQL") -> "SQLResult | None":
        """Hook for AIOSQLite-specific special operations.

        Args:
            cursor: AIOSQLite cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for AIOSQLite
        """
        _ = (cursor, statement)
        return None

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
            column_names = [col[0] for col in cursor.description or []]

            data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
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
        partitioner: "dict[str, Any] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and stream Arrow results into storage."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline: AsyncStoragePipeline = cast("AsyncStoragePipeline", self._storage_pipeline())
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
        partitioner: "dict[str, Any] | None" = None,
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
            insert_sql = _build_sqlite_insert_statement(table, columns)
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
        partitioner: "dict[str, Any] | None" = None,
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
            import random

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

    async def _truncate_table_async(self, table: str) -> None:
        statement = f"DELETE FROM {_format_sqlite_identifier(table)}"
        async with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
            await cursor.execute(statement)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteAsyncDataDictionary

            self._data_dictionary = AiosqliteAsyncDataDictionary()
        return self._data_dictionary


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_sqlite_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def _format_sqlite_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = [part for part in cleaned.split(".") if part]
    formatted = ".".join(_quote_sqlite_identifier(part) for part in parts)
    return formatted or _quote_sqlite_identifier(cleaned)


def _build_sqlite_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_sqlite_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {_format_sqlite_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def _build_aiosqlite_profile() -> DriverParameterProfile:
    """Create the AIOSQLite driver parameter profile."""

    return DriverParameterProfile(
        name="AIOSQLite",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            bool: _bool_to_int,
            datetime: _TIME_TO_ISO,
            date: _TIME_TO_ISO,
            Decimal: _DECIMAL_TO_STRING,
        },
        default_dialect="sqlite",
    )


_AIOSQLITE_PROFILE = _build_aiosqlite_profile()

register_driver_profile("aiosqlite", _AIOSQLITE_PROFILE)

aiosqlite_statement_config = build_statement_config_from_profile(
    _AIOSQLITE_PROFILE, statement_overrides={"dialect": "sqlite"}, json_serializer=to_json
)
