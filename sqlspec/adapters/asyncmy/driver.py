"""AsyncMy MySQL driver implementation.

Provides MySQL/MariaDB connectivity with parameter style conversion,
type coercion, error handling, and transaction management.
"""

from typing import TYPE_CHECKING, Any, Final, cast

import asyncmy.errors  # pyright: ignore
from asyncmy.constants import FIELD_TYPE as ASYNC_MY_FIELD_TYPE  # pyright: ignore
from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore

from sqlspec.adapters.asyncmy.core import (
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
from sqlspec.adapters.asyncmy.data_dictionary import AsyncmyDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.type_guards import supports_json_type

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.adapters.asyncmy._typing import AsyncmyConnection
    from sqlspec.core import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.asyncmy._typing import AsyncmySessionContext

__all__ = ("AsyncmyCursor", "AsyncmyDriver", "AsyncmyExceptionHandler", "AsyncmySessionContext")

logger = get_logger(__name__)

json_type_value = (
    ASYNC_MY_FIELD_TYPE.JSON if ASYNC_MY_FIELD_TYPE is not None and supports_json_type(ASYNC_MY_FIELD_TYPE) else None
)
ASYNCMY_JSON_TYPE_CODES: Final[set[int]] = {json_type_value} if json_type_value is not None else set()


class AsyncmyCursor:
    """Context manager for AsyncMy cursor operations.

    Provides automatic cursor acquisition and cleanup for database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection
        self.cursor: Cursor | DictCursor | None = None

    async def __aenter__(self) -> Cursor | DictCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: Any) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class AsyncmyExceptionHandler:
    """Async context manager for handling asyncmy (MySQL) database exceptions.

    Maps MySQL error codes and SQLSTATE to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "AsyncmyExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, asyncmy.errors.Error):
            result = create_mapped_exception(exc_val, logger=logger)
            if result is True:
                return True
            self.pending_exception = cast("Exception", result)
            return True
        return False


class AsyncmyDriver(AsyncDriverAdapterBase):
    """MySQL/MariaDB database driver using AsyncMy client library.

    Implements asynchronous database operations for MySQL and MariaDB servers
    with support for parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "mysql"

    def __init__(
        self,
        connection: "AsyncmyConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncmyDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS - The Execution Engine
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Handles parameter processing, result fetching, and data transformation
        for MySQL/MariaDB operations.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult: Statement execution results with data or row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            fetched_rows = list(fetched_data) if fetched_data else None
            description = list(cursor.description) if cursor.description else None
            json_indexes = detect_json_columns(cursor, ASYNCMY_JSON_TYPE_CODES)
            deserializer = cast("Callable[[Any], Any]", self.driver_features.get("json_deserializer", from_json))
            rows, column_names = collect_rows(fetched_rows, description, json_indexes, deserializer, logger=logger)

            return self.create_execution_result(
                cursor, selected_data=rows, column_names=column_names, data_row_count=len(rows), is_select_result=True
            )

        affected_rows = resolve_rowcount(cursor)
        last_id = normalize_lastrowid(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_id)

    async def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL statement with multiple parameter sets.

        Uses AsyncMy's executemany for batch operations with MySQL type conversion
        and parameter processing.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult: Batch execution results

        Raises:
            ValueError: If no parameters provided for executemany operation
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        await cursor.executemany(sql, prepared_parameters)

        affected_rows = len(prepared_parameters)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Splits multi-statement scripts and executes each statement sequentially.
        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL script to execute

        Returns:
            ExecutionResult: Script execution results with statement count
        """
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
        """Begin a database transaction.

        Explicitly starts a MySQL transaction to ensure proper transaction boundaries.

        Raises:
            SQLSpecError: If transaction initialization fails
        """
        try:
            async with AsyncmyCursor(self.connection) as cursor:
                await cursor.execute("BEGIN")
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to begin MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If transaction commit fails
        """
        try:
            await self.connection.commit()
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to commit MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If transaction rollback fails
        """
        try:
            await self.connection.rollback()
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to rollback MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    def with_cursor(self, connection: "AsyncmyConnection") -> "AsyncmyCursor":
        """Create cursor context manager for the connection.

        Args:
            connection: AsyncMy database connection

        Returns:
            AsyncmyCursor: Context manager for cursor operations
        """
        return AsyncmyCursor(connection)

    def handle_database_exceptions(self) -> "AsyncmyExceptionHandler":
        """Provide exception handling context manager.

        Returns:
            AsyncmyExceptionHandler: Context manager for AsyncMy exception handling
        """
        return AsyncmyExceptionHandler()

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
        """Execute a query and stream Arrow-formatted results into storage."""

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
        """Load Arrow data into MySQL using batched inserts."""

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
        """Load staged artifacts from storage into MySQL."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "AsyncmyDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AsyncmyDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        AsyncMy uses explicit BEGIN and does not expose reliable transaction state.

        Returns:
            False - AsyncMy requires explicit transaction management.
        """
        return False


register_driver_profile("asyncmy", driver_profile)
