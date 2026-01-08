"""SQLite driver implementation."""

import contextlib
import sqlite3
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.sqlite._typing import SqliteSessionContext
from sqlspec.adapters.sqlite.core import (
    build_sqlite_insert_statement,
    build_sqlite_profile,
    format_sqlite_identifier,
    normalize_sqlite_rowcount,
    process_sqlite_result,
    raise_sqlite_exception,
    sqlite_statement_config,
)
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.core import ArrowResult, get_cache_config, register_driver_profile
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite._typing import SqliteConnection
    from sqlspec.core import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("SqliteCursor", "SqliteDriver", "SqliteExceptionHandler", "SqliteSessionContext", "sqlite_statement_config")


class SqliteCursor:
    """Context manager for SQLite cursor management.

    Provides automatic cursor creation and cleanup for SQLite database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "SqliteConnection") -> None:
        """Initialize cursor manager.

        Args:
            connection: SQLite database connection
        """
        self.connection = connection
        self.cursor: sqlite3.Cursor | None = None

    def __enter__(self) -> "sqlite3.Cursor":
        """Create and return a new cursor.

        Returns:
            Active SQLite cursor object
        """
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        """Clean up cursor resources.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class SqliteExceptionHandler:
    """Context manager for handling SQLite database exceptions.

    Maps SQLite extended result codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "SqliteExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, sqlite3.Error):
            try:
                raise_sqlite_exception(exc_val)
            except Exception as mapped:
                self.pending_exception = mapped
                return True
        return False


class SqliteDriver(SyncDriverAdapterBase):
    """SQLite driver implementation.

    Provides SQL statement execution, transaction management, and result handling
    for SQLite databases using the standard sqlite3 module.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "sqlite"

    def __init__(
        self,
        connection: "SqliteConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        """Initialize SQLite driver.

        Args:
            connection: SQLite database connection
            statement_config: Statement configuration settings
            driver_features: Driver-specific feature flags
        """
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = sqlite_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="sqlite",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SqliteDataDictionary | None = None

    def with_cursor(self, connection: "SqliteConnection") -> "SqliteCursor":
        """Create context manager for SQLite cursor.

        Args:
            connection: SQLite database connection

        Returns:
            Cursor context manager for safe cursor operations
        """
        return SqliteCursor(connection)

    def handle_database_exceptions(self) -> "SqliteExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler with deferred exception pattern for mypyc compatibility.
        """
        return SqliteExceptionHandler()

    def _execute_script(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        cursor.executemany(sql, prepared_parameters)

        affected_rows = normalize_sqlite_rowcount(cursor)

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_statement(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            data, column_names, row_count = process_sqlite_result(fetched_data, cursor.description)

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=row_count, is_select_result=True
            )

        affected_rows = normalize_sqlite_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

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
        """Execute a query and write Arrow-compatible output to storage (sync)."""

        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline = self._storage_pipeline()
        telemetry_payload = self._write_result_to_storage_sync(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
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
        """Load Arrow data into SQLite using batched inserts."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = f"DELETE FROM {format_sqlite_identifier(table)}"
            with self.handle_database_exceptions(), self.with_cursor(self.connection) as cursor:
                cursor.execute(statement)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = build_sqlite_insert_statement(table, columns)
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
        """Load staged artifacts from storage into SQLite."""

        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def begin(self) -> None:
        """Begin a database transaction.

        Raises:
            SQLSpecError: If transaction cannot be started
        """
        try:
            if not self.connection.in_transaction:
                self.connection.execute("BEGIN")
        except sqlite3.Error as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If transaction cannot be rolled back
        """
        try:
            self.connection.rollback()
        except sqlite3.Error as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If transaction cannot be committed
        """
        try:
            self.connection.commit()
        except sqlite3.Error as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)

    @property
    def data_dictionary(self) -> "SqliteDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = SqliteDataDictionary()
        return self._data_dictionary


_SQLITE_PROFILE = build_sqlite_profile()

register_driver_profile("sqlite", _SQLITE_PROFILE)
