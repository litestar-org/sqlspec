"""SQLite driver implementation."""

import contextlib
from typing import TYPE_CHECKING, Any, Literal, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.sqlite._typing import SqliteCursor, SqliteSessionContext
from sqlspec.adapters.sqlite._typing import sqlite_module as sqlite3
from sqlspec.adapters.sqlite.core import (
    SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT,
    SqliteStreamSource,
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_lastrowid,
    resolve_rowcount,
)
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters._registry import register_driver_profile
from sqlspec.core.result._base import ArrowResult, DMLResult
from sqlspec.driver._exception_handler import BaseSyncExceptionHandler
from sqlspec.driver._stream import SyncRowStream
from sqlspec.driver._sync import SyncDriverAdapterBase
from sqlspec.utils.type_guards import resolve_row_format

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.sqlite._typing import SqliteConnection
    from sqlspec.core.compiler import OperationType
    from sqlspec.core.result._base import SQLResult
    from sqlspec.core.statement import SQL, StatementConfig
    from sqlspec.driver._common import CachedQuery, ExecutionResult
    from sqlspec.storage.pipeline import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

__all__ = ("SqliteCursor", "SqliteDriver", "SqliteExceptionHandler", "SqliteSessionContext")


@mypyc_attr(allow_interpreted_subclasses=True)
class SqliteExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling SQLite database exceptions.

    Maps SQLite extended result codes to SQLSpec exceptions stored on
    ``pending_exception``.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, sqlite3.Error):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


@mypyc_attr(allow_interpreted_subclasses=True)
class SqliteDriver(SyncDriverAdapterBase):
    """SQLite driver implementation.

    Provides SQL statement execution, transaction management, and result handling
    for SQLite databases using the standard sqlite3 module.
    """

    __slots__ = ("_data_dictionary", "_rowid_target_cache")
    dialect: "DialectType | None" = "sqlite"

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
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SqliteDataDictionary | None = None
        self._rowid_target_cache: dict[tuple[str | None, str], bool] = {}

    @property
    def data_dictionary(self) -> "SqliteDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = SqliteDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        self._invalidate_rowid_target_cache(statement.operation_type)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            data, column_names, row_count = collect_rows(fetched_data, cursor.description)
            row_format = resolve_row_format(data)
            affected_rows = resolve_rowcount(cursor)
            effective_rowcount = affected_rows or (row_count if statement.operation_type == "INSERT" else 0)
            last_inserted_id = resolve_lastrowid(
                self.connection,
                cursor,
                statement.operation_type,
                effective_rowcount,
                statement._peek_expression(),
                self._rowid_target_cache,
            )

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=row_count,
                is_select_result=True,
                row_format=row_format,
                last_inserted_id=last_inserted_id,
            )

        affected_rows = resolve_rowcount(cursor)
        last_inserted_id = resolve_lastrowid(
            self.connection,
            cursor,
            statement.operation_type,
            affected_rows,
            statement._peek_expression(),
            self._rowid_target_cache,
        )
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_inserted_id)

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        self._invalidate_rowid_target_cache(statement.operation_type)
        cursor.executemany(sql, normalize_execute_many_parameters(prepared_parameters))
        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        self._rowid_target_cache.clear()
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0

        for stmt in statements:
            cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1

        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def begin(self, mode: "Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE'] | None" = None) -> None:
        """Begin a database transaction.

        Args:
            mode: Transaction lock mode (DEFERRED, IMMEDIATE, or EXCLUSIVE).
                Defaults to configured driver feature or SQLite default (DEFERRED).

        Raises:
            SQLSpecError: If transaction cannot be started
        """
        transaction_mode = mode if mode is not None else self.driver_features.get("default_transaction_mode")
        if transaction_mode is not None and transaction_mode not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
            msg = "Transaction mode must be DEFERRED, IMMEDIATE, or EXCLUSIVE"
            raise ValueError(msg)
        try:
            if not self.connection.in_transaction:
                stmt = f"BEGIN {transaction_mode}" if transaction_mode else "BEGIN"
                cursor = self.connection.execute(stmt)
                with contextlib.suppress(Exception):
                    cursor.close()
        except sqlite3.Error as e:
            raise create_mapped_exception(e) from e

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If transaction cannot be committed
        """
        try:
            if self._in_autocommit_mode():
                if self.connection.in_transaction:
                    cursor = self.connection.execute("COMMIT")
                    with contextlib.suppress(Exception):
                        cursor.close()
                return
            self.connection.commit()
        except sqlite3.Error as e:
            raise create_mapped_exception(e) from e

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If transaction cannot be rolled back
        """
        try:
            if self._in_autocommit_mode():
                if self.connection.in_transaction:
                    cursor = self.connection.execute("ROLLBACK")
                    with contextlib.suppress(Exception):
                        cursor.close()
                self._rowid_target_cache.clear()
                return
            self.connection.rollback()
            self._rowid_target_cache.clear()
        except sqlite3.Error as e:
            self._rowid_target_cache.clear()
            raise create_mapped_exception(e) from e

    def with_cursor(self, connection: "SqliteConnection") -> "SqliteCursor":
        """Create context manager for SQLite cursor.

        Args:
            connection: SQLite database connection

        Returns:
            Cursor context manager for safe cursor operations
        """
        return SqliteCursor(connection)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native SQLite row stream backed by chunked ``fetchmany``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(SqliteStreamSource(self, sql, prepared_parameters, chunk_size))

    def handle_database_exceptions(self) -> "SqliteExceptionHandler":
        """Return a context manager that maps SQLite errors to SQLSpec exceptions.

        Returns:
            Synchronous SQLite exception handler exposing mapped errors via ``pending_exception``.
        """
        return SqliteExceptionHandler()

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
        telemetry_payload = self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=sync_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
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
        """Load Arrow data into SQLite using chunked batched inserts."""
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)
        self._require_capability("arrow_import_enabled")
        if stage_table or conflict_keys or drop_indexes:
            return self._execute_staged_load_sync(
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

        owns_transaction = not self.connection.in_transaction
        try:
            if owns_transaction:
                transaction_mode = cast(
                    "Literal['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']",
                    self.driver_features.get("default_transaction_mode") or "IMMEDIATE",
                )
                self.begin(mode=transaction_mode)
            if overwrite:
                statement = f"DELETE FROM {format_identifier(table)}"
                with self.with_cursor(self.connection) as cursor:
                    cursor.execute(statement)
            for batch in arrow_table.to_batches(max_chunksize=batch_size):
                pydict = batch.to_pydict()
                records = list(zip(*[pydict[col] for col in columns], strict=False))
                if records:
                    prepared_records = (
                        self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                        if needs_prep
                        else records
                    )
                    with self.with_cursor(self.connection) as cursor:
                        cursor.executemany(insert_sql, cast("Any", prepared_records))
            if owns_transaction:
                self.commit()
        except BaseException as exc:
            if owns_transaction:
                self.rollback()
            if isinstance(exc, sqlite3.Error):
                raise create_mapped_exception(exc) from exc
            raise

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
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage into SQLite."""

        return super().load_from_storage(
            table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
        )

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect SQLite rows for the direct execution path."""
        return collect_rows(fetched, cursor.description)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from SQLite cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    def _in_autocommit_mode(self) -> bool:
        """Report whether the connection runs in sqlite3's autocommit mode.

        Under that mode ``Connection.commit`` and ``Connection.rollback`` are
        no-ops, so a manually started transaction has to be ended with an
        explicit statement.
        """
        return SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT and getattr(self.connection, "autocommit", None) is True

    def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute cached query through SQLite connection.execute fast path.

        This bypasses cursor context-manager overhead for repeated cached
        statements while preserving driver exception mapping behavior.
        """
        cursor = None
        returns_rows = cached.operation_profile.returns_rows
        self._invalidate_rowid_target_cache(cached.operation_type)
        try:
            try:
                cursor = self.connection.execute(cached.compiled_sql, params)
            except sqlite3.Error as exc:
                raise create_mapped_exception(exc) from exc

            if not returns_rows:
                rowcount = cursor.rowcount
                affected_rows = rowcount if isinstance(rowcount, int) and rowcount > 0 else 0
                last_inserted_id = resolve_lastrowid(
                    self.connection,
                    cursor,
                    cached.operation_type,
                    affected_rows,
                    cached.processed_state.parsed_expression,
                    self._rowid_target_cache,
                )
                return DMLResult(cached.operation_type, affected_rows, last_inserted_id)

            fetched_data = cursor.fetchall()
            affected_rows = resolve_rowcount(cursor)
            effective_rowcount = affected_rows or (len(fetched_data) if cached.operation_type == "INSERT" else 0)
            last_inserted_id = None
            if cached.operation_type != "SELECT":
                last_inserted_id = resolve_lastrowid(
                    self.connection,
                    cursor,
                    cached.operation_type,
                    effective_rowcount,
                    cached.processed_state.parsed_expression,
                    self._rowid_target_cache,
                )
            description = cursor.description
            column_names = [col[0] for col in description] if description else []
            row_format = resolve_row_format(fetched_data)
            return self.build_cached_select_result(
                sql,
                params,
                cached,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                row_format=row_format,
                last_inserted_id=last_inserted_id,
            )
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()

    def _invalidate_rowid_target_cache(self, operation_type: "OperationType") -> None:
        if operation_type not in {"SELECT", "INSERT", "UPDATE", "DELETE"}:
            self._rowid_target_cache.clear()

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)


register_driver_profile("sqlite", driver_profile)
