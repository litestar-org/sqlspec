"""SQLite driver implementation."""

import sqlite3
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.sqlite._typing import SqliteCursor, SqliteSessionContext
from sqlspec.adapters.sqlite.core import (
    SqliteStreamSource,
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    require_python_version,
    resolve_rowcount,
)
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary
from sqlspec.core import ArrowResult, ParameterStyle, TypedParameter, get_cache_config, register_driver_profile
from sqlspec.core.result import DMLResult
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from os import PathLike

    from sqlspec.adapters.sqlite._typing import SqliteConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, SQLResult, Statement, StatementConfig, StatementFilter
    from sqlspec.core.compiler import OperationType
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._query_cache import CachedQuery
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import StatementParameters

__all__ = ("SqliteCursor", "SqliteDriver", "SqliteExceptionHandler", "SqliteSessionContext")

_WAL_CHECKPOINT_MODES = frozenset({"FULL", "PASSIVE", "RESTART", "TRUNCATE"})


class SqliteExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling SQLite database exceptions.

    Maps SQLite extended result codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if issubclass(exc_type, sqlite3.Error):
            self.pending_exception = create_mapped_exception(exc_val)
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
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SqliteDataDictionary | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            data, column_names, row_count = collect_rows(fetched_data, cursor.description)

            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=row_count,
                is_select_result=True,
                row_format="tuple",
            )

        affected_rows = resolve_rowcount(cursor)
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
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

    def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute many with a SQLite thin path for simple qmark batches."""
        config = statement_config or self.statement_config
        if (
            isinstance(statement, str)
            and not filters
            and not kwargs
            and config is self.statement_config
            and self.observability.is_idle
            and self._can_use_execute_many_thin_path(statement, parameters, config)
        ):
            try:
                cursor = self.connection.executemany(statement, parameters)
            except sqlite3.Error as exc:
                raise create_mapped_exception(exc) from exc

            rowcount = cursor.rowcount
            affected_rows = rowcount if isinstance(rowcount, int) and rowcount > 0 else 0
            operation = self._resolve_dml_operation_type(statement)
            return DMLResult(operation, affected_rows)
        return super().execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)

    def _stmt_cache_execute_direct(
        self, sql: str, params: "tuple[Any, ...] | list[Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute cached query through SQLite connection.execute fast path.

        This bypasses cursor context-manager overhead for repeated cached
        statements while preserving driver exception mapping behavior.
        """
        direct_statement: SQL | None = None
        returns_rows = cached.operation_profile.returns_rows
        try:
            if not returns_rows:
                try:
                    cursor = self.connection.execute(cached.compiled_sql, params)
                except sqlite3.Error as exc:
                    raise create_mapped_exception(exc) from exc

                rowcount = cursor.rowcount
                affected_rows = rowcount if isinstance(rowcount, int) and rowcount > 0 else 0
                return DMLResult(cached.operation_type, affected_rows)

            try:
                cursor = self.connection.execute(cached.compiled_sql, params)
            except sqlite3.Error as exc:
                raise create_mapped_exception(exc) from exc

            fetched_data = cursor.fetchall()
            column_names = cached.column_names
            if column_names is None:
                description = cursor.description
                column_names = [col[0] for col in description] if description else []
            execution_result = self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
                row_format="tuple",
            )
            direct_statement = self._stmt_cache_build_direct(
                sql, params, cached, params, params_are_simple=True, compiled_sql=cached.compiled_sql
            )
            return self.build_statement_result(direct_statement, execution_result)
        finally:
            if direct_statement is not None:
                self._release_pooled_statement(direct_statement)
        msg = "unreachable"
        raise AssertionError(msg)  # pragma: no cover

    def _can_use_execute_many_thin_path(
        self, statement: str, parameters: "Sequence[StatementParameters]", config: "StatementConfig"
    ) -> bool:
        if type(parameters) is not list:
            return False
        if not parameters:
            return False
        if "?" not in statement:
            return False

        parameter_config = config.parameter_config
        if parameter_config.default_parameter_style is not ParameterStyle.QMARK:
            return False
        if (
            parameter_config.default_execution_parameter_style is not None
            and parameter_config.default_execution_parameter_style is not ParameterStyle.QMARK
        ):
            return False
        if parameter_config.ast_transformer is not None or parameter_config.output_transformer is not None:
            return False
        if parameter_config.needs_static_script_compilation:
            return False
        if config.output_transformer is not None or config.statement_transformers:
            return False

        return self._thin_path_parameters_are_eligible(parameters, parameter_config.type_coercion_map)

    @staticmethod
    def _thin_path_parameters_are_eligible(
        parameters: "list[StatementParameters]", type_coercion_map: "dict[type, Any] | None"
    ) -> bool:
        """Validate parameter payload for the SQLite execute-many thin path."""
        first_sequence = SqliteDriver._as_sequence_parameter_set(parameters[0])
        if first_sequence is None:
            return False

        first_type = type(first_sequence)
        row_len = len(first_sequence)
        coercion_map = type_coercion_map
        has_type_coercion = bool(coercion_map)

        # Common benchmark shape: list[tuple[value]]
        if row_len == 1:
            if has_type_coercion and coercion_map is not None:
                for param_set in parameters:
                    sequence = SqliteDriver._as_sequence_parameter_set(param_set)
                    if sequence is None or type(sequence) is not first_type:
                        return False
                    if len(sequence) != 1:
                        return False
                    value_type = type(sequence[0])
                    if value_type is TypedParameter or value_type in coercion_map:
                        return False
                return True

            for param_set in parameters:
                sequence = SqliteDriver._as_sequence_parameter_set(param_set)
                if sequence is None or type(sequence) is not first_type:
                    return False
                if len(sequence) != 1:
                    return False
                if type(sequence[0]) is TypedParameter:
                    return False
            return True

        if has_type_coercion and coercion_map is not None:
            for param_set in parameters:
                sequence = SqliteDriver._as_sequence_parameter_set(param_set)
                if sequence is None or type(sequence) is not first_type:
                    return False
                if len(sequence) != row_len:
                    return False
                for value in sequence:
                    value_type = type(value)
                    if value_type is TypedParameter or value_type in coercion_map:
                        return False
            return True

        for param_set in parameters:
            sequence = SqliteDriver._as_sequence_parameter_set(param_set)
            if sequence is None or type(sequence) is not first_type:
                return False
            if len(sequence) != row_len:
                return False
            for value in sequence:
                if type(value) is TypedParameter:
                    return False
        return True

    @staticmethod
    def _as_sequence_parameter_set(param_set: "StatementParameters") -> "list[Any] | tuple[Any, ...] | None":
        if isinstance(param_set, list):
            return param_set
        if isinstance(param_set, tuple):
            return param_set
        return None

    @staticmethod
    def _resolve_dml_operation_type(statement: str) -> "OperationType":
        command_keyword = statement.lstrip().split(None, 1)[0].upper() if statement.strip() else "COMMAND"
        if command_keyword == "INSERT":
            return "INSERT"
        if command_keyword == "UPDATE":
            return "UPDATE"
        if command_keyword == "DELETE":
            return "DELETE"
        return "COMMAND"

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

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
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        return SyncRowStream(SqliteStreamSource(self, sql, prepared_parameters, chunk_size))

    def handle_database_exceptions(self) -> "SqliteExceptionHandler":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Exception handler with deferred exception pattern for mypyc compatibility.
        """
        return SqliteExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # MAINTENANCE OPERATIONS
    # ─────────────────────────────────────────────────────────────────────────────

    def backup(
        self,
        target: "SqliteConnection | str | PathLike[str]",
        *,
        pages: int = 0,
        progress: "Callable[[int, int, int], None] | None" = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        """Copy the database into another connection or file path."""
        if isinstance(target, sqlite3.Connection):
            self.connection.backup(target, pages=pages, progress=progress, name=name, sleep=sleep)
            return
        target_connection = sqlite3.connect(str(target))
        try:
            self.connection.backup(target_connection, pages=pages, progress=progress, name=name, sleep=sleep)
        finally:
            target_connection.close()

    def iterdump(self, *, filter_pattern: "str | None" = None) -> "Iterator[str]":
        """Yield SQL statements that recreate the database.

        filter_pattern filters dumped objects by LIKE pattern and requires Python 3.13+.
        """
        if filter_pattern is None:
            return self.connection.iterdump()
        require_python_version("sqlite3.Connection.iterdump(filter=...)", (3, 13))
        return self.connection.iterdump(filter=filter_pattern)

    def serialize(self, *, name: str = "main") -> bytes:
        """Return the named database as bytes. Requires Python 3.11+."""
        require_python_version("sqlite3.Connection.serialize()", (3, 11))
        return self.connection.serialize(name=name)

    def deserialize(self, data: bytes, *, name: str = "main") -> None:
        """Replace the named database with serialized bytes. Requires Python 3.11+."""
        require_python_version("sqlite3.Connection.deserialize()", (3, 11))
        self.connection.deserialize(data, name=name)

    def blob_open(self, table: str, column: str, rowid: int, *, readonly: bool = False, name: str = "main") -> Any:
        """Open incremental blob I/O on an existing BLOB cell. Requires Python 3.11+."""
        require_python_version("sqlite3.Connection.blobopen()", (3, 11))
        return self.connection.blobopen(table, column, rowid, readonly=readonly, name=name)

    def optimize(self) -> None:
        """Run PRAGMA optimize."""
        self.connection.execute("PRAGMA optimize")

    def wal_checkpoint(
        self, mode: Literal["PASSIVE", "FULL", "RESTART", "TRUNCATE"] = "PASSIVE"
    ) -> tuple[int, int, int]:
        """Run a WAL checkpoint and return (busy, log_pages, checkpointed_pages).

        TRUNCATE and RESTART block writers while the checkpoint runs.
        """
        if mode not in _WAL_CHECKPOINT_MODES:
            msg = f"Invalid WAL checkpoint mode {mode!r}; expected one of {sorted(_WAL_CHECKPOINT_MODES)}."
            raise SQLSpecError(msg)
        cursor = self.connection.execute(f"PRAGMA wal_checkpoint({mode})")
        row = cursor.fetchone()
        return (int(row[0]), int(row[1]), int(row[2]))

    def integrity_check(self, *, max_errors: int = 100) -> list[str]:
        """Run PRAGMA integrity_check and return reported messages (['ok'] when clean)."""
        cursor = self.connection.execute(f"PRAGMA integrity_check({int(max_errors)})")
        return [str(row[0]) for row in cursor.fetchall()]

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API
    # ─────────────────────────────────────────────────────────────────────────────

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
            statement = f"DELETE FROM {format_identifier(table)}"
            try:
                with self.with_cursor(self.connection) as cursor:
                    cursor.execute(statement)
            except sqlite3.Error as exc:
                raise create_mapped_exception(exc) from exc

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = build_insert_statement(table, columns)
            prepared_records = (
                self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                if self._arrow_table_needs_parameter_preparation(arrow_table)
                else records
            )
            try:
                with self.with_cursor(self.connection) as cursor:
                    cursor.executemany(insert_sql, cast("Any", prepared_records))
            except sqlite3.Error as exc:
                raise create_mapped_exception(exc) from exc

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

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "SqliteDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = SqliteDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect SQLite rows for the direct execution path."""
        return collect_rows(fetched, cursor.description)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve rowcount from SQLite cursor for the direct execution path."""
        return resolve_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction.

        Returns:
            True if connection is in an active transaction.
        """
        return bool(self.connection.in_transaction)


register_driver_profile("sqlite", driver_profile)
