"""mssql-python sync and async drivers."""

import contextlib
from typing import TYPE_CHECKING, Any, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.mssql_python._typing import (
    MSSQL_PYTHON_MODULE,
    MssqlPythonConnection,
    MssqlPythonCursor,
    MssqlPythonRawCursor,
    MssqlPythonSessionContext,
)
from sqlspec.adapters.mssql_python.core import (
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    materialize_tuple_rows,
)
from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonSyncDataDictionary
from sqlspec.core import (
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import BaseSyncExceptionHandler, SyncDriverAdapterBase, SyncRowStream, rows_to_dicts
from sqlspec.driver._common import validate_savepoint_name
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.arrow_helpers import arrow_reader_with_deferred_close
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow
from sqlspec.utils.text import quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, Statement, StatementConfig, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowRecordBatchReader, ArrowReturnFormat, StatementParameters


def _quote_mssql_table(table: str) -> str:
    return ".".join(quote_identifier(part) for part in split_qualified_identifier(table))


__all__ = (
    "MssqlPythonBulkCopyResult",
    "MssqlPythonCursor",
    "MssqlPythonDriver",
    "MssqlPythonExceptionHandler",
    "MssqlPythonSessionContext",
)

logger = get_logger("sqlspec.adapters.mssql_python")
_MSSQL_ERROR = cast("type[BaseException]", getattr(MSSQL_PYTHON_MODULE, "Error", Exception))
_COLUMN_CACHE_MAX_SIZE = 256


class MssqlPythonBulkCopyResult(TypedDict):
    """BulkCopy statistics returned by mssql-python."""

    rows_copied: int
    batch_count: NotRequired[int]
    elapsed_time: NotRequired[float]


class MssqlPythonExceptionHandler(BaseSyncExceptionHandler):
    """Sync context manager handling mssql-python exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, _MSSQL_ERROR):
            self.pending_exception = create_mapped_exception(cast("Exception", exc_val), logger=logger)
            return True
        return False


class MssqlPythonStreamSource:
    """Native mssql-python chunk source backed by ``cursor.fetchmany()``."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor_manager", "_driver", "_parameters", "_sql")

    def __init__(self, driver: "MssqlPythonDriver", sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor_manager: MssqlPythonCursor | None = None
        self._column_names: list[str] | None = None

    def start(self) -> None:
        cursor_manager = self._driver.with_cursor(self._driver.connection)
        try:
            cursor = cursor_manager.__enter__()
            handler = self._driver.handle_database_exceptions()
            with handler:
                _execute_cursor(cursor, self._sql, self._parameters)
            self._driver._check_pending_exception(handler)
        except BaseException:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)
            raise
        self._cursor_manager = cursor_manager

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        cursor_manager = self._cursor_manager
        if cursor_manager is None or cursor_manager.cursor is None:
            return []
        cursor = cursor_manager.cursor
        handler = self._driver.handle_database_exceptions()
        rows: Any = []
        with handler:
            rows = cursor.fetchmany(self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        column_names = self._column_names
        if column_names is None:
            column_names = _resolve_column_names(cursor.description, self._driver._column_name_cache)
            self._column_names = column_names
        return rows_to_dicts(rows, column_names)

    def close(self, error: bool = False) -> None:
        cursor_manager = self._cursor_manager
        self._cursor_manager = None
        if cursor_manager is not None:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)


class MssqlPythonDriver(SyncDriverAdapterBase):
    """mssql-python sync driver."""

    __slots__ = ("_column_name_cache", "_data_dictionary", "_transaction_active")
    dialect = "tsql"

    def __init__(
        self,
        connection: "MssqlPythonConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )
        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: MssqlPythonSyncDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._transaction_active = False

    @property
    def data_dictionary(self) -> "MssqlPythonSyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = MssqlPythonSyncDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        _execute_cursor(cursor, sql, prepared_parameters)

        if statement.returns_rows():
            fetched = materialize_tuple_rows(cursor.fetchall())
            column_names = _resolve_column_names(cursor.description, self._column_name_cache)
            return self.create_execution_result(
                cursor,
                selected_data=fetched,
                column_names=column_names,
                data_row_count=len(fetched),
                is_select_result=True,
                row_format="tuple",
            )

        return self.create_execution_result(cursor, rowcount_override=_cursor_rowcount(cursor))

    def dispatch_execute_many(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        cursor.executemany(sql, cast("Any", prepared_parameters))
        return self.create_execution_result(cursor, rowcount_override=_cursor_rowcount(cursor), is_many_result=True)

    def dispatch_execute_script(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        successful_count = 0
        for stmt in statements:
            _execute_cursor(cursor, stmt, prepared_parameters)
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def collect_rows(self, cursor: "MssqlPythonRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        column_names = _resolve_column_names(cursor.description, self._column_name_cache)
        return fetched, column_names, len(fetched)

    def resolve_rowcount(self, cursor: "MssqlPythonRawCursor") -> int:
        return _cursor_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        return self._transaction_active

    def begin(self) -> None:
        try:
            with self.with_cursor(self.connection) as cursor:
                cursor.execute("BEGIN TRANSACTION")
        except _MSSQL_ERROR as exc:
            msg = f"Failed to begin transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._transaction_active = True

    def commit(self) -> None:
        try:
            self.connection.commit()
        except _MSSQL_ERROR as exc:
            msg = f"Failed to commit transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._transaction_active = False

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except _MSSQL_ERROR as exc:
            msg = f"Failed to rollback transaction: {exc}"
            raise SQLSpecError(msg) from exc
        finally:
            self._transaction_active = False

    def with_cursor(self, connection: "MssqlPythonConnection") -> "MssqlPythonCursor":
        return MssqlPythonCursor(connection)

    def handle_database_exceptions(self) -> "MssqlPythonExceptionHandler":
        return MssqlPythonExceptionHandler()

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native mssql-python row stream backed by ``fetchmany()``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(MssqlPythonStreamSource(self, sql, prepared_parameters, chunk_size))

    def create_savepoint(self, name: str) -> None:
        self.execute_script(f"SAVE TRANSACTION {validate_savepoint_name(name)}")

    def release_savepoint(self, name: str) -> None:
        validate_savepoint_name(name)

    def rollback_to_savepoint(self, name: str) -> None:
        self.execute_script(f"ROLLBACK TRANSACTION {validate_savepoint_name(name)}")

    def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: "ArrowReturnFormat" = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute a query and return native mssql-python Arrow results."""
        ensure_pyarrow()
        config = statement_config or self.statement_config
        prepared_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        prepared_statement.compile()
        sql, prepared_parameters = self._compiled_sql(prepared_statement, config)
        arrow_kwargs: dict[str, int] = {"batch_size": batch_size} if batch_size is not None else {}
        table: Any | None = None

        if return_format == "reader":
            cursor_manager = self.with_cursor(self.connection)
            cursor = None
            reader: object | None = None
            exc_handler = self.handle_database_exceptions()
            with exc_handler:
                cursor = cursor_manager.__enter__()
                try:
                    _execute_cursor(cursor, sql, prepared_parameters)
                    reader = _cursor_arrow_reader(cursor, arrow_kwargs)
                    if reader is None:
                        table = cursor.arrow(**arrow_kwargs)
                        cursor_manager.__exit__(None, None, None)
                        cursor = None
                except Exception:
                    cursor_manager.__exit__(None, None, None)
                    cursor = None
                    raise
            self._check_pending_exception(exc_handler)
            if table is not None:
                return build_arrow_result_from_table(
                    prepared_statement,
                    table,
                    return_format=return_format,
                    batch_size=batch_size,
                    arrow_schema=arrow_schema,
                )
            if cursor is None or reader is None:
                msg = "mssql-python did not return an Arrow reader."
                raise SQLSpecError(msg)
            return build_arrow_result_from_reader(
                prepared_statement,
                arrow_reader_with_deferred_close(reader, cursor.close),
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )

        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection) as cursor:
            _execute_cursor(cursor, sql, prepared_parameters)
            if return_format == "batches":
                reader = _cursor_arrow_reader(cursor, arrow_kwargs)
                if reader is not None:
                    return build_arrow_result_from_reader(
                        prepared_statement,
                        reader,
                        return_format=return_format,
                        batch_size=batch_size,
                        arrow_schema=arrow_schema,
                    )
            table = cursor.arrow(**arrow_kwargs)
        self._check_pending_exception(exc_handler)

        if table is None:
            msg = "mssql-python did not return an Arrow table."
            raise SQLSpecError(msg)
        return build_arrow_result_from_table(
            prepared_statement, table, return_format=return_format, batch_size=batch_size, arrow_schema=arrow_schema
        )

    def bulk_copy(
        self,
        target_table: str,
        rows: "Iterable[tuple[Any, ...]]",
        *,
        batch_size: int = 0,
        timeout: int = 30,
        column_mappings: list[str] | list[tuple[int, str]] | None = None,
        keep_identity: bool = False,
        check_constraints: bool = False,
        table_lock: bool = False,
        keep_nulls: bool = False,
        fire_triggers: bool = False,
        use_internal_transaction: bool = False,
    ) -> MssqlPythonBulkCopyResult:
        """Bulk insert rows via mssql-python cursor.bulkcopy()."""
        result: MssqlPythonBulkCopyResult = {"rows_copied": 0}
        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection) as cursor:
            raw_result = cursor.bulkcopy(
                target_table,
                rows,
                batch_size=batch_size,
                timeout=timeout,
                column_mappings=column_mappings,
                keep_identity=keep_identity,
                check_constraints=check_constraints,
                table_lock=table_lock,
                keep_nulls=keep_nulls,
                fire_triggers=fire_triggers,
                use_internal_transaction=use_internal_transaction,
            )
            result = _coerce_bulk_copy_result(raw_result, cursor)
        self._check_pending_exception(exc_handler)
        return result

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into SQL Server via BulkCopy."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        columns, records = self._arrow_table_to_rows(arrow_table)
        if overwrite:
            exc_handler = self.handle_database_exceptions()
            with exc_handler, self.with_cursor(self.connection) as cursor:
                cursor.execute(f"DELETE FROM {_quote_mssql_table(table)}")
            self._check_pending_exception(exc_handler)
        if records:
            self.bulk_copy(table, records, column_mappings=columns)
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
    ) -> "StorageBridgeJob":
        """Load staged artifacts from storage into SQL Server via BulkCopy."""
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)


def _execute_cursor(cursor: "MssqlPythonRawCursor", sql: str, parameters: Any) -> None:
    if parameters is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, parameters)


def _cursor_rowcount(cursor: "MssqlPythonRawCursor") -> int:
    rowcount = getattr(cursor, "rowcount", 0)
    return rowcount if isinstance(rowcount, int) and rowcount > 0 else 0


def _resolve_column_names(description: Any, cache: "dict[int, tuple[Any, list[str]]]") -> list[str]:
    if not description:
        return []
    cache_key = id(description)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]
    column_names = [desc[0] for desc in description]
    if len(cache) >= _COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (description, column_names)
    return column_names


def _cursor_arrow_reader(
    cursor: "MssqlPythonRawCursor", arrow_kwargs: "dict[str, int]"
) -> "ArrowRecordBatchReader | None":
    arrow_reader = getattr(cursor, "arrow_reader", None)
    if not callable(arrow_reader):
        return None
    return cast("ArrowRecordBatchReader", arrow_reader(**arrow_kwargs))


def _coerce_bulk_copy_result(result: Any, cursor: "MssqlPythonRawCursor") -> MssqlPythonBulkCopyResult:
    if isinstance(result, dict):
        return cast("MssqlPythonBulkCopyResult", dict(result))
    return {"rows_copied": _cursor_rowcount(cursor)}


register_driver_profile("mssql_python", driver_profile, allow_override=True)
