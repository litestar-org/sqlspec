"""mssql-python sync and async drivers."""

import asyncio
from typing import TYPE_CHECKING, Any, TypedDict, cast

from typing_extensions import NotRequired

from sqlspec.adapters.mssql_python._typing import (
    MSSQL_PYTHON_MODULE,
    MssqlPythonAsyncCursor,
    MssqlPythonAsyncSessionContext,
    MssqlPythonConnection,
    MssqlPythonCursor,
    MssqlPythonRawCursor,
    MssqlPythonSessionContext,
)
from sqlspec.adapters.mssql_python.core import create_mapped_exception, default_statement_config, driver_profile
from sqlspec.adapters.mssql_python.data_dictionary import MssqlPythonAsyncDataDictionary, MssqlPythonSyncDataDictionary
from sqlspec.core import (
    build_arrow_result_from_reader,
    build_arrow_result_from_table,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import (
    AsyncDriverAdapterBase,
    BaseAsyncExceptionHandler,
    BaseSyncExceptionHandler,
    SyncDriverAdapterBase,
)
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
    "MssqlPythonAsyncCursor",
    "MssqlPythonAsyncDriver",
    "MssqlPythonAsyncExceptionHandler",
    "MssqlPythonAsyncSessionContext",
    "MssqlPythonBulkCopyResult",
    "MssqlPythonCursor",
    "MssqlPythonDriver",
    "MssqlPythonExceptionHandler",
    "MssqlPythonSessionContext",
)

logger = get_logger("sqlspec.adapters.mssql_python")
_MSSQL_ERROR = cast("type[BaseException]", getattr(MSSQL_PYTHON_MODULE, "Error", Exception))


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


class MssqlPythonAsyncExceptionHandler(BaseAsyncExceptionHandler):
    """Async context manager handling mssql-python exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, _MSSQL_ERROR):
            self.pending_exception = create_mapped_exception(cast("Exception", exc_val), logger=logger)
            return True
        return False


class MssqlPythonDriver(SyncDriverAdapterBase):
    """mssql-python sync driver."""

    __slots__ = ("_data_dictionary",)
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

    @property
    def data_dictionary(self) -> "MssqlPythonSyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = MssqlPythonSyncDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        _execute_cursor(cursor, sql, prepared_parameters)

        if statement.returns_rows():
            fetched = cursor.fetchall()
            column_names = [desc[0] for desc in (cursor.description or [])]
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
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.executemany(sql, cast("Any", prepared_parameters))
        return self.create_execution_result(cursor, rowcount_override=_cursor_rowcount(cursor), is_many_result=True)

    def dispatch_execute_script(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        successful_count = 0
        for stmt in statements:
            _execute_cursor(cursor, stmt, prepared_parameters)
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def collect_rows(self, cursor: "MssqlPythonRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        column_names = [desc[0] for desc in (cursor.description or [])]
        return fetched, column_names, len(fetched)

    def resolve_rowcount(self, cursor: "MssqlPythonRawCursor") -> int:
        return _cursor_rowcount(cursor)

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        try:
            self.connection.commit()
        except _MSSQL_ERROR as exc:
            msg = f"Failed to commit transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except _MSSQL_ERROR as exc:
            msg = f"Failed to rollback transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: "MssqlPythonConnection") -> "MssqlPythonCursor":
        return MssqlPythonCursor(connection)

    def handle_database_exceptions(self) -> "MssqlPythonExceptionHandler":
        return MssqlPythonExceptionHandler()

    def create_savepoint(self, name: str) -> None:
        self.execute_script(f"SAVE TRANSACTION {name}")

    def release_savepoint(self, name: str) -> None:
        return None

    def rollback_to_savepoint(self, name: str) -> None:
        self.execute_script(f"ROLLBACK TRANSACTION {name}")

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
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)
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
        """Load staged artifacts from storage into SQL Server via BulkCopy."""
        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)


class MssqlPythonAsyncDriver(AsyncDriverAdapterBase):
    """Async wrapper around mssql-python's sync DB-API via asyncio.to_thread."""

    __slots__ = ("_data_dictionary",)
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
        self._data_dictionary: MssqlPythonAsyncDataDictionary | None = None

    @property
    def data_dictionary(self) -> "MssqlPythonAsyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = MssqlPythonAsyncDataDictionary()
        return self._data_dictionary

    async def dispatch_execute(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await asyncio.to_thread(_execute_cursor, cursor, sql, prepared_parameters)

        if statement.returns_rows():
            fetched = await asyncio.to_thread(cursor.fetchall)
            column_names = [desc[0] for desc in (cursor.description or [])]
            return self.create_execution_result(
                cursor,
                selected_data=fetched,
                column_names=column_names,
                data_row_count=len(fetched),
                is_select_result=True,
                row_format="tuple",
            )

        return self.create_execution_result(cursor, rowcount_override=_cursor_rowcount(cursor))

    async def dispatch_execute_many(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await asyncio.to_thread(cursor.executemany, sql, cast("Any", prepared_parameters))
        return self.create_execution_result(cursor, rowcount_override=_cursor_rowcount(cursor), is_many_result=True)

    async def dispatch_execute_script(self, cursor: "MssqlPythonRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        successful_count = 0
        for stmt in statements:
            await asyncio.to_thread(_execute_cursor, cursor, stmt, prepared_parameters)
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def collect_rows(self, cursor: "MssqlPythonRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        column_names = [desc[0] for desc in (cursor.description or [])]
        return fetched, column_names, len(fetched)

    def resolve_rowcount(self, cursor: "MssqlPythonRawCursor") -> int:
        return _cursor_rowcount(cursor)

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        try:
            await asyncio.to_thread(self.connection.commit)
        except _MSSQL_ERROR as exc:
            msg = f"Failed to commit transaction: {exc}"
            raise SQLSpecError(msg) from exc

    async def rollback(self) -> None:
        try:
            await asyncio.to_thread(self.connection.rollback)
        except _MSSQL_ERROR as exc:
            msg = f"Failed to rollback transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: "MssqlPythonConnection") -> "MssqlPythonAsyncCursor":
        return MssqlPythonAsyncCursor(connection)

    def handle_database_exceptions(self) -> "MssqlPythonAsyncExceptionHandler":
        return MssqlPythonAsyncExceptionHandler()

    async def create_savepoint(self, name: str) -> None:
        await self.execute_script(f"SAVE TRANSACTION {name}")

    async def release_savepoint(self, name: str) -> None:
        return None

    async def rollback_to_savepoint(self, name: str) -> None:
        await self.execute_script(f"ROLLBACK TRANSACTION {name}")

    async def select_to_arrow(
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
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)
        arrow_kwargs: dict[str, int] = {"batch_size": batch_size} if batch_size is not None else {}
        table: Any | None = None

        if return_format == "reader":
            cursor_manager = self.with_cursor(self.connection)
            cursor = None
            reader: object | None = None
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                cursor = await cursor_manager.__aenter__()
                try:
                    await asyncio.to_thread(_execute_cursor, cursor, sql, prepared_parameters)
                    arrow_reader = getattr(cursor, "arrow_reader", None)
                    if callable(arrow_reader):
                        reader = await asyncio.to_thread(arrow_reader, **arrow_kwargs)
                    if reader is None:
                        table = await asyncio.to_thread(cursor.arrow, **arrow_kwargs)
                        await cursor_manager.__aexit__(None, None, None)
                        cursor = None
                except Exception:
                    await cursor_manager.__aexit__(None, None, None)
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
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            await asyncio.to_thread(_execute_cursor, cursor, sql, prepared_parameters)
            if return_format == "batches":
                arrow_reader = getattr(cursor, "arrow_reader", None)
                reader = await asyncio.to_thread(arrow_reader, **arrow_kwargs) if callable(arrow_reader) else None
                if reader is not None:
                    return build_arrow_result_from_reader(
                        prepared_statement,
                        reader,
                        return_format=return_format,
                        batch_size=batch_size,
                        arrow_schema=arrow_schema,
                    )
            table = await asyncio.to_thread(cursor.arrow, **arrow_kwargs)
        self._check_pending_exception(exc_handler)

        if table is None:
            msg = "mssql-python did not return an Arrow table."
            raise SQLSpecError(msg)
        return build_arrow_result_from_table(
            prepared_statement, table, return_format=return_format, batch_size=batch_size, arrow_schema=arrow_schema
        )

    async def bulk_copy(
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
        exc_handler = self.handle_database_exceptions()
        result: MssqlPythonBulkCopyResult = {"rows_copied": 0}
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            raw_result = await asyncio.to_thread(
                cursor.bulkcopy,
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

    async def load_from_arrow(
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
            async with exc_handler, self.with_cursor(self.connection) as cursor:
                await asyncio.to_thread(_execute_cursor, cursor, f"DELETE FROM {_quote_mssql_table(table)}", None)
            self._check_pending_exception(exc_handler)
        if records:
            await self.bulk_copy(table, records, column_mappings=columns)
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
        """Load staged artifacts from storage into SQL Server via BulkCopy."""
        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )


def _execute_cursor(cursor: "MssqlPythonRawCursor", sql: str, parameters: Any) -> None:
    if parameters is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, parameters)


def _cursor_rowcount(cursor: "MssqlPythonRawCursor") -> int:
    rowcount = getattr(cursor, "rowcount", 0)
    return rowcount if isinstance(rowcount, int) and rowcount > 0 else 0


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
