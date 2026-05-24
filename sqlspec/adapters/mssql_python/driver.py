"""mssql-python sync and async drivers."""

import asyncio
from typing import TYPE_CHECKING, Any, cast

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
from sqlspec.core import build_arrow_result_from_table, get_cache_config, register_driver_profile
from sqlspec.driver import (
    AsyncDriverAdapterBase,
    BaseAsyncExceptionHandler,
    BaseSyncExceptionHandler,
    SyncDriverAdapterBase,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_pyarrow

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlspec.builder import QueryBuilder
    from sqlspec.core import SQL, ArrowResult, Statement, StatementConfig, StatementFilter
    from sqlspec.driver import ExecutionResult
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = (
    "MssqlPythonAsyncCursor",
    "MssqlPythonAsyncDriver",
    "MssqlPythonAsyncExceptionHandler",
    "MssqlPythonAsyncSessionContext",
    "MssqlPythonCursor",
    "MssqlPythonDriver",
    "MssqlPythonExceptionHandler",
    "MssqlPythonSessionContext",
)

logger = get_logger("sqlspec.adapters.mssql_python")
_MSSQL_ERROR = cast("type[BaseException]", getattr(MSSQL_PYTHON_MODULE, "Error", Exception))


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
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)
        arrow_kwargs = {"batch_size": batch_size} if batch_size is not None else {}
        table: Any | None = None

        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection) as cursor:
            _execute_cursor(cursor, sql, prepared_parameters)
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
        batch_size: int = 64_000,
        timeout: int = 3600,
        column_mappings: list[str] | list[tuple[int, str]] | None = None,
        keep_identity: bool = False,
        check_constraints: bool = True,
        table_lock: bool = False,
        keep_nulls: bool = False,
        fire_triggers: bool = False,
        use_internal_transaction: bool = False,
    ) -> int:
        """Bulk insert rows via mssql-python cursor.bulkcopy()."""
        rowcount = 0
        exc_handler = self.handle_database_exceptions()
        with exc_handler, self.with_cursor(self.connection) as cursor:
            cursor.bulkcopy(
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
            rowcount = _cursor_rowcount(cursor)
        self._check_pending_exception(exc_handler)
        return int(rowcount)


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
        sql, prepared_parameters = self._get_compiled_sql(prepared_statement, config)
        arrow_kwargs = {"batch_size": batch_size} if batch_size is not None else {}
        table: Any | None = None
        exc_handler = self.handle_database_exceptions()
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            await asyncio.to_thread(_execute_cursor, cursor, sql, prepared_parameters)
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
        batch_size: int = 64_000,
        timeout: int = 3600,
        column_mappings: list[str] | list[tuple[int, str]] | None = None,
        keep_identity: bool = False,
        check_constraints: bool = True,
        table_lock: bool = False,
        keep_nulls: bool = False,
        fire_triggers: bool = False,
        use_internal_transaction: bool = False,
    ) -> int:
        """Bulk insert rows via mssql-python cursor.bulkcopy()."""
        exc_handler = self.handle_database_exceptions()
        rowcount = 0
        async with exc_handler, self.with_cursor(self.connection) as cursor:
            await asyncio.to_thread(
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
            rowcount = _cursor_rowcount(cursor)
        self._check_pending_exception(exc_handler)
        return rowcount


def _execute_cursor(cursor: "MssqlPythonRawCursor", sql: str, parameters: Any) -> None:
    if parameters is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, parameters)


def _cursor_rowcount(cursor: "MssqlPythonRawCursor") -> int:
    rowcount = getattr(cursor, "rowcount", 0)
    return rowcount if isinstance(rowcount, int) and rowcount > 0 else 0


register_driver_profile("mssql_python", driver_profile, allow_override=True)
