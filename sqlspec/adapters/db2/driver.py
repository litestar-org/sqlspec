"""IBM Db2 database driver implementation."""

import contextlib
import logging
from collections.abc import Sequence, Sized
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, Statement, StatementFilter
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

from sqlspec.adapters.db2._typing import Db2Error, Db2SyncCursor, Db2SyncSessionContext, connection_autocommit_enabled
from sqlspec.adapters.db2.core import (
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_rowcount,
)
from sqlspec.adapters.db2.data_dictionary import Db2SyncDataDictionary
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.driver import (
    BaseSyncExceptionHandler,
    ExecutionResult,
    SyncDriverAdapterBase,
    SyncRowStream,
    rows_to_dicts,
    validate_savepoint_name,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger, log_with_context

__all__ = ("Db2SyncCursor", "Db2SyncDriver", "Db2SyncExceptionHandler", "Db2SyncSessionContext")

logger = get_logger("sqlspec.adapters.db2")


class Db2SyncExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling IBM Db2 exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, Exception):
            self.pending_exception = create_mapped_exception(exc_val, logger=logger)
            return True
        return False


class Db2SyncStreamSource:
    """Native Db2 chunk source backed by cursor.fetchmany()."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor_manager", "_driver", "_parameters", "_sql")

    def __init__(self, driver: "Db2SyncDriver", sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor_manager: Db2SyncCursor | None = None
        self._column_names: list[str] | None = None

    def start(self) -> None:
        cursor_manager = self._driver.with_cursor(self._driver.connection)
        try:
            cursor = cursor_manager.__enter__()
            handler = self._driver.handle_database_exceptions()
            with handler:
                cursor.execute(self._sql, normalize_execute_parameters(self._parameters))
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
            column_names = resolve_column_names(
                cursor.description or None, self._driver._column_name_cache, lowercase=self._driver._lowercase_columns
            )
            self._column_names = column_names
        return rows_to_dicts(rows, column_names)

    def close(self, error: bool = False) -> None:
        cursor_manager = self._cursor_manager
        self._cursor_manager = None
        if cursor_manager is not None:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)


class Db2SyncDriver(SyncDriverAdapterBase):
    """IBM Db2 database driver."""

    __slots__ = (
        "_column_name_cache",
        "_data_dictionary",
        "_lowercase_columns",
        "_restore_autocommit",
        "_transaction_active",
    )
    dialect = "db2"

    def __init__(
        self,
        connection: Any,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: Db2SyncDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._lowercase_columns = bool(self.driver_features.get("enable_lowercase_column_names", True))
        self._transaction_active = False
        self._restore_autocommit = False

    def dispatch_execute(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            description = cursor.description or None
            rows, column_names, row_format = collect_rows(
                fetched_data, description, self._column_name_cache, lowercase=self._lowercase_columns
            )
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=column_names,
                data_row_count=len(rows),
                is_select_result=True,
                row_format=row_format,
            )

        return self.create_execution_result(cursor, rowcount_override=resolve_rowcount(cursor))

    def dispatch_execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        parameter_count = len(prepared_parameters) if isinstance(prepared_parameters, Sized) else 0
        cursor.executemany(sql, cast("Sequence[Any]", prepared_parameters))

        affected_rows = resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        for stmt in statements:
            cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def begin(self) -> None:
        """Begin a transaction by turning autocommit off for its duration.

        Does nothing while a transaction started by this driver is active. When the connection
        was in autocommit mode, ``commit()`` and ``rollback()`` switch it back on.

        Raises:
            SQLSpecError: When the driver reports an error.
        """
        if self._transaction_active:
            return
        try:
            enabled = connection_autocommit_enabled(self.connection)
            if enabled:
                self.connection.set_autocommit(False)
        except Db2Error as exc:
            msg = f"Failed to begin Db2 transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._restore_autocommit = enabled
        self._transaction_active = True

    def commit(self) -> None:
        """Commit the current unit of work and restore the autocommit baseline.

        Raises:
            SQLSpecError: When the driver reports an error.
        """
        try:
            self.connection.commit()
        except Db2Error as exc:
            msg = f"Failed to commit Db2 transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._transaction_active = False
        self._restore_connection_autocommit()

    def rollback(self) -> None:
        """Roll back the current unit of work and restore the autocommit baseline.

        Raises:
            SQLSpecError: When the driver reports an error.
        """
        try:
            self.connection.rollback()
        except Db2Error as exc:
            msg = f"Failed to rollback Db2 transaction: {exc}"
            raise SQLSpecError(msg) from exc
        self._transaction_active = False
        self._restore_connection_autocommit()

    def release_open_work(self, *, autocommit_baseline: bool) -> None:
        """Roll back work left open before the connection is returned to its pool.

        A transaction started by this driver is always rolled back; on a connection whose
        autocommit baseline is off, any pending unit of work is rolled back as well. A rollback
        failure is logged and not raised, so it never masks an error from the session body.

        Args:
            autocommit_baseline: Autocommit mode the connection was opened in.
        """
        if not self._transaction_active and autocommit_baseline:
            return
        try:
            self.rollback()
        except SQLSpecError as exc:
            log_with_context(logger, logging.DEBUG, "db2.session.rollback_failed", error=str(exc))

    def _restore_connection_autocommit(self) -> None:
        """Switch autocommit back on when ``begin()`` turned it off.

        Raises:
            SQLSpecError: When the driver reports an error.
        """
        restore_autocommit = self._restore_autocommit
        self._restore_autocommit = False
        if not restore_autocommit:
            return
        try:
            self.connection.set_autocommit(True)
        except Db2Error as exc:
            msg = f"Failed to restore Db2 autocommit: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: Any) -> "Db2SyncCursor":
        return Db2SyncCursor(connection)

    def handle_database_exceptions(self) -> "Db2SyncExceptionHandler":
        return Db2SyncExceptionHandler()

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native Db2 row stream backed by cursor.fetchmany()."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(Db2SyncStreamSource(self, sql, prepared_parameters, chunk_size))

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
        """Execute query and return results converted to Apache Arrow format.

        Db2 driver utilizes in-memory conversion since ibm_db lacks native Arrow C export.
        """
        return super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=return_format,
            native_only=native_only,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

    def create_savepoint(self, name: str) -> None:
        """Create a transaction savepoint retaining open cursors."""
        self.execute_script(f"SAVEPOINT {validate_savepoint_name(name)} ON ROLLBACK RETAIN CURSORS")

    def release_savepoint(self, name: str) -> None:
        """Release a transaction savepoint."""
        self.execute_script(f"RELEASE SAVEPOINT {validate_savepoint_name(name)}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a named transaction savepoint."""
        self.execute_script(f"ROLLBACK TO SAVEPOINT {validate_savepoint_name(name)}")

    @property
    def data_dictionary(self) -> "Db2SyncDataDictionary":
        """Return the Db2 data dictionary bound to this driver.

        Returns:
            The lazily created data dictionary instance.
        """
        if self._data_dictionary is None:
            self._data_dictionary = Db2SyncDataDictionary()
        return self._data_dictionary

    def collect_rows(self, cursor: Any, fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        column_names = resolve_column_names(
            cursor.description or None, self._column_name_cache, lowercase=self._lowercase_columns
        )
        return fetched, column_names, len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        return resolve_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        """Return whether a transaction opened by this driver remains active."""
        return self._transaction_active


register_driver_profile("db2", driver_profile)
