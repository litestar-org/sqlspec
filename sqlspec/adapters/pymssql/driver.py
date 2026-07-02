"""pymssql SQL Server driver implementation."""

from collections.abc import Sized
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.pymssql._typing import (
    PYMSSQL_MODULE,
    PymssqlConnection,
    PymssqlCursor,
    PymssqlRawCursor,
    PymssqlSessionContext,
)
from sqlspec.adapters.pymssql.core import (
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
from sqlspec.adapters.pymssql.data_dictionary import PymssqlSyncDataDictionary
from sqlspec.core import SQL, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.driver import BaseSyncExceptionHandler, ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pymssql._pymssql import QueryParams

__all__ = ("PymssqlCursor", "PymssqlDriver", "PymssqlExceptionHandler", "PymssqlSessionContext")

logger = get_logger("sqlspec.adapters.pymssql")
pymssql = PYMSSQL_MODULE


class _UnavailablePymssqlError(Exception):
    """Fallback pymssql exception base when pymssql is unavailable."""


class PymssqlExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling pymssql exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        error_type = _pymssql_error_type()
        if isinstance(exc_val, error_type):
            self.pending_exception = create_mapped_exception(cast("Exception", exc_val), logger=logger)
            return True
        return False


class PymssqlDriver(SyncDriverAdapterBase):
    """SQL Server database driver using pymssql."""

    __slots__ = ("_data_dictionary",)
    dialect = "tsql"

    def __init__(
        self,
        connection: "PymssqlConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PymssqlSyncDataDictionary | None = None

    def dispatch_execute(self, cursor: "PymssqlRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            description = cursor.description or None
            rows, column_names, row_format = collect_rows(fetched_data, description)
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=column_names,
                data_row_count=len(rows),
                is_select_result=True,
                row_format=row_format,
            )

        return self.create_execution_result(cursor, rowcount_override=resolve_rowcount(cursor))

    def dispatch_execute_many(self, cursor: "PymssqlRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        parameter_count = len(prepared_parameters) if isinstance(prepared_parameters, Sized) else None
        cursor.executemany(sql, cast("Sequence[QueryParams]", prepared_parameters))

        affected_rows = resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: "PymssqlRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        if prepared_parameters and len(statements) > 1:
            msg = "execute_script with parameters is not supported for multi-statement scripts; use execute or execute_many for parameterized statements"
            raise SQLSpecError(msg)

        successful_count = 0
        for stmt in statements:
            cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def begin(self) -> None:
        try:
            with PymssqlCursor(self.connection) as cursor:
                cursor.execute("BEGIN TRANSACTION")
        except _pymssql_error_type() as exc:
            msg = f"Failed to begin SQL Server transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def commit(self) -> None:
        try:
            self.connection.commit()
        except _pymssql_error_type() as exc:
            msg = f"Failed to commit SQL Server transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def rollback(self) -> None:
        try:
            self.connection.rollback()
        except _pymssql_error_type() as exc:
            msg = f"Failed to rollback SQL Server transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: "PymssqlConnection") -> "PymssqlCursor":
        return PymssqlCursor(connection)

    def handle_database_exceptions(self) -> "PymssqlExceptionHandler":
        return PymssqlExceptionHandler()

    def create_savepoint(self, name: str) -> None:
        self.execute_script(f"SAVE TRANSACTION {name}")

    def release_savepoint(self, name: str) -> None:
        return None

    def rollback_to_savepoint(self, name: str) -> None:
        self.execute_script(f"ROLLBACK TRANSACTION {name}")

    @property
    def data_dictionary(self) -> "PymssqlSyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = PymssqlSyncDataDictionary()
        return self._data_dictionary

    def collect_rows(self, cursor: "PymssqlRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        column_names = resolve_column_names(cursor.description or None)
        return fetched, column_names, len(fetched)

    def resolve_rowcount(self, cursor: "PymssqlRawCursor") -> int:
        return resolve_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        return False


def _pymssql_error_type() -> "type[BaseException]":
    return cast("type[BaseException]", getattr(pymssql, "Error", _UnavailablePymssqlError))


register_driver_profile("pymssql", driver_profile)
