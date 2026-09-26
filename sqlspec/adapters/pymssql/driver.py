"""pymssql SQL Server driver implementation."""

import contextlib
from collections.abc import Iterable, Sequence, Sized
from typing import TYPE_CHECKING, Any, cast

import sqlglot
from sqlglot import exp

from sqlspec.adapters.pymssql._typing import (
    PymssqlConnection,
    PymssqlCursor,
    PymssqlError,
    PymssqlRawCursor,
    PymssqlSessionContext,
)
from sqlspec.adapters.pymssql.core import (
    build_multi_row_insert,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    quote_tsql_identifier,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_rowcount,
)
from sqlspec.adapters.pymssql.data_dictionary import PymssqlSyncDataDictionary
from sqlspec.core import SQL, ArrowResult, StatementConfig, get_cache_config, register_driver_profile
from sqlspec.core.result import DMLResult, SQLResult
from sqlspec.driver import (
    BaseSyncExceptionHandler,
    ExecutionResult,
    SyncDriverAdapterBase,
    SyncRowStream,
    rows_to_dicts,
    validate_savepoint_name,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.pymssql._typing import PymssqlQueryParams as QueryParams
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import Statement, StatementFilter
    from sqlspec.typing import StatementParameters

__all__ = ("PymssqlCursor", "PymssqlDriver", "PymssqlExceptionHandler", "PymssqlSessionContext")

logger = get_logger("sqlspec.adapters.pymssql")


class PymssqlExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling pymssql exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, PymssqlError):
            self.pending_exception = create_mapped_exception(cast("Exception", exc_val), logger=logger)
            return True
        return False


class PymssqlStreamSource:
    """Native pymssql chunk source backed by ``cursor.fetchmany()``."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor_manager", "_driver", "_parameters", "_sql")

    def __init__(self, driver: "PymssqlDriver", sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor_manager: PymssqlCursor | None = None
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

    def fetch_chunk(self) -> list[dict[str, Any]]:
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
            column_names = resolve_column_names(cursor.description or None, self._driver._column_name_cache)
            self._column_names = column_names
        return rows_to_dicts(rows, column_names)

    def close(self, error: bool = False) -> None:
        cursor_manager = self._cursor_manager
        self._cursor_manager = None
        if cursor_manager is not None:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)


class PymssqlDriver(SyncDriverAdapterBase):
    """SQL Server database driver using pymssql."""

    __slots__ = (
        "_column_name_cache",
        "_data_dictionary",
        "_explicit_transaction",
        "_migration_schema_restore",
        "_transaction_active",
    )
    dialect = "tsql"

    def __init__(
        self,
        connection: PymssqlConnection,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )
        if driver_features is None or "storage_capabilities" not in driver_features:
            driver_features = dict(driver_features) if driver_features else {}
            driver_features["storage_capabilities"] = {
                "arrow_export_enabled": False,
                "arrow_import_enabled": True,
                "parquet_export_enabled": False,
                "parquet_import_enabled": False,
                "partition_strategies": [],
            }

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: PymssqlSyncDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._migration_schema_restore: tuple[str, str] | None = None
        self._transaction_active = False
        self._explicit_transaction = False

    def dispatch_execute(self, cursor: PymssqlRawCursor, statement: SQL) -> ExecutionResult:
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        cursor.execute(sql, normalize_execute_parameters(prepared_parameters))

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            description = cursor.description or None
            rows, column_names, row_format = collect_rows(fetched_data, description, self._column_name_cache)
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=column_names,
                data_row_count=len(rows),
                is_select_result=True,
                row_format=row_format,
            )

        return self.create_execution_result(cursor, rowcount_override=resolve_rowcount(cursor))

    def dispatch_execute_many(self, cursor: PymssqlRawCursor, statement: SQL) -> ExecutionResult:
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        parameter_count = len(prepared_parameters) if isinstance(prepared_parameters, Sized) else None
        cursor.executemany(sql, cast("Sequence[QueryParams]", prepared_parameters))

        affected_rows = resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: PymssqlRawCursor, statement: SQL) -> ExecutionResult:
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        for stmt in statements:
            cursor.execute(stmt, normalize_execute_parameters(prepared_parameters))
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def collect_rows(self, cursor: PymssqlRawCursor, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        rows, column_names, _ = collect_rows(fetched, cursor.description or None, self._column_name_cache)
        return rows, column_names, len(rows)

    def resolve_rowcount(self, cursor: PymssqlRawCursor) -> int:
        return resolve_rowcount(cursor)

    def begin(self) -> None:
        """Begin a transaction on the connection.

        A connection with autocommit disabled already holds an open transaction that
        ``commit()`` and ``rollback()`` end. An autocommit connection issues
        ``BEGIN TRANSACTION``, and the matching ``commit()`` or ``rollback()`` ends it
        with T-SQL because pymssql ignores those calls under autocommit.
        """
        try:
            explicit = bool(self.connection.autocommit_state)
            if explicit:
                with PymssqlCursor(self.connection) as cursor:
                    cursor.execute("BEGIN TRANSACTION")
            self._explicit_transaction = explicit
            self._transaction_active = True
        except PymssqlError as exc:
            msg = f"Failed to begin SQL Server transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def commit(self) -> None:
        try:
            if self._explicit_transaction:
                with PymssqlCursor(self.connection) as cursor:
                    cursor.execute("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")
            else:
                self.connection.commit()
            self._explicit_transaction = False
            self._transaction_active = False
        except PymssqlError as exc:
            msg = f"Failed to commit SQL Server transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def rollback(self) -> None:
        try:
            if self._explicit_transaction:
                with PymssqlCursor(self.connection) as cursor:
                    cursor.execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
            else:
                self.connection.rollback()
            self._explicit_transaction = False
            self._transaction_active = False
        except PymssqlError as exc:
            msg = f"Failed to rollback SQL Server transaction: {exc}"
            raise SQLSpecError(msg) from exc

    def with_cursor(self, connection: PymssqlConnection) -> PymssqlCursor:
        return PymssqlCursor(connection)

    def handle_database_exceptions(self) -> PymssqlExceptionHandler:
        return PymssqlExceptionHandler()

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> SyncRowStream[dict[str, Any]] | None:
        """Return a native pymssql row stream backed by ``fetchmany()``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(PymssqlStreamSource(self, sql, prepared_parameters, chunk_size))

    def create_savepoint(self, name: str) -> None:
        self.execute_script(f"SAVE TRANSACTION {validate_savepoint_name(name)}")

    def release_savepoint(self, name: str) -> None:
        validate_savepoint_name(name)

    def rollback_to_savepoint(self, name: str) -> None:
        self.execute_script(f"ROLLBACK TRANSACTION {validate_savepoint_name(name)}")

    def set_migration_session_schema(self, schema: str) -> None:
        """Point the database user's default schema at the migration schema, remembering the prior one."""
        with self.with_cursor(self.connection) as cursor:
            if self._migration_schema_restore is None:
                cursor.execute("SELECT USER_NAME() AS user_name, SCHEMA_NAME() AS schema_name;")
                row: Any = cursor.fetchone()
                user_name, current_schema = (
                    (row["user_name"], row["schema_name"]) if isinstance(row, dict) else (row[0], row[1])
                )
                cursor.execute(_alter_default_schema_sql(str(user_name), schema))
                self._migration_schema_restore = (str(user_name), str(current_schema))
                return
            cursor.execute(_alter_default_schema_sql(self._migration_schema_restore[0], schema))

    def reset_migration_session_schema(self) -> None:
        """Restore the user's default schema captured by set_migration_session_schema and commit it."""
        if self._migration_schema_restore is None:
            return
        user_name, previous_schema = self._migration_schema_restore
        self._migration_schema_restore = None
        with self.with_cursor(self.connection) as cursor:
            cursor.execute(_alter_default_schema_sql(user_name, previous_schema))
        self.connection.commit()

    def has_schema(self, schema: str) -> bool:
        """Return whether the specified schema exists."""
        with self.with_cursor(self.connection) as cursor:
            cursor.execute("SELECT 1 FROM sys.schemas WHERE name = %s", (schema,))
            return cursor.fetchone() is not None

    @property
    def data_dictionary(self) -> PymssqlSyncDataDictionary:
        if self._data_dictionary is None:
            self._data_dictionary = PymssqlSyncDataDictionary()
        return self._data_dictionary

    def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SQLResult:
        """Execute a statement across parameter sets with multi-row batching."""
        config = statement_config or self.statement_config
        if isinstance(statement, str) and not filters and not kwargs and config is self.statement_config:
            prepared_statement = SQL(
                statement,
                tuple(parameters) if isinstance(parameters, list) else parameters,
                statement_config=config,
                is_many=True,
            )
            cached_statement, prepared_parameters = self._compiled_statement(prepared_statement, config)
            parsed_expression = cached_statement.expression
            if parsed_expression is None and statement.lstrip().upper().startswith("INSERT"):
                with contextlib.suppress(Exception):
                    parsed_expression = sqlglot.parse_one(statement, read="tsql")
            if isinstance(parsed_expression, exp.Insert) and not parsed_expression.args.get("returning"):
                bulk_result = self._execute_bulk_insert_many(parsed_expression, prepared_parameters)
                if bulk_result is not None:
                    return bulk_result
        return super().execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)

    def _execute_bulk_insert_many(self, expression: exp.Insert, prepared_parameters: Any) -> DMLResult | None:
        """Execute a batch INSERT via multi-row VALUES chunking up to 1,000 rows."""
        if not isinstance(prepared_parameters, (list, tuple)) or not prepared_parameters:
            return None
        if not isinstance(expression.this, exp.Schema):
            return None
        if not _is_plain_values_insert(expression, len(expression.this.expressions)):
            return None
        if not isinstance(prepared_parameters[0], (list, tuple)):
            return None

        table_expr = expression.this.this
        if not isinstance(table_expr, exp.Table) or table_expr.alias:
            return None

        column_names = [column.name for column in expression.this.expressions]
        target_table = table_expr.sql(dialect="tsql")
        rows = prepared_parameters
        total_affected = 0
        chunk_size = 1000

        handler = self.handle_database_exceptions()
        with handler, self.with_cursor(self.connection) as cursor:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                chunk_sql = build_multi_row_insert(target_table, column_names, len(chunk))
                flat_params: list[Any] = []
                for row in chunk:
                    flat_params.extend(row)
                cursor.execute(chunk_sql, tuple(flat_params))
                total_affected += len(chunk)
        self._check_pending_exception(handler)
        return DMLResult("INSERT", total_affected)

    def bulk_copy(
        self,
        table_name: str,
        rows: Sequence[Sequence[Any]] | Iterable[Sequence[Any]],
        *,
        column_ids: Sequence[int] | None = None,
        batch_size: int = 1000,
        tablock: bool = False,
        check_constraints: bool = False,
        fire_triggers: bool = False,
    ) -> int:
        """Perform high-performance bulk insert using FreeTDS BCP APIs.

        Args:
            table_name: Target SQL Server table name.
            rows: Sequence or iterable of row tuples/sequences.
            column_ids: Optional 1-based column IDs mapping elements to table columns.
            batch_size: Number of rows per batch commit. Defaults to 1000.
            tablock: Apply TABLOCK hint for minimal logging.
            check_constraints: Enforce table constraints during BCP.
            fire_triggers: Execute insert triggers during BCP.

        Returns:
            Number of rows ingested.
        """
        row_list = list(rows) if not isinstance(rows, (list, tuple)) else rows
        if not row_list:
            return 0
        formatted_table = format_identifier(table_name)
        handler = self.handle_database_exceptions()
        with handler:
            self.connection.bulk_copy(
                formatted_table,
                row_list,
                column_ids=list(column_ids) if column_ids is not None else None,
                batch_size=batch_size,
                tablock=tablock,
                check_constraints=check_constraints,
                fire_triggers=fire_triggers,
            )
        self._check_pending_exception(handler)
        return len(row_list)

    def load_from_arrow(
        self,
        table: str,
        source: ArrowResult | Any,
        *,
        partitioner: dict[str, object] | None = None,
        overwrite: bool = False,
        telemetry: StorageTelemetry | None = None,
        batch_size: int = 1000,
        tablock: bool = False,
        check_constraints: bool = False,
        fire_triggers: bool = False,
        column_ids: Sequence[int] | None = None,
    ) -> StorageBridgeJob:
        """Load Arrow data into SQL Server via FreeTDS BCP bulk copy."""
        self._require_capability("arrow_import_enabled")
        if overwrite:
            quoted_table = format_identifier(table)
            handler = self.handle_database_exceptions()
            with handler, self.with_cursor(self.connection) as cursor:
                try:
                    cursor.execute(f"TRUNCATE TABLE {quoted_table}")
                except Exception as exc:
                    error_msg = str(exc)
                    if "4712" in error_msg or "foreign key" in error_msg.lower():
                        cursor.execute(f"DELETE FROM {quoted_table}")
                    else:
                        raise
            self._check_pending_exception(handler)

        arrow_table = self._coerce_arrow_table(source)
        if arrow_table.num_rows > 0:
            for batch in arrow_table.to_batches():
                pydict = batch.to_pydict()
                rows = list(zip(*pydict.values(), strict=False))
                self.bulk_copy(
                    table,
                    rows,
                    column_ids=column_ids,
                    batch_size=batch_size,
                    tablock=tablock,
                    check_constraints=check_constraints,
                    fire_triggers=fire_triggers,
                )

        telemetry_payload = self._ingest_telemetry(arrow_table)
        extra = telemetry_payload.setdefault("extra", {})
        extra["rows_ingested"] = arrow_table.num_rows
        telemetry_payload["rows_processed"] = arrow_table.num_rows
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_storage(
        self,
        table: str,
        source: StorageDestination,
        *,
        file_format: StorageFormat,
        partitioner: dict[str, object] | None = None,
        overwrite: bool = False,
    ) -> StorageBridgeJob:
        """Load staged artifacts from storage into SQL Server via BCP."""
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def _connection_in_transaction(self) -> bool:
        """Return whether a transaction opened by this driver remains active."""
        return self._transaction_active


def _is_plain_values_insert(expression: exp.Insert, expected_columns: int) -> bool:
    values = expression.expression
    if not isinstance(values, exp.Values):
        return False
    rows = values.expressions
    if len(rows) != 1:
        return False
    row = rows[0]
    if not isinstance(row, exp.Tuple):
        return False
    return len(row.expressions) == expected_columns


def _alter_default_schema_sql(user_name: str, schema: str) -> str:
    return f"ALTER USER {quote_tsql_identifier(user_name)} WITH DEFAULT_SCHEMA = {quote_tsql_identifier(schema)};"


register_driver_profile("pymssql", driver_profile)
