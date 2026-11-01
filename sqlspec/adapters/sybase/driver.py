"""Sybase ASE database driver implementation over pymssql (FreeTDS TDS 5.0)."""

import contextlib
import sys
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.sybase._typing import (
    SybaseConnection,
    SybaseCursor,
    SybaseError,
    SybaseRawCursor,
    SybaseSessionContext,
)
from sqlspec.adapters.sybase.core import (
    build_insert_statement,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    escape_literal_percent,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_rowcount,
)
from sqlspec.adapters.sybase.data_dictionary import SybaseSyncDataDictionary
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
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.adapters.sybase._typing import SybaseQueryParams as QueryParams
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.driver import CachedQuery
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import StatementParameters

__all__ = ("SybaseCursor", "SybaseDriver", "SybaseExceptionHandler", "SybaseSessionContext", "SybaseStreamSource")

logger = get_logger("sqlspec.adapters.sybase")


class SybaseExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling Sybase pymssql exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, Exception) and not isinstance(exc_val, SQLSpecError):
            self.pending_exception = create_mapped_exception(exc_val, logger=logger)
            return True
        return False


class SybaseStreamSource:
    """Native Sybase pymssql chunk source backed by ``cursor.fetchmany()``."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor_manager", "_driver", "_parameters", "_sql")

    def __init__(self, driver: "SybaseDriver", sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor_manager: SybaseCursor | None = None
        self._column_names: list[str] | None = None

    def start(self) -> None:
        cursor_manager = self._driver.with_cursor(self._driver.connection)
        try:
            cursor = cursor_manager.__enter__()
            handler = self._driver.handle_database_exceptions()
            normalized_params = normalize_execute_parameters(self._parameters)
            sql = escape_literal_percent(
                self._sql, normalized_params, self._driver.statement_config.parameter_validator
            )
            if sys.exc_info()[1] is None:
                with handler:
                    cursor.execute(sql, normalized_params)
            else:
                self._driver._run_with_exception_handler(handler, cursor.execute, sql, normalized_params)
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
        if sys.exc_info()[1] is None:
            with handler:
                rows = cursor.fetchmany(self._chunk_size)
        else:
            rows = self._driver._run_with_exception_handler(handler, cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        column_names = self._column_names
        if column_names is None:
            column_names = resolve_column_names(cursor.description or None, self._driver._column_name_cache)
            self._column_names = column_names
        return rows_to_dicts(rows, column_names)

    def close(self, error: bool = False) -> None:
        _ = error
        cursor_manager = self._cursor_manager
        self._cursor_manager = None
        if cursor_manager is not None:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)


class SybaseDriver(SyncDriverAdapterBase):
    """Sybase Adaptive Server Enterprise database driver using pymssql over FreeTDS TDS 5.0."""

    __slots__ = ("_column_name_cache", "_data_dictionary", "_explicit_transaction", "_transaction_active")
    dialect = "sybase"

    def __init__(
        self,
        connection: "SybaseConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SybaseSyncDataDictionary | None = None
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._transaction_active = False
        self._explicit_transaction = False

    @property
    def data_dictionary(self) -> "SybaseSyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = SybaseSyncDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "SybaseRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        normalized_params = normalize_execute_parameters(prepared_parameters)
        sql = escape_literal_percent(sql, normalized_params, self.statement_config.parameter_validator)
        cursor.execute(sql, normalized_params)

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

    def dispatch_execute_many(self, cursor: "SybaseRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        prepared_parameters = normalize_execute_many_parameters(prepared_parameters)
        sql = escape_literal_percent(sql, prepared_parameters, self.statement_config.parameter_validator)
        parameter_count = len(prepared_parameters) if isinstance(prepared_parameters, Sized) else None
        cursor.executemany(sql, cast("Sequence[QueryParams]", prepared_parameters))

        affected_rows = resolve_many_rowcount(cursor, prepared_parameters, fallback_count=parameter_count)
        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: "SybaseRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        normalized_params = normalize_execute_parameters(prepared_parameters)

        successful_count = 0
        for stmt in statements:
            stmt_sql = escape_literal_percent(stmt, normalized_params, self.statement_config.parameter_validator)
            cursor.execute(stmt_sql, normalized_params)
            successful_count += 1
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def begin(self) -> None:
        """Begin a transaction on the Sybase ASE connection."""
        try:
            explicit = bool(getattr(self.connection, "autocommit_state", True))
            if explicit:
                with SybaseCursor(self.connection) as cursor:
                    cursor.execute("BEGIN TRANSACTION")
            self._explicit_transaction = explicit
            self._transaction_active = True
        except Exception as exc:
            mapped = create_mapped_exception(exc, logger=logger)
            msg = f"Failed to begin Sybase ASE transaction: {exc}"
            raise type(mapped)(msg) from exc

    def commit(self) -> None:
        try:
            if self._explicit_transaction:
                with SybaseCursor(self.connection) as cursor:
                    cursor.execute("IF @@TRANCOUNT > 0 COMMIT TRANSACTION")
            else:
                self.connection.commit()
            self._explicit_transaction = False
            self._transaction_active = False
        except Exception as exc:
            mapped = create_mapped_exception(exc, logger=logger)
            msg = f"Failed to commit Sybase ASE transaction: {exc}"
            raise type(mapped)(msg) from exc

    def rollback(self) -> None:
        try:
            if self._explicit_transaction:
                with SybaseCursor(self.connection) as cursor:
                    cursor.execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
            else:
                self.connection.rollback()
        except Exception as exc:
            mapped = create_mapped_exception(exc, logger=logger)
            msg = f"Failed to rollback Sybase ASE transaction: {exc}"
            raise type(mapped)(msg) from exc
        finally:
            self._explicit_transaction = False
            self._transaction_active = False

    def with_cursor(self, connection: "SybaseConnection") -> "SybaseCursor":
        return SybaseCursor(connection)

    def handle_database_exceptions(self) -> "SybaseExceptionHandler":
        return SybaseExceptionHandler()

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native Sybase pymssql row stream backed by ``fetchmany()``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        return SyncRowStream(SybaseStreamSource(self, sql, prepared_parameters, chunk_size))

    def create_savepoint(self, name: str) -> None:
        self.execute_script(f"SAVE TRANSACTION {validate_savepoint_name(name)}")

    def release_savepoint(self, name: str) -> None:
        validate_savepoint_name(name)

    def rollback_to_savepoint(self, name: str) -> None:
        self.execute_script(f"ROLLBACK TRANSACTION {validate_savepoint_name(name)}")

    def has_schema(self, schema: str) -> bool:
        """Return whether the specified user/schema exists in dbo.sysusers."""
        result = False
        exc_handler = self.handle_database_exceptions()
        with self.with_cursor(self.connection) as cursor:
            if sys.exc_info()[1] is None:
                with exc_handler:
                    cursor.execute("SELECT 1 FROM dbo.sysusers WHERE name = %s", (schema,))
                    result = cursor.fetchone() is not None
            else:
                def _fetch() -> None:
                    nonlocal result
                    cursor.execute("SELECT 1 FROM dbo.sysusers WHERE name = %s", (schema,))
                    result = cursor.fetchone() is not None

                self._run_with_exception_handler(exc_handler, _fetch)
        self._check_pending_exception(exc_handler)
        return result

    def select_to_storage(
        self,
        statement: "Statement | QueryBuilder | SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and write the Arrow result to storage."""
        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        pipeline = self._storage_pipeline()
        telemetry_payload = self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = False,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = True,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into the target Sybase ASE table via executemany."""
        self._require_capability("arrow_import_enabled")
        if drop_indexes:
            with self.without_indexes(table, concurrent=concurrent_indexes):
                return self.load_from_arrow(
                    table,
                    source,
                    partitioner=partitioner,
                    overwrite=overwrite,
                    telemetry=telemetry,
                    stage_table=stage_table,
                    conflict_keys=conflict_keys,
                    on_conflict=on_conflict,
                    exclude_update_columns=exclude_update_columns,
                    drop_stage_table=drop_stage_table,
                    drop_indexes=False,
                    type_casts=type_casts,
                )
        if stage_table or conflict_keys:
            return self._execute_staged_load_sync(
                table,
                source,
                raw_loader=self.load_from_arrow,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=drop_indexes,
                concurrent_indexes=concurrent_indexes,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                type_casts=type_casts,
            )
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            statement = f"DELETE FROM {format_identifier(table)}"
            exc_handler = self.handle_database_exceptions()
            with self.with_cursor(self.connection) as cursor:
                if sys.exc_info()[1] is None:
                    with exc_handler:
                        cursor.execute(statement)
                else:
                    self._run_with_exception_handler(exc_handler, cursor.execute, statement)
            self._check_pending_exception(exc_handler)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = escape_literal_percent(
                build_insert_statement(table, columns), records, self.statement_config.parameter_validator
            )
            needs_preparation = self._arrow_rows_need_preparation(arrow_table)
            prepared_records = (
                self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                if needs_preparation
                else records
            )
            normalized_records = normalize_execute_many_parameters(prepared_records)
            exc_handler = self.handle_database_exceptions()
            with self.with_cursor(self.connection) as cursor:
                if sys.exc_info()[1] is None:
                    with exc_handler:
                        cursor.executemany(insert_sql, cast("Sequence[QueryParams]", normalized_records))
                else:
                    self._run_with_exception_handler(
                        exc_handler, cursor.executemany, insert_sql, cast("Sequence[QueryParams]", normalized_records)
                    )
            self._check_pending_exception(exc_handler)

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
        """Load staged artifacts from storage into the target Sybase ASE table."""
        if kwargs:
            return super().load_from_storage(
                table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
            )
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def collect_rows(self, cursor: "SybaseRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        rows, column_names, _row_format = collect_rows(fetched, cursor.description or None, self._column_name_cache)
        return rows, column_names, len(rows)

    def resolve_rowcount(self, cursor: "SybaseRawCursor") -> int:
        return resolve_rowcount(cursor)

    def _execute_cache_hit(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        normalized_params = normalize_execute_parameters(params) if isinstance(params, list) else params
        if not normalized_params or (
            "%" in cached.compiled_sql
            and escape_literal_percent(
                cached.compiled_sql, normalized_params, self.statement_config.parameter_validator
            )
            != cached.compiled_sql
        ):
            statement = self._cached_statement(
                sql,
                normalized_params,
                cached,
                normalized_params or None,
                params_are_simple=True,
                compiled_sql=cached.compiled_sql,
            )
            return self._execute_cached_statement(statement)
        return super()._execute_cache_hit(sql, normalized_params, cached)

    def _connection_in_transaction(self) -> bool:
        """Return whether a transaction opened by this driver remains active."""
        return self._transaction_active


register_driver_profile("sybase", driver_profile)
