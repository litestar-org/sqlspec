"""Teradata Vantage database driver implementation over teradatasql."""

import contextlib
import sys
from typing import TYPE_CHECKING, Any, Literal, cast

from sqlspec.adapters.teradata._typing import (
    TeradataConnection,
    TeradataCursor,
    TeradataRawCursor,
    TeradataSessionContext,
)
from sqlspec.adapters.teradata.core import (
    SCHEMA_EXISTS_SQL,
    build_insert_statement,
    build_set_query_band_sql,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    format_identifier,
    is_ddl_or_session_band_sql,
    maybe_prefix_fastexport,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_rowcount,
)
from sqlspec.adapters.teradata.data_dictionary import TeradataSyncDataDictionary
from sqlspec.adapters.teradata.type_converter import (
    coerce_teradata_row,
    extract_arrow_column_types,
    prepare_teradata_parameters,
)
from sqlspec.core import (
    SQL,
    SQLResult,
    StatementConfig,
    build_arrow_result_from_reader,
    create_arrow_result,
    get_cache_config,
    register_driver_profile,
)
from sqlspec.driver import (
    BaseSyncExceptionHandler,
    ExecutionResult,
    SyncDriverAdapterBase,
    SyncRowStream,
    rows_to_dicts,
)
from sqlspec.exceptions import ImproperConfigurationError, SQLSpecError, TransactionError
from sqlspec.utils.arrow_helpers import (
    arrow_batches_from_rows,
    arrow_reader_with_deferred_close,
    convert_dict_to_arrow_with_schema,
)
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlspec.adapters.teradata._typing import TeradataQueryParams as QueryParams
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, CachedQuery, Statement, StatementFilter
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

__all__ = (
    "TeradataCursor",
    "TeradataDriver",
    "TeradataExceptionHandler",
    "TeradataSessionContext",
    "TeradataStreamSource",
)

logger = get_logger("sqlspec.adapters.teradata")


class TeradataExceptionHandler(BaseSyncExceptionHandler):
    """Context manager for handling Teradata Vantage teradatasql exceptions."""

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, Exception) and not isinstance(exc_val, SQLSpecError):
            self.pending_exception = create_mapped_exception(exc_val, logger=logger)
            return True
        return False


class TeradataStreamSource:
    """Native Teradata teradatasql chunk source backed by ``cursor.fetchmany()``."""

    __slots__ = (
        "_chunk_size",
        "_column_names",
        "_cursor_manager",
        "_driver",
        "_enable_lowercase_column_names",
        "_parameters",
        "_sql",
        "column_names",
        "column_types",
    )

    def __init__(self, driver: "TeradataDriver", sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._enable_lowercase_column_names = bool(driver.driver_features.get("enable_lowercase_column_names", True))
        self._cursor_manager: TeradataCursor | None = None
        self._column_names: list[str] | None = None
        self.column_names: list[str] = []
        self.column_types: dict[str, str] = {}

    def start(self) -> None:
        cursor_manager = self._driver.with_cursor(self._driver.connection)
        try:
            cursor = cursor_manager.__enter__()
            handler = self._driver.handle_database_exceptions()
            if sys.exc_info()[1] is None:
                with handler:
                    normalized_params = prepare_teradata_parameters(self._parameters)
                    if normalized_params:
                        cursor.execute(self._sql, normalized_params)
                    else:
                        cursor.execute(self._sql)
            else:
                normalized_params = prepare_teradata_parameters(self._parameters)
                if normalized_params:
                    self._driver._run_with_exception_handler(handler, cursor.execute, self._sql, normalized_params)
                else:
                    self._driver._run_with_exception_handler(handler, cursor.execute, self._sql)
            self._driver._check_pending_exception(handler)
        except BaseException:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)
            raise
        self._cursor_manager = cursor_manager
        self._driver._stream_sources.append(self)
        self.column_names = resolve_column_names(
            cursor.description,
            self._driver._column_name_cache,
            enable_lowercase_column_names=self._enable_lowercase_column_names,
        )
        self._column_names = self.column_names
        self.column_types = extract_arrow_column_types(cursor.description, column_names=self.column_names)

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
            column_names = resolve_column_names(
                cursor.description or None,
                self._driver._column_name_cache,
                enable_lowercase_column_names=self._enable_lowercase_column_names,
            )
            self._column_names = column_names
        return rows_to_dicts([coerce_teradata_row(row, description=cursor.description) for row in rows], column_names)

    def close(self, error: bool = False) -> None:
        _ = error
        cursor_manager = self._cursor_manager
        self._cursor_manager = None
        if self in self._driver._stream_sources:
            self._driver._stream_sources.remove(self)
        if cursor_manager is not None:
            with contextlib.suppress(Exception):
                cursor_manager.__exit__(None, None, None)


class TeradataDriver(SyncDriverAdapterBase):
    """Teradata Vantage database driver using teradatasql."""

    __slots__ = (
        "_column_name_cache",
        "_data_dictionary",
        "_previous_autocommit",
        "_stream_sources",
        "_transaction_active",
    )
    dialect = "vantage"

    def __init__(
        self,
        connection: "TeradataConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: TeradataSyncDataDictionary | None = None
        self._column_name_cache: dict[tuple[int, bool], tuple[Any, list[str]]] = {}
        self._stream_sources: list[TeradataStreamSource] = []
        self._transaction_active = False
        self._previous_autocommit: bool | None = None

    @property
    def data_dictionary(self) -> "TeradataSyncDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = TeradataSyncDataDictionary()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "TeradataRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, parameters = self._compiled_sql(statement, self.statement_config)
        cursor.arraysize = int(self.driver_features.get("arraysize", 2048))
        if statement.returns_rows():
            sql = maybe_prefix_fastexport(
                sql,
                enable_fastexport=bool(self.driver_features.get("enable_fastexport", False)),
                in_transaction=self._connection_in_transaction(),
            )
        cursor.execute(sql, prepare_teradata_parameters(parameters))
        if statement.returns_rows():
            description = cursor.description or None
            names = resolve_column_names(
                description,
                self._column_name_cache,
                enable_lowercase_column_names=bool(self.driver_features.get("enable_lowercase_column_names", True)),
            )
            rows: list[tuple[Any, ...]] = []
            while chunk := cursor.fetchmany(cursor.arraysize):
                rows.extend(coerce_teradata_row(row, description=description) for row in chunk)
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=names,
                column_types=extract_arrow_column_types(description, column_names=names),
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
            )
        if getattr(self.connection, "autocommit", True) is False and is_ddl_or_session_band_sql(sql):
            self.connection.commit()
        return self.create_execution_result(cursor, rowcount_override=resolve_rowcount(cursor))

    def _execute_cache_hit_with_cursor(
        self, sql: str, params: "tuple[Any, ...] | list[Any] | dict[str, Any]", cached: "CachedQuery"
    ) -> "SQLResult":
        """Execute a compiled cache hit while preserving FastExport, row coercion, and DDL commit rules."""
        _ = sql
        compiled_sql = cached.compiled_sql
        returns_rows = cached.operation_profile.returns_rows
        exec_sql = (
            maybe_prefix_fastexport(
                compiled_sql,
                enable_fastexport=bool(self.driver_features.get("enable_fastexport", False)),
                in_transaction=self._connection_in_transaction(),
            )
            if returns_rows
            else compiled_sql
        )
        prepared_params = prepare_teradata_parameters(params)
        rows: list[tuple[Any, ...]] = []
        names: list[str] = []
        column_types: dict[str, str] = {}
        affected = 0
        exc_handler = self.handle_database_exceptions()

        def _run(cursor: "TeradataRawCursor") -> None:
            nonlocal names, column_types, affected
            cursor.arraysize = int(self.driver_features.get("arraysize", 2048))
            cursor.execute(exec_sql, prepared_params)
            if returns_rows:
                description = cursor.description or None
                names = resolve_column_names(
                    description,
                    self._column_name_cache,
                    enable_lowercase_column_names=bool(self.driver_features.get("enable_lowercase_column_names", True)),
                )
                while chunk := cursor.fetchmany(cursor.arraysize):
                    rows.extend(coerce_teradata_row(row, description=description) for row in chunk)
                column_types = extract_arrow_column_types(description, column_names=names)
                return
            if getattr(self.connection, "autocommit", True) is False and is_ddl_or_session_band_sql(compiled_sql):
                self.connection.commit()
            affected = resolve_rowcount(cursor)

        with self.with_cursor(self.connection) as cursor:
            if sys.exc_info()[1] is None:
                with exc_handler:
                    _run(cursor)
            else:
                self._run_with_exception_handler(exc_handler, _run, cursor)
        self._check_pending_exception(exc_handler)

        if returns_rows:
            return SQLResult(
                statement=None,
                data=rows,
                rows_affected=len(rows),
                operation_type=cached.operation_type,
                column_names=names,
                total_count=len(rows),
                row_format="tuple",
                metadata={"column_types": column_types},
            )
        return SQLResult(
            statement=None, data=[], rows_affected=affected, operation_type=cached.operation_type, total_count=0
        )

    def dispatch_execute_many(self, cursor: "TeradataRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, parameters = self._compiled_sql(statement, self.statement_config)
        prepared = prepare_teradata_parameters(parameters, is_many=True)
        fastload = bool(self.driver_features.get("enable_fastload", False)) and sql.lstrip().upper().startswith(
            "INSERT"
        )
        previous = getattr(self.connection, "autocommit", True)
        if fastload:
            sql = "{fn teradata_try_fastload} " + sql
            self.connection.autocommit = False
        try:
            cursor.executemany(sql, prepared)
            if fastload and previous:
                self.connection.commit()
        except BaseException:
            if fastload and previous:
                self.connection.rollback()
            raise
        finally:
            if fastload:
                self.connection.autocommit = previous
        return self.create_execution_result(
            cursor, rowcount_override=resolve_many_rowcount(cursor, prepared), is_many_result=True
        )

    def dispatch_execute_script(self, cursor: "TeradataRawCursor", statement: "SQL") -> "ExecutionResult":
        sql, parameters = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        for text in statements:
            cursor.execute(text, prepare_teradata_parameters(parameters))
            if getattr(self.connection, "autocommit", True) is False and is_ddl_or_session_band_sql(text):
                self.connection.commit()
        return self.create_execution_result(
            cursor, statement_count=len(statements), successful_statements=len(statements), is_script_result=True
        )

    def begin(self) -> None:
        """Disable native autocommit for a transaction owned by this session."""
        if self._transaction_active:
            msg = "A Teradata transaction is already active"
            raise TransactionError(msg)
        try:
            self._previous_autocommit = bool(getattr(self.connection, "autocommit", True))
            self.connection.autocommit = False
            self._transaction_active = True
        except Exception as exc:
            msg = "Failed to begin Teradata transaction"
            raise TransactionError(msg) from exc

    def commit(self) -> None:
        """Commit manual-mode work and restore the autocommit state captured by ``begin()``."""
        try:
            if getattr(self.connection, "autocommit", True) is False:
                self.connection.commit()
                if self._previous_autocommit is not None:
                    self.connection.autocommit = self._previous_autocommit
            self._transaction_active = False
            self._previous_autocommit = None
        except Exception as exc:
            msg = "Failed to commit Teradata transaction"
            raise TransactionError(msg) from exc

    def rollback(self) -> None:
        """Roll back manual-mode work and restore the autocommit state captured by ``begin()``."""
        try:
            if getattr(self.connection, "autocommit", True) is False:
                self.connection.rollback()
                if self._previous_autocommit is not None:
                    self.connection.autocommit = self._previous_autocommit
            self._transaction_active = False
            self._previous_autocommit = None
        except Exception as exc:
            msg = "Failed to rollback Teradata transaction"
            raise TransactionError(msg) from exc

    def _release_open_work(self) -> None:
        for source in tuple(self._stream_sources):
            source.close()
        if self._connection_in_transaction():
            self.rollback()

    def set_query_band(self, band: "Mapping[str, str] | str", *, transaction: bool = False) -> None:
        """Apply validated query-band metadata to the session or active transaction."""
        if transaction and not self._connection_in_transaction():
            msg = "FOR TRANSACTION query band requires an active transaction"
            raise TransactionError(msg)
        self.execute_script(build_set_query_band_sql(band, transaction=transaction))

    def with_cursor(self, connection: "TeradataConnection") -> "TeradataCursor":
        return TeradataCursor(connection)

    def handle_database_exceptions(self) -> "TeradataExceptionHandler":
        return TeradataExceptionHandler()

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        """Return a native Teradata teradatasql row stream backed by ``fetchmany()``."""
        if not statement.returns_rows():
            return None
        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)
        sql = maybe_prefix_fastexport(
            sql,
            enable_fastexport=bool(self.driver_features.get("enable_fastexport", False)),
            in_transaction=self._connection_in_transaction(),
        )
        return SyncRowStream(TeradataStreamSource(self, sql, prepared_parameters, chunk_size))

    def create_savepoint(self, name: str) -> None:
        msg = "Teradata Vantage does not support SQL savepoints"
        raise NotImplementedError(msg)

    def release_savepoint(self, name: str) -> None:
        msg = "Teradata Vantage does not support SQL savepoints"
        raise NotImplementedError(msg)

    def rollback_to_savepoint(self, name: str) -> None:
        msg = "Teradata Vantage does not support SQL savepoints"
        raise NotImplementedError(msg)

    def has_schema(self, schema: str) -> bool:
        """Return whether the specified user/schema exists in DBC.DatabasesV."""
        result = False
        exc_handler = self.handle_database_exceptions()
        with self.with_cursor(self.connection) as cursor:
            if sys.exc_info()[1] is None:
                with exc_handler:
                    cursor.execute(SCHEMA_EXISTS_SQL, (schema,))
                    result = cursor.fetchone() is not None
            else:

                def _fetch() -> None:
                    nonlocal result
                    cursor.execute(SCHEMA_EXISTS_SQL, (schema,))
                    result = cursor.fetchone() is not None

                self._run_with_exception_handler(exc_handler, _fetch)
        self._check_pending_exception(exc_handler)
        return result

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
        """Convert bounded DB-API chunks to Arrow while retaining empty-result types."""
        if native_only:
            msg = "Teradata Arrow export uses chunked DB-API conversion"
            raise ImproperConfigurationError(msg)
        size = batch_size if batch_size is not None else int(self.driver_features.get("arraysize", 2048))
        if isinstance(size, bool) or size < 1:
            msg = "batch_size must be >= 1"
            raise ImproperConfigurationError(msg)
        fast = bool(self.driver_features.get("enable_fastexport", False))
        prepared = self.prepare_statement(
            statement, parameters, statement_config=statement_config or self.statement_config, kwargs=kwargs
        )
        sql, params = self._compiled_sql(prepared, prepared.statement_config)
        sql = maybe_prefix_fastexport(sql, enable_fastexport=fast, in_transaction=self._connection_in_transaction())
        source = TeradataStreamSource(self, sql, params, size)
        stream: SyncRowStream[dict[str, Any]] = SyncRowStream(source)
        try:
            reader = arrow_batches_from_rows(stream, batch_size=size, arrow_schema=arrow_schema)
            if reader is None:
                data = convert_dict_to_arrow_with_schema(
                    [],
                    return_format=return_format,
                    batch_size=size,
                    arrow_schema=arrow_schema,
                    column_types=source.column_types,
                    column_names=source.column_names,
                )
                return create_arrow_result(statement=prepared, data=data, rows_affected=0)
            return build_arrow_result_from_reader(
                prepared,
                arrow_reader_with_deferred_close(reader, stream.close),
                return_format=return_format,
                batch_size=size,
                arrow_schema=arrow_schema,
            )
        except BaseException:
            stream.close()
            raise

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
        arrow_result = self._select_for_storage(statement, *parameters, statement_config=statement_config, **kwargs)
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
        """Load Arrow data into the target Teradata table via executemany."""
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
        columns, records = self._arrow_table_to_rows(arrow_table)
        fastload = bool(self.driver_features.get("enable_fastload", False)) and not overwrite and bool(records)
        insert_sql = ""
        normalized_records: tuple[Any, ...] | list[Any] = ()
        if records:
            insert_sql = build_insert_statement(table, columns)
            if fastload:
                insert_sql = f"{{fn teradata_try_fastload}} {insert_sql}"
            needs_preparation = self._arrow_rows_need_preparation(arrow_table)
            prepared_records = (
                self.prepare_driver_parameters(records, self.statement_config, is_many=True)
                if needs_preparation
                else records
            )
            normalized_records = prepare_teradata_parameters(prepared_records, is_many=True)

        if overwrite or records:
            previous_autocommit = getattr(self.connection, "autocommit", True)
            owns_transaction = (overwrite or fastload) and previous_autocommit is True
            if owns_transaction or fastload:
                self.connection.autocommit = False
            try:
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
                if records:
                    exc_handler = self.handle_database_exceptions()
                    with self.with_cursor(self.connection) as cursor:
                        if sys.exc_info()[1] is None:
                            with exc_handler:
                                cursor.executemany(insert_sql, cast("Sequence[QueryParams]", normalized_records))
                        else:
                            self._run_with_exception_handler(
                                exc_handler,
                                cursor.executemany,
                                insert_sql,
                                cast("Sequence[QueryParams]", normalized_records),
                            )
                    self._check_pending_exception(exc_handler)
                if owns_transaction:
                    self.connection.commit()
            except BaseException:
                if owns_transaction:
                    self.connection.rollback()
                raise
            finally:
                if owns_transaction or fastload:
                    self.connection.autocommit = previous_autocommit

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
        """Load staged artifacts from storage into the target Teradata table."""
        if kwargs:
            return super().load_from_storage(
                table, source, file_format=file_format, partitioner=partitioner, overwrite=overwrite, **kwargs
            )
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def collect_rows(self, cursor: "TeradataRawCursor", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        description = cursor.description or None
        raw_rows, column_names, _row_format = collect_rows(
            fetched,
            description,
            self._column_name_cache,
            enable_lowercase_column_names=bool(self.driver_features.get("enable_lowercase_column_names", True)),
        )
        coerced_rows = [
            coerce_teradata_row(row, description=description) if not isinstance(row, dict) else row for row in raw_rows
        ]
        return coerced_rows, column_names, len(coerced_rows)

    def resolve_rowcount(self, cursor: "TeradataRawCursor") -> int:
        return resolve_rowcount(cursor)

    def _connection_in_transaction(self) -> bool:
        """Return whether a transaction opened by this driver remains active."""
        return bool(self._transaction_active or getattr(self.connection, "autocommit", True) is False)


register_driver_profile("teradata", driver_profile)
