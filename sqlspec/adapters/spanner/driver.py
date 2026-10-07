"""Spanner driver implementation."""

from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast, overload

from sqlglot import exp as _sqlglot_exp

from sqlspec.adapters.spanner._typing import (
    SpannerAsyncConnection,
    SpannerAsyncCursor,
    SpannerAsyncSessionContext,
    SpannerAsyncTransaction,
    SpannerGoogleAPICallError,
    SpannerSyncCursor,
    SpannerSyncSessionContext,
    SpannerTransaction,
)
from sqlspec.adapters.spanner.core import (
    SpannerAsyncStreamSource,
    SpannerExecuteOptions,
    SpannerSyncStreamSource,
    build_execute_kwargs,
    build_param_type_signature,
    chunk_mutation_rows,
    coerce_params,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    execute_ddl_async,
    execute_ddl_sync,
    infer_param_types,
    is_ddl_statement,
    is_query_statement,
    pop_execute_options,
    renew_transaction,
    resolve_row_plan,
    resolve_transaction_completion,
    run_in_transaction_async,
    run_in_transaction_sync,
    supports_batch_update,
    supports_write,
)
from sqlspec.adapters.spanner.data_dictionary import SpannerAsyncDataDictionary, SpannerSyncDataDictionary
from sqlspec.core import StatementConfig, register_driver_profile
from sqlspec.driver import (
    AsyncDriverAdapterBase,
    AsyncRowStream,
    BaseAsyncExceptionHandler,
    BaseSyncExceptionHandler,
    ExecutionResult,
    SyncDriverAdapterBase,
    SyncRowStream,
)
from sqlspec.exceptions import SQLConversionError
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.spanner._typing import SpannerSyncConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.core.statement import SQL
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import SchemaT, StatementParameters

__all__ = (
    "SpannerAsyncCursor",
    "SpannerAsyncDriver",
    "SpannerAsyncExceptionHandler",
    "SpannerAsyncSessionContext",
    "SpannerSyncCursor",
    "SpannerSyncDriver",
    "SpannerSyncExceptionHandler",
    "SpannerSyncSessionContext",
)


_READ_ONLY_SNAPSHOT_ERROR_MESSAGE = (
    "Cannot execute DML in a read-only Snapshot context. "
    "SpannerSyncConfig.provide_session() opens a write-capable Transaction by default; "
    "the current session must have been opened via SpannerSyncConfig.provide_read_session()."
)

_READ_ONLY_ASYNC_SNAPSHOT_ERROR_MESSAGE = (
    "Cannot execute DML in a read-only Snapshot context. "
    "SpannerAsyncConfig.provide_session() opens a write-capable Transaction by default; "
    "the current session must have been opened via SpannerAsyncConfig.provide_read_session()."
)


class SpannerSyncExceptionHandler(BaseSyncExceptionHandler):
    """Map Spanner client exceptions to SQLSpec exceptions.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False

        if isinstance(exc_val, SpannerGoogleAPICallError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class SpannerAsyncExceptionHandler(BaseAsyncExceptionHandler):
    """Map Spanner client exceptions to SQLSpec exceptions.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False

        if isinstance(exc_val, SpannerGoogleAPICallError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class SpannerSyncDriver(SyncDriverAdapterBase):
    """Synchronous Spanner driver operating on Snapshot or Transaction contexts."""

    dialect: "DialectType" = "spanner"
    __slots__ = (
        "_data_dictionary",
        "_owns_transaction",
        "_pending_execute_options",
        "_row_plan_cache",
        "_row_plan_deserializer",
    )

    def __init__(
        self,
        connection: "SpannerSyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = dict(driver_features) if driver_features else {}
        if statement_config is None:
            statement_config = default_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)
        self._data_dictionary: SpannerSyncDataDictionary | None = None
        self._owns_transaction = True
        self._pending_execute_options: SpannerExecuteOptions | None = None
        self._row_plan_cache: dict[int, tuple[Any, list[str], tuple[tuple[int, Any], ...] | None]] = {}
        self._row_plan_deserializer = cast("Callable[[str], Any]", features.get("json_deserializer", from_json))

    def dispatch_execute(self, cursor: "SpannerSyncConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._compiled_sql(statement, self.statement_config)
        if is_ddl_statement(sql):
            self._commit_before_ddl()
            execute_ddl_sync(self._resolve_database(), [sql])
            return self.create_execution_result(cursor, rowcount_override=0)
        params = cast("dict[str, Any] | None", params)
        param_types_map = self._infer_param_types(params)
        coerced_params = self._coerce_params(params)

        if statement.returns_rows():
            reader = cast("_SpannerReadProtocol", cursor)
            execute_kwargs = self._execute_kwargs(for_read=True)
            result_set = reader.execute_sql(sql, params=coerced_params, param_types=param_types_map, **execute_kwargs)
            rows = list(result_set)
            try:
                metadata = result_set.metadata
                row_type = metadata.row_type
                fields = row_type.fields
            except AttributeError:
                fields = None
            if not fields:
                msg = "Result set metadata not available."
                raise SQLConversionError(msg)
            column_names, column_plan = self._resolve_row_plan(fields)
            data, column_names = collect_rows(rows, fields, column_names=column_names, column_plan=column_plan)
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="tuple",
            )

        if supports_write(cursor):
            writer = cast("_SpannerWriteProtocol", cursor)
            execute_kwargs = self._execute_kwargs()
            row_count = writer.execute_update(sql, params=coerced_params, param_types=param_types_map, **execute_kwargs)
            return self.create_execution_result(cursor, rowcount_override=row_count)

        raise SQLConversionError(_READ_ONLY_SNAPSHOT_ERROR_MESSAGE)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "SyncRowStream[dict[str, Any]] | None":
        if not statement.returns_rows():
            return None
        sql, params = self._compiled_sql(statement, self.statement_config)
        params = cast("dict[str, Any] | None", params)
        param_types_map = self._infer_param_types(params)
        coerced_params = self._coerce_params(params)
        return SyncRowStream(
            SpannerSyncStreamSource(
                self, sql, coerced_params, param_types_map, chunk_size, self._execute_kwargs(for_read=True)
            )
        )

    def dispatch_execute_many(self, cursor: "SpannerSyncConnection", statement: "SQL") -> ExecutionResult:
        if not supports_batch_update(cursor):
            msg = "execute_many requires a Transaction context"
            raise SQLConversionError(msg)

        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        if not isinstance(prepared_parameters, list):
            msg = "execute_many requires a list of parameter sets"
            raise SQLConversionError(msg)

        _coerce = self._coerce_params
        _infer = self._infer_param_types
        execute_kwargs = self._execute_kwargs(for_batch=True)
        param_types_cache: dict[tuple[tuple[str, type[Any], Any], ...], dict[str, Any]] = {}
        empty_param_types: dict[str, Any] = {}
        batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
        append_batch_arg = batch_args.append
        for params in prepared_parameters:
            raw_params = cast("dict[str, Any] | None", params)
            coerced_params = _coerce(raw_params)
            if not coerced_params:
                append_batch_arg((sql, {}, empty_param_types))
                continue
            signature = build_param_type_signature(raw_params)
            param_types = param_types_cache.get(signature)
            if param_types is None:
                param_types = _infer(raw_params)
                param_types_cache[signature] = param_types
            append_batch_arg((sql, coerced_params, param_types))

        writer = cast("_SpannerWriteProtocol", cursor)
        _status, row_counts = writer.batch_update(batch_args, **execute_kwargs)
        total_rows = sum(row_counts) if row_counts else 0

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: "SpannerSyncConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        is_transaction = supports_write(cursor)
        reader = cast("_SpannerReadProtocol", cursor)

        count = 0
        script_params = cast("dict[str, Any] | None", params)
        param_types_map = self._infer_param_types(script_params)
        coerced_params = self._coerce_params(script_params)
        read_execute_kwargs = self._execute_kwargs(for_read=True)
        write_execute_kwargs = self._execute_kwargs()
        dialect_str = str(self.dialect) if self.dialect else "spanner"
        pending_ddl: list[str] = []
        for index, stmt in enumerate(statements):
            if is_ddl_statement(stmt):
                pending_ddl.append(stmt)
                count += 1
                continue
            if pending_ddl:
                self._commit_before_ddl()
                execute_ddl_sync(self._resolve_database(), pending_ddl)
                pending_ddl = []
                cursor = self.connection
                reader = cast("_SpannerReadProtocol", cursor)
            is_select = is_query_statement(stmt, dialect_str)
            if not is_select and not is_transaction:
                raise SQLConversionError(_READ_ONLY_SNAPSHOT_ERROR_MESSAGE)
            if not is_select and is_transaction:
                writer = cast("_SpannerWriteProtocol", cursor)
                statement_kwargs = write_execute_kwargs
                if "last_statement" in write_execute_kwargs and index != len(statements) - 1:
                    statement_kwargs = {
                        key: value for key, value in write_execute_kwargs.items() if key != "last_statement"
                    }
                writer.execute_update(stmt, params=coerced_params, param_types=param_types_map, **statement_kwargs)
            else:
                _ = list(
                    reader.execute_sql(stmt, params=coerced_params, param_types=param_types_map, **read_execute_kwargs)
                )
            count += 1

        if pending_ddl:
            self._commit_before_ddl()
            execute_ddl_sync(self._resolve_database(), pending_ddl)
        return self.create_execution_result(
            cursor, statement_count=count, successful_statements=count, is_script_result=True
        )

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        """Commit the active transaction when it has work to commit."""
        if isinstance(self.connection, SpannerTransaction):
            exc_handler = self.handle_database_exceptions()
            with exc_handler:
                if resolve_transaction_completion(self.connection, failed=False) == "commit":
                    cast("_SpannerWriteProtocol", self.connection).commit()
            self._check_pending_exception(exc_handler)
            self._renew_finished_transaction()

    def rollback(self) -> None:
        """Roll back the active transaction when it has begun."""
        if isinstance(self.connection, SpannerTransaction):
            exc_handler = self.handle_database_exceptions()
            with exc_handler:
                if resolve_transaction_completion(self.connection, failed=True) == "rollback":
                    cast("_SpannerWriteProtocol", self.connection).rollback()
            self._check_pending_exception(exc_handler)
            self._renew_finished_transaction()

    def create_savepoint(self, name: str) -> None:
        """Raise because Spanner does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "Spanner does not support savepoints."
        raise NotImplementedError(msg)

    def release_savepoint(self, name: str) -> None:
        """Raise because Spanner does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "Spanner does not support savepoints."
        raise NotImplementedError(msg)

    def rollback_to_savepoint(self, name: str) -> None:
        """Raise because Spanner does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "Spanner does not support savepoints."
        raise NotImplementedError(msg)

    def with_cursor(self, connection: "SpannerSyncConnection") -> "SpannerSyncCursor":
        return SpannerSyncCursor(connection)

    def handle_database_exceptions(self) -> "SpannerSyncExceptionHandler":
        return SpannerSyncExceptionHandler()

    def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with optional Spanner per-call request options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return super().execute(statement, *parameters, statement_config=statement_config, **kwargs)
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return super().execute(statement, *parameters, statement_config=statement_config, **kwargs)
        finally:
            self._pending_execute_options = previous_options

    def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a batch statement with optional Spanner per-call request options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return super().execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return super().execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)
        finally:
            self._pending_execute_options = previous_options

    def execute_script(
        self,
        statement: "str | SQL",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script with optional Spanner per-call request options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)
        finally:
            self._pending_execute_options = previous_options

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "SyncRowStream[SchemaT]": ...

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "SyncRowStream[dict[str, Any]]": ...

    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "SyncRowStream[SchemaT] | SyncRowStream[dict[str, Any]]":
        """Execute a query and stream rows with optional Spanner per-call options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return super().select_stream(
                statement,
                *parameters,
                schema_type=schema_type,
                statement_config=statement_config,
                chunk_size=chunk_size,
                native_only=native_only,
                **kwargs,
            )
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return super().select_stream(
                statement,
                *parameters,
                schema_type=schema_type,
                statement_config=statement_config,
                chunk_size=chunk_size,
                native_only=native_only,
                **kwargs,
            )
        finally:
            self._pending_execute_options = previous_options

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
        """Execute query and stream Arrow results to storage."""
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
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into Spanner table via batch mutations."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)

        exc_handler = self.handle_database_exceptions()
        with exc_handler:
            if overwrite:
                dialect_str = str(self.dialect) if self.dialect else "spanner"
                table_sql = _sqlglot_exp.to_table(table, dialect=dialect_str).sql(dialect=dialect_str, identify=True)
                delete_sql = f"DELETE FROM {table_sql} WHERE TRUE"
                if isinstance(self.connection, SpannerTransaction):
                    writer = cast("_SpannerWriteProtocol", self.connection)
                    writer.execute_update(delete_sql)
                else:
                    msg = "Delete requires a Transaction context."
                    raise SQLConversionError(msg)

            columns, records = self._arrow_table_to_rows(arrow_table)
            if records:
                chunks = self._chunk_mutation_rows(columns, records)
                if self.driver_features.get("enable_batch_write_api") and not overwrite:
                    self._batch_write_mutations(table, columns, chunks)
                else:
                    conn = self.connection
                    if not isinstance(conn, SpannerTransaction):
                        msg = "Arrow import requires a Transaction context."
                        raise SQLConversionError(msg)
                    writer = cast("_SpannerWriteProtocol", conn)
                    for chunk in chunks:
                        writer.insert_or_update(table, columns, chunk)
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
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into Spanner table."""
        arrow_table, inbound = self._read_storage_arrow(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    @property
    def data_dictionary(self) -> "SpannerSyncDataDictionary":
        if self._data_dictionary is None:
            dialect_str = str(self.statement_config.dialect) if self.statement_config.dialect else "spanner"
            mode = "postgresql" if dialect_str in {"spangres", "postgres", "postgresql"} else "googlesql"
            self._data_dictionary = SpannerSyncDataDictionary(mode=mode)
        return self._data_dictionary

    def collect_rows(self, cursor: "SpannerSyncConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect Spanner rows for the direct execution path.

        Dict rows supply their keys as column names; other rows are returned
        unchanged with no column names.
        """
        if not fetched:
            return [], [], 0
        if isinstance(fetched[0], dict):
            column_names = list(fetched[0].keys())
            return fetched, column_names, len(fetched)
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: "SpannerSyncConnection") -> int:
        """Resolve rowcount from Spanner cursor for the direct execution path.

        Spanner uses execute_update return value, not cursor.rowcount, so this
        returns 0.
        """
        return 0

    def _execute_kwargs(self, *, for_read: bool = False, for_batch: bool = False) -> dict[str, Any]:
        return build_execute_kwargs(
            self.driver_features, self._pending_execute_options, for_read=for_read, for_batch=for_batch
        )

    def _chunk_mutation_rows(self, columns: "list[str]", records: "list[tuple[Any, ...]]") -> "list[list[list[Any]]]":
        return chunk_mutation_rows(columns, records, self._coerce_params)

    def _resolve_database(self) -> Any:
        return cast("Any", self.connection)._session._database

    def _batch_write_mutations(self, table: str, columns: "list[str]", chunks: "list[list[list[Any]]]") -> None:
        """High-throughput ingest via the Spanner Batch Write API (one mutation group per chunk)."""
        database = self._resolve_database()
        with database.mutation_groups() as mutation_groups:
            for chunk in chunks:
                group = mutation_groups.group()
                group.insert_or_update(table, columns, chunk)
            for response in mutation_groups.batch_write():
                status = response.status
                if status is not None and status.code:
                    msg = f"Spanner batch_write group failed: {status.message}"
                    raise SQLConversionError(msg)

    def run_in_transaction(self, fn: "Callable[..., Any]", *args: Any, **kwargs: Any) -> Any:
        """Execute a callable inside Spanner's retryable transaction runner.

        ``fn`` is called as ``fn(driver, *args, **kwargs)`` with a driver bound
        to the runner's transaction. A ``DeadlockError`` caused by
        ``google.api_core.exceptions.Aborted`` is unwrapped so the runner
        retries, and a terminal ``Aborted`` is raised as ``DeadlockError``.
        """
        return run_in_transaction_sync(self._resolve_database(), self._transaction_driver, fn, *args, **kwargs)

    def _commit_before_ddl(self) -> None:
        """Commit a begun session transaction before a schema change, as DDL does on MySQL or Oracle."""
        if self._owns_transaction and isinstance(self.connection, SpannerTransaction):
            self.commit()

    def _renew_finished_transaction(self) -> None:
        """Start a new transaction on the session once the owned transaction has finished."""
        connection = cast("Any", self.connection)
        if self._owns_transaction and (connection.committed is not None or connection.rolled_back):
            self.connection = renew_transaction(connection)

    def _transaction_driver(self, transaction: "SpannerSyncConnection") -> "SpannerSyncDriver":
        driver = type(self)(
            connection=transaction, statement_config=self.statement_config, driver_features=self.driver_features
        )
        driver._owns_transaction = False
        return driver

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False

    def _coerce_params(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any] | None":
        return coerce_params(
            params,
            json_serializer=self.driver_features.get("json_serializer"),
            enable_uuid_conversion=self.driver_features.get("enable_uuid_conversion", True),
        )

    def _infer_param_types(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any]":
        return infer_param_types(params)

    def _resolve_row_plan(self, fields: Any) -> "tuple[list[str], tuple[tuple[int, Any], ...] | None]":
        json_deserializer = cast("Callable[[str], Any]", self.driver_features.get("json_deserializer", from_json))
        if json_deserializer is not self._row_plan_deserializer:
            self._row_plan_cache.clear()
            self._row_plan_deserializer = json_deserializer
        return resolve_row_plan(fields, self._row_plan_cache, json_deserializer=json_deserializer)


class SpannerAsyncDriver(AsyncDriverAdapterBase):
    """Asynchronous Spanner driver operating on AsyncSnapshot or AsyncTransaction contexts."""

    dialect: "DialectType" = "spanner"
    __slots__ = (
        "_data_dictionary",
        "_owns_transaction",
        "_pending_execute_options",
        "_row_plan_cache",
        "_row_plan_deserializer",
    )

    def __init__(
        self,
        connection: "SpannerAsyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = dict(driver_features) if driver_features else {}
        if statement_config is None:
            statement_config = default_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)
        self._data_dictionary: SpannerAsyncDataDictionary | None = None
        self._owns_transaction = True
        self._pending_execute_options: SpannerExecuteOptions | None = None
        self._row_plan_cache: dict[int, tuple[Any, list[str], tuple[tuple[int, Any], ...] | None]] = {}
        self._row_plan_deserializer = cast("Callable[[str], Any]", features.get("json_deserializer", from_json))

    async def dispatch_execute(self, cursor: "SpannerAsyncConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._compiled_sql(statement, self.statement_config)
        if is_ddl_statement(sql):
            await self._commit_before_ddl()
            await execute_ddl_async(self._resolve_database(), [sql])
            return self.create_execution_result(cursor, rowcount_override=0)
        params = cast("dict[str, Any] | None", params)
        param_types_map = self._infer_param_types(params)
        coerced_params = self._coerce_params(params)

        if statement.returns_rows():
            reader = cast("_SpannerAsyncReadProtocol", cursor)
            execute_kwargs = self._execute_kwargs(for_read=True)
            result_set = await reader.execute_sql(
                sql, params=coerced_params, param_types=param_types_map, **execute_kwargs
            )
            rows = [row async for row in result_set]
            try:
                metadata = result_set.metadata
                row_type = metadata.row_type
                fields = row_type.fields
            except AttributeError:
                fields = None
            if not fields:
                msg = "Result set metadata not available."
                raise SQLConversionError(msg)
            column_names, column_plan = self._resolve_row_plan(fields)
            data, column_names = collect_rows(rows, fields, column_names=column_names, column_plan=column_plan)
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(data),
                is_select_result=True,
                row_format="tuple",
            )

        if supports_write(cursor):
            writer = cast("_SpannerAsyncWriteProtocol", cursor)
            execute_kwargs = self._execute_kwargs()
            row_count = await writer.execute_update(
                sql, params=coerced_params, param_types=param_types_map, **execute_kwargs
            )
            return self.create_execution_result(cursor, rowcount_override=row_count)

        raise SQLConversionError(_READ_ONLY_ASYNC_SNAPSHOT_ERROR_MESSAGE)

    def dispatch_select_stream(self, statement: "SQL", chunk_size: int) -> "AsyncRowStream[dict[str, Any]] | None":
        if not statement.returns_rows():
            return None
        sql, params = self._compiled_sql(statement, self.statement_config)
        params = cast("dict[str, Any] | None", params)
        param_types_map = self._infer_param_types(params)
        coerced_params = self._coerce_params(params)
        return AsyncRowStream(
            SpannerAsyncStreamSource(
                self, sql, coerced_params, param_types_map, chunk_size, self._execute_kwargs(for_read=True)
            )
        )

    async def dispatch_execute_many(self, cursor: "SpannerAsyncConnection", statement: "SQL") -> ExecutionResult:
        if not supports_batch_update(cursor):
            msg = "execute_many requires a Transaction context"
            raise SQLConversionError(msg)

        sql, prepared_parameters = self._compiled_sql(statement, self.statement_config)

        if not isinstance(prepared_parameters, list):
            msg = "execute_many requires a list of parameter sets"
            raise SQLConversionError(msg)

        _coerce = self._coerce_params
        _infer = self._infer_param_types
        execute_kwargs = self._execute_kwargs(for_batch=True)
        param_types_cache: dict[tuple[tuple[str, type[Any], Any], ...], dict[str, Any]] = {}
        empty_param_types: dict[str, Any] = {}
        batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
        append_batch_arg = batch_args.append
        for params in prepared_parameters:
            raw_params = cast("dict[str, Any] | None", params)
            coerced_params = _coerce(raw_params)
            if not coerced_params:
                append_batch_arg((sql, {}, empty_param_types))
                continue
            signature = build_param_type_signature(raw_params)
            param_types = param_types_cache.get(signature)
            if param_types is None:
                param_types = _infer(raw_params)
                param_types_cache[signature] = param_types
            append_batch_arg((sql, coerced_params, param_types))

        writer = cast("_SpannerAsyncWriteProtocol", cursor)
        _status, row_counts = await writer.batch_update(batch_args, **execute_kwargs)
        total_rows = sum(row_counts) if row_counts else 0

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "SpannerAsyncConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        is_transaction = supports_write(cursor)
        reader = cast("_SpannerAsyncReadProtocol", cursor)

        count = 0
        script_params = cast("dict[str, Any] | None", params)
        param_types_map = self._infer_param_types(script_params)
        coerced_params = self._coerce_params(script_params)
        read_execute_kwargs = self._execute_kwargs(for_read=True)
        write_execute_kwargs = self._execute_kwargs()
        dialect_str = str(self.dialect) if self.dialect else "spanner"
        pending_ddl: list[str] = []
        for index, stmt in enumerate(statements):
            if is_ddl_statement(stmt):
                pending_ddl.append(stmt)
                count += 1
                continue
            if pending_ddl:
                await self._commit_before_ddl()
                await execute_ddl_async(self._resolve_database(), pending_ddl)
                pending_ddl = []
                cursor = self.connection
                reader = cast("_SpannerAsyncReadProtocol", cursor)
            is_select = is_query_statement(stmt, dialect_str)
            if not is_select and not is_transaction:
                raise SQLConversionError(_READ_ONLY_ASYNC_SNAPSHOT_ERROR_MESSAGE)
            if not is_select and is_transaction:
                writer = cast("_SpannerAsyncWriteProtocol", cursor)
                statement_kwargs = write_execute_kwargs
                if "last_statement" in write_execute_kwargs and index != len(statements) - 1:
                    statement_kwargs = {
                        key: value for key, value in write_execute_kwargs.items() if key != "last_statement"
                    }
                await writer.execute_update(
                    stmt, params=coerced_params, param_types=param_types_map, **statement_kwargs
                )
            else:
                rs = await reader.execute_sql(
                    stmt, params=coerced_params, param_types=param_types_map, **read_execute_kwargs
                )
                _ = [row async for row in rs]
            count += 1

        if pending_ddl:
            await self._commit_before_ddl()
            await execute_ddl_async(self._resolve_database(), pending_ddl)
        return self.create_execution_result(
            cursor, statement_count=count, successful_statements=count, is_script_result=True
        )

    async def begin(self) -> None:
        return None

    async def commit(self) -> None:
        """Commit the active transaction when it has work to commit."""
        if isinstance(self.connection, SpannerAsyncTransaction):
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                if resolve_transaction_completion(self.connection, failed=False) == "commit":
                    await cast("_SpannerAsyncWriteProtocol", self.connection).commit()
            self._check_pending_exception(exc_handler)
            self._renew_finished_transaction()

    async def rollback(self) -> None:
        """Roll back the active transaction when it has begun."""
        if isinstance(self.connection, SpannerAsyncTransaction):
            exc_handler = self.handle_database_exceptions()
            async with exc_handler:
                if resolve_transaction_completion(self.connection, failed=True) == "rollback":
                    await cast("_SpannerAsyncWriteProtocol", self.connection).rollback()
            self._check_pending_exception(exc_handler)
            self._renew_finished_transaction()

    async def create_savepoint(self, name: str) -> None:
        """Raise because Spanner does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "Spanner does not support savepoints."
        raise NotImplementedError(msg)

    async def release_savepoint(self, name: str) -> None:
        """Raise because Spanner does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "Spanner does not support savepoints."
        raise NotImplementedError(msg)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Raise because Spanner does not support savepoints.

        Raises:
            NotImplementedError: Always.
        """
        msg = "Spanner does not support savepoints."
        raise NotImplementedError(msg)

    def with_cursor(self, connection: "SpannerAsyncConnection") -> "SpannerAsyncCursor":
        return SpannerAsyncCursor(connection)

    def handle_database_exceptions(self) -> "SpannerAsyncExceptionHandler":
        return SpannerAsyncExceptionHandler()

    async def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with optional Spanner per-call request options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return await super().execute(statement, *parameters, statement_config=statement_config, **kwargs)
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return await super().execute(statement, *parameters, statement_config=statement_config, **kwargs)
        finally:
            self._pending_execute_options = previous_options

    async def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a batch statement with optional Spanner per-call request options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return await super().execute_many(
                statement, parameters, *filters, statement_config=statement_config, **kwargs
            )
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return await super().execute_many(
                statement, parameters, *filters, statement_config=statement_config, **kwargs
            )
        finally:
            self._pending_execute_options = previous_options

    async def execute_script(
        self,
        statement: "str | SQL",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script with optional Spanner per-call request options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return await super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return await super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)
        finally:
            self._pending_execute_options = previous_options

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT]": ...

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[dict[str, Any]]": ...

    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT] | AsyncRowStream[dict[str, Any]]":
        """Execute a query and stream rows with optional Spanner per-call options."""
        execute_options = pop_execute_options(kwargs)
        if execute_options is None:
            return super().select_stream(
                statement,
                *parameters,
                schema_type=schema_type,
                statement_config=statement_config,
                chunk_size=chunk_size,
                native_only=native_only,
                **kwargs,
            )
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return super().select_stream(
                statement,
                *parameters,
                schema_type=schema_type,
                statement_config=statement_config,
                chunk_size=chunk_size,
                native_only=native_only,
                **kwargs,
            )
        finally:
            self._pending_execute_options = previous_options

    async def select_to_storage(
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
        """Execute query and stream Arrow results to storage asynchronously."""
        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_storage_result(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into Spanner table via batch mutations."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)

        exc_handler = self.handle_database_exceptions()
        async with exc_handler:
            if overwrite:
                dialect_str = str(self.dialect) if self.dialect else "spanner"
                table_sql = _sqlglot_exp.to_table(table, dialect=dialect_str).sql(dialect=dialect_str, identify=True)
                delete_sql = f"DELETE FROM {table_sql} WHERE TRUE"
                if isinstance(self.connection, SpannerAsyncTransaction):
                    writer = cast("_SpannerAsyncWriteProtocol", self.connection)
                    await writer.execute_update(delete_sql)
                else:
                    msg = "Delete requires a Transaction context."
                    raise SQLConversionError(msg)

            columns, records = self._arrow_table_to_rows(arrow_table)
            if records:
                chunks = self._chunk_mutation_rows(columns, records)
                if self.driver_features.get("enable_batch_write_api") and not overwrite:
                    await self._batch_write_mutations(table, columns, chunks)
                else:
                    conn = self.connection
                    if not isinstance(conn, SpannerAsyncTransaction):
                        msg = "Arrow import requires a Transaction context."
                        raise SQLConversionError(msg)
                    writer = cast("_SpannerAsyncWriteProtocol", conn)
                    for chunk in chunks:
                        writer.insert_or_update(table, columns, chunk)
        self._check_pending_exception(exc_handler)

        telemetry_payload = self._ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into Spanner table asynchronously."""
        arrow_table, inbound = await self._read_storage_arrow(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    @property
    def data_dictionary(self) -> "SpannerAsyncDataDictionary":
        if self._data_dictionary is None:
            dialect_str = str(self.statement_config.dialect) if self.statement_config.dialect else "spanner"
            mode = "postgresql" if dialect_str in {"spangres", "postgres", "postgresql"} else "googlesql"
            self._data_dictionary = SpannerAsyncDataDictionary(mode=mode)
        return self._data_dictionary

    def collect_rows(
        self, cursor: "SpannerAsyncConnection", fetched: "list[Any]"
    ) -> "tuple[list[Any], list[str], int]":
        """Collect Spanner rows for the direct execution path.

        Dict rows supply their keys as column names; other rows are returned
        unchanged with no column names.
        """
        if not fetched:
            return [], [], 0
        if isinstance(fetched[0], dict):
            column_names = list(fetched[0].keys())
            return fetched, column_names, len(fetched)
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: "SpannerAsyncConnection") -> int:
        """Resolve rowcount from Spanner cursor for the direct execution path.

        Spanner uses execute_update return value, not cursor.rowcount, so this
        returns 0.
        """
        return 0

    def _execute_kwargs(self, *, for_read: bool = False, for_batch: bool = False) -> dict[str, Any]:
        return build_execute_kwargs(
            self.driver_features, self._pending_execute_options, for_read=for_read, for_batch=for_batch
        )

    def _chunk_mutation_rows(self, columns: "list[str]", records: "list[tuple[Any, ...]]") -> "list[list[list[Any]]]":
        return chunk_mutation_rows(columns, records, self._coerce_params)

    def _resolve_database(self) -> Any:
        return cast("Any", self.connection)._session._database

    async def _batch_write_mutations(self, table: str, columns: "list[str]", chunks: "list[list[list[Any]]]") -> None:
        """High-throughput async ingest via the Spanner Batch Write API (one mutation group per chunk)."""
        database = self._resolve_database()
        async with database.mutation_groups() as mutation_groups:
            for chunk in chunks:
                group = mutation_groups.group()
                group.insert_or_update(table, columns, chunk)
            async for response in await mutation_groups.batch_write():
                status = response.status
                if status is not None and status.code:
                    msg = f"Spanner batch_write group failed: {status.message}"
                    raise SQLConversionError(msg)

    async def run_in_transaction(self, fn: "Callable[..., Awaitable[Any]]", *args: Any, **kwargs: Any) -> Any:
        """Execute a coroutine function inside Spanner's async retryable transaction runner.

        ``fn`` is awaited as ``fn(driver, *args, **kwargs)`` with a driver bound
        to the runner's transaction. A ``DeadlockError`` caused by
        ``google.api_core.exceptions.Aborted`` is unwrapped so the runner
        retries, and a terminal ``Aborted`` is raised as ``DeadlockError``.
        """
        return await run_in_transaction_async(self._resolve_database(), self._transaction_driver, fn, *args, **kwargs)

    async def _commit_before_ddl(self) -> None:
        """Commit a begun session transaction before a schema change, as DDL does on MySQL or Oracle."""
        if self._owns_transaction and isinstance(self.connection, SpannerAsyncTransaction):
            await self.commit()

    def _renew_finished_transaction(self) -> None:
        """Start a new transaction on the session once the owned transaction has finished."""
        connection = cast("Any", self.connection)
        if self._owns_transaction and (connection.committed is not None or connection.rolled_back):
            self.connection = renew_transaction(connection)

    def _transaction_driver(self, transaction: "SpannerAsyncConnection") -> "SpannerAsyncDriver":
        driver = type(self)(
            connection=transaction, statement_config=self.statement_config, driver_features=self.driver_features
        )
        driver._owns_transaction = False
        return driver

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False

    def _coerce_params(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any] | None":
        return coerce_params(
            params,
            json_serializer=self.driver_features.get("json_serializer"),
            enable_uuid_conversion=self.driver_features.get("enable_uuid_conversion", True),
        )

    def _infer_param_types(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any]":
        return infer_param_types(params)

    def _resolve_row_plan(self, fields: Any) -> "tuple[list[str], tuple[tuple[int, Any], ...] | None]":
        json_deserializer = cast("Callable[[str], Any]", self.driver_features.get("json_deserializer", from_json))
        if json_deserializer is not self._row_plan_deserializer:
            self._row_plan_cache.clear()
            self._row_plan_deserializer = json_deserializer
        return resolve_row_plan(fields, self._row_plan_cache, json_deserializer=json_deserializer)


class _SpannerResultSetProtocol(Protocol):
    metadata: Any

    def __iter__(self) -> Iterator[Any]: ...


class _SpannerReadProtocol(Protocol):
    def execute_sql(
        self,
        sql: str,
        params: "dict[str, Any] | None" = None,
        param_types: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> _SpannerResultSetProtocol: ...


class _SpannerWriteProtocol(_SpannerReadProtocol, Protocol):
    committed: "Any | None"

    def execute_update(
        self,
        sql: str,
        params: "dict[str, Any] | None" = None,
        param_types: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> int: ...

    def batch_update(
        self, batch: "list[tuple[str, dict[str, Any] | None, dict[str, Any]]]", **kwargs: Any
    ) -> "tuple[Any, list[int]]": ...

    def insert_or_update(self, table: str, columns: "list[str]", values: "list[list[Any]]") -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


class _SpannerAsyncResultSetProtocol(Protocol):
    metadata: Any

    def __aiter__(self) -> AsyncIterator[Any]: ...


class _SpannerAsyncReadProtocol(Protocol):
    async def execute_sql(
        self,
        sql: str,
        params: "dict[str, Any] | None" = None,
        param_types: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> _SpannerAsyncResultSetProtocol: ...


class _SpannerAsyncWriteProtocol(_SpannerAsyncReadProtocol, Protocol):
    committed: "Any | None"

    async def execute_update(
        self,
        sql: str,
        params: "dict[str, Any] | None" = None,
        param_types: "dict[str, Any] | None" = None,
        **kwargs: Any,
    ) -> int: ...

    async def batch_update(
        self, batch: "list[tuple[str, dict[str, Any] | None, dict[str, Any]]]", **kwargs: Any
    ) -> "tuple[Any, list[int]]": ...

    def insert_or_update(self, table: str, columns: "list[str]", values: "list[list[Any]]") -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


register_driver_profile("spanner", driver_profile)
