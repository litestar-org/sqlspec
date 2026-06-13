"""Spanner driver implementation."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast

import sqlglot as _sqlglot
from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1.keyset import KeySet
from google.cloud.spanner_v1.transaction import Transaction
from sqlglot import exp as _sqlglot_exp

from sqlspec.adapters.spanner._typing import SpannerSessionContext, SpannerSyncCursor
from sqlspec.adapters.spanner.core import (
    build_param_type_signature,
    coerce_params,
    collect_rows,
    create_arrow_data,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    infer_param_types,
    resolve_column_names,
    supports_batch_update,
    supports_write,
)
from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.adapters.spanner.type_converter import SpannerOutputConverter
from sqlspec.core import StatementConfig, create_arrow_result, register_driver_profile
from sqlspec.driver import BaseSyncExceptionHandler, ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError, SQLConversionError
from sqlspec.utils.serializers import from_json

_READ_ONLY_SNAPSHOT_ERROR_MESSAGE = (
    "Cannot execute DML in a read-only Snapshot context. "
    "SpannerSyncConfig.provide_session() opens a write-capable Transaction by default; "
    "the current session must have been opened via SpannerSyncConfig.provide_read_session()."
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.spanner._typing import SpannerConnection
    from sqlspec.core import ArrowResult, SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat

__all__ = (
    "SpannerDataDictionary",
    "SpannerExceptionHandler",
    "SpannerSessionContext",
    "SpannerSyncCursor",
    "SpannerSyncDriver",
)


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

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


class SpannerExceptionHandler(BaseSyncExceptionHandler):
    """Map Spanner client exceptions to SQLSpec exceptions.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ()

    def _handle_exception(self, exc_type: "type[BaseException] | None", exc_val: "BaseException") -> bool:
        if exc_type is None:
            return False

        if isinstance(exc_val, api_exceptions.GoogleAPICallError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class _PerCallExecuteOptions:
    """Per-call Spanner execution options captured for a single dispatch."""

    __slots__ = ("directed_read_options", "request_options", "retry", "timeout")

    def __init__(
        self,
        *,
        request_options: "Any | None" = None,
        directed_read_options: "Any | None" = None,
        retry: "Any | None" = None,
        timeout: "float | None" = None,
    ) -> None:
        self.request_options = request_options
        self.directed_read_options = directed_read_options
        self.retry = retry
        self.timeout = timeout


class SpannerSyncDriver(SyncDriverAdapterBase):
    """Synchronous Spanner driver operating on Snapshot or Transaction contexts."""

    dialect: "DialectType" = "spanner"
    __slots__ = ("_column_name_cache", "_data_dictionary", "_pending_execute_options", "_type_converter")

    def __init__(
        self,
        connection: "SpannerConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = dict(driver_features) if driver_features else {}
        if statement_config is None:
            statement_config = default_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)

        json_deserializer = features.get("json_deserializer")
        self._type_converter = SpannerOutputConverter(
            enable_uuid_conversion=features.get("enable_uuid_conversion", True),
            json_deserializer=cast("Callable[[str], Any]", json_deserializer or from_json),
        )
        self._column_name_cache: dict[int, tuple[Any, list[str]]] = {}
        self._data_dictionary: SpannerDataDictionary | None = None
        self._pending_execute_options: _PerCallExecuteOptions | None = None

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS - The Execution Engine
    # ─────────────────────────────────────────────────────────────────────────────

    def dispatch_execute(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        params = cast("dict[str, Any] | None", params)
        coerced_params = self._coerce_params(params)
        param_types_map = self._infer_param_types(coerced_params)

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
            column_names = self._resolve_column_names(fields)
            data, column_names = collect_rows(rows, fields, self._type_converter, column_names=column_names)
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

    def dispatch_execute_many(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        if not supports_batch_update(cursor):
            msg = "execute_many requires a Transaction context"
            raise SQLConversionError(msg)

        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters or not isinstance(prepared_parameters, list):
            msg = "execute_many requires at least one parameter set"
            raise SQLConversionError(msg)

        _coerce = self._coerce_params
        _infer = self._infer_param_types
        execute_kwargs = self._execute_kwargs()
        param_types_cache: dict[tuple[tuple[str, type[Any]], ...], dict[str, Any]] = {}
        empty_param_types: dict[str, Any] = {}
        batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
        append_batch_arg = batch_args.append
        for params in prepared_parameters:
            coerced_params = _coerce(cast("dict[str, Any] | None", params))
            if not coerced_params:
                append_batch_arg((sql, {}, empty_param_types))
                continue
            signature = build_param_type_signature(coerced_params)
            param_types = param_types_cache.get(signature)
            if param_types is None:
                param_types = _infer(coerced_params)
                param_types_cache[signature] = param_types
            append_batch_arg((sql, coerced_params, param_types))

        writer = cast("_SpannerWriteProtocol", cursor)
        _status, row_counts = writer.batch_update(batch_args, **execute_kwargs)
        total_rows = sum(row_counts) if row_counts else 0

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        is_transaction = supports_write(cursor)
        reader = cast("_SpannerReadProtocol", cursor)

        count = 0
        script_params = cast("dict[str, Any] | None", params)
        coerced_params = self._coerce_params(script_params)
        param_types_map = self._infer_param_types(coerced_params)
        read_execute_kwargs = self._execute_kwargs(for_read=True)
        write_execute_kwargs = self._execute_kwargs()
        for stmt in statements:
            try:
                parsed = _sqlglot.parse_one(stmt)
                is_select = isinstance(parsed, _sqlglot_exp.Select)
            except Exception:
                is_select = stmt.upper().strip().startswith("SELECT")
            if not is_select and not is_transaction:
                raise SQLConversionError(_READ_ONLY_SNAPSHOT_ERROR_MESSAGE)
            if not is_select and is_transaction:
                writer = cast("_SpannerWriteProtocol", cursor)
                writer.execute_update(stmt, params=coerced_params, param_types=param_types_map, **write_execute_kwargs)
            else:
                _ = list(
                    reader.execute_sql(stmt, params=coerced_params, param_types=param_types_map, **read_execute_kwargs)
                )
            count += 1

        return self.create_execution_result(
            cursor, statement_count=count, successful_statements=count, is_script_result=True
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    def begin(self) -> None:
        return None

    def commit(self) -> None:
        if isinstance(self.connection, Transaction):
            writer = cast("_SpannerWriteProtocol", self.connection)
            if writer.committed is not None:
                return
            writer.commit()

    def rollback(self) -> None:
        if isinstance(self.connection, Transaction):
            writer = cast("_SpannerWriteProtocol", self.connection)
            writer.rollback()

    def with_cursor(self, connection: "SpannerConnection") -> "SpannerSyncCursor":
        return SpannerSyncCursor(connection)

    def handle_database_exceptions(self) -> "SpannerExceptionHandler":
        return SpannerExceptionHandler()

    def _execute_kwargs(self, *, for_read: bool = False) -> dict[str, Any]:
        kwargs = {key: self.driver_features[key] for key in ("retry", "timeout") if key in self.driver_features}
        request_options = self.driver_features.get("request_options")
        if request_options is not None:
            kwargs["request_options"] = request_options
        pending = self._pending_execute_options
        if pending is not None:
            if pending.request_options is not None:
                kwargs["request_options"] = pending.request_options
            if pending.retry is not None:
                kwargs["retry"] = pending.retry
            if pending.timeout is not None:
                kwargs["timeout"] = pending.timeout
            if for_read and pending.directed_read_options is not None:
                kwargs["directed_read_options"] = pending.directed_read_options
        return kwargs

    def execute_with_options(
        self,
        statement: "Any",
        /,
        *parameters: "Any",
        request_options: "Any | None" = None,
        directed_read_options: "Any | None" = None,
        retry: "Any | None" = None,
        timeout: "float | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a single statement with per-call Spanner request options."""
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        self._pending_execute_options = _PerCallExecuteOptions(
            request_options=request_options, directed_read_options=directed_read_options, retry=retry, timeout=timeout
        )
        try:
            return self.dispatch_statement_execution(statement=sql_statement, connection=self.connection)
        finally:
            self._pending_execute_options = None

    def _require_database(self) -> "Any":
        provider = self.driver_features.get("database_provider")
        if provider is None:
            msg = "Spanner database-level operations require a session created via SpannerSyncConfig.provide_session()."
            raise ImproperConfigurationError(msg)
        return provider()

    def execute_partitioned_dml(
        self,
        statement: "Any",
        /,
        *parameters: "Any",
        request_options: "Any | None" = None,
        exclude_txn_from_change_streams: bool = False,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> int:
        """Execute a partitioned DML statement across the whole table."""
        database = self._require_database()
        config = statement_config or self.statement_config
        sql_statement = self.prepare_statement(statement, parameters, statement_config=config, kwargs=kwargs)
        sql, params = self._get_compiled_sql(sql_statement, config)
        coerced_params = self._coerce_params(cast("dict[str, Any] | None", params))
        param_types_map = self._infer_param_types(coerced_params)
        exc_handler = self.handle_database_exceptions()
        row_count = 0
        with exc_handler:
            row_count = int(
                database.execute_partitioned_dml(
                    sql,
                    params=coerced_params,
                    param_types=param_types_map,
                    request_options=request_options
                    if request_options is not None
                    else self.driver_features.get("request_options"),
                    exclude_txn_from_change_streams=exclude_txn_from_change_streams,
                )
            )
        self._check_pending_exception(exc_handler)
        return row_count

    def apply_mutations(
        self,
        table: str,
        *,
        columns: "Sequence[str] | None" = None,
        insert: "Sequence[Sequence[Any]] | None" = None,
        update: "Sequence[Sequence[Any]] | None" = None,
        insert_or_update: "Sequence[Sequence[Any]] | None" = None,
        replace: "Sequence[Sequence[Any]] | None" = None,
        delete_keys: "Sequence[Sequence[Any]] | None" = None,
        delete_all: bool = False,
        request_options: "Any | None" = None,
        max_commit_delay: "Any | None" = None,
    ) -> None:
        """Apply blind-write mutations to a table in a single atomic commit."""
        row_groups = (insert, update, insert_or_update, replace)
        if any(group is not None for group in row_groups) and columns is None:
            msg = "apply_mutations() requires 'columns' when row mutations are provided."
            raise ImproperConfigurationError(msg)

        database = self._require_database()
        resolved_request_options = (
            request_options if request_options is not None else self.driver_features.get("request_options")
        )
        exc_handler = self.handle_database_exceptions()
        with (
            exc_handler,
            database.batch(request_options=resolved_request_options, max_commit_delay=max_commit_delay) as batch,
        ):
            if insert is not None:
                batch.insert(table, columns, insert)
            if update is not None:
                batch.update(table, columns, update)
            if insert_or_update is not None:
                batch.insert_or_update(table, columns, insert_or_update)
            if replace is not None:
                batch.replace(table, columns, replace)
            if delete_all:
                batch.delete(table, KeySet(all_=True))  # type: ignore[no-untyped-call]
            elif delete_keys is not None:
                batch.delete(table, KeySet(keys=list(delete_keys)))  # type: ignore[no-untyped-call]
        self._check_pending_exception(exc_handler)

    # ─────────────────────────────────────────────────────────────────────────────
    # ARROW API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def select_to_arrow(self, statement: "Any", /, *parameters: "Any", **kwargs: Any) -> "ArrowResult":
        result = self.execute(statement, *parameters, **kwargs)

        return_format = cast("ArrowReturnFormat", kwargs.get("return_format", "table"))
        arrow_data = create_arrow_data(result.get_data(), return_format)
        return create_arrow_result(result.statement, arrow_data, rows_affected=result.rows_affected)

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
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
        """Execute query and stream Arrow results to storage."""
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
        """Load Arrow data into Spanner table via batch mutations."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)

        if overwrite:
            delete_sql = f"DELETE FROM {table} WHERE TRUE"
            if isinstance(self.connection, Transaction):
                writer = cast("_SpannerWriteProtocol", self.connection)
                writer.execute_update(delete_sql)
            else:
                msg = "Delete requires a Transaction context."
                raise SQLConversionError(msg)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('@p' + str(i) for i in range(len(columns)))})"
            batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
            param_types_cache: dict[tuple[tuple[str, type[Any]], ...], dict[str, Any]] = {}
            empty_param_types: dict[str, Any] = {}
            for record in records:
                params = {f"p{i}": val for i, val in enumerate(record)}
                coerced = self._coerce_params(params)
                if not coerced:
                    batch_args.append((insert_sql, {}, empty_param_types))
                    continue
                signature = build_param_type_signature(coerced)
                param_types = param_types_cache.get(signature)
                if param_types is None:
                    param_types = self._infer_param_types(coerced)
                    param_types_cache[signature] = param_types
                batch_args.append((insert_sql, coerced, param_types))

            conn = self.connection
            if not isinstance(conn, Transaction):
                msg = "Arrow import requires a Transaction context."
                raise SQLConversionError(msg)
            writer = cast("_SpannerWriteProtocol", conn)
            writer.batch_update(batch_args)

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
        """Load artifacts from storage into Spanner table."""
        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "SpannerDataDictionary":
        if self._data_dictionary is None:
            self._data_dictionary = SpannerDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def collect_rows(self, cursor: "SpannerConnection", fetched: "list[Any]") -> "tuple[list[Any], list[str], int]":
        """Collect Spanner rows for the direct execution path.

        Note: Spanner's collect_rows requires result set fields and a type converter.
        The direct execution path may not always have this metadata available,
        so this falls back to basic collection.
        """
        # For direct path, we need fields metadata from the result set.
        # If not available, return raw data with no column names.
        if not fetched:
            return [], [], 0
        # Attempt to extract column names from dict keys if rows are dicts
        if isinstance(fetched[0], dict):
            column_names = list(fetched[0].keys())
            return fetched, column_names, len(fetched)
        # For tuple rows without metadata, return as-is
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: "SpannerConnection") -> int:
        """Resolve rowcount from Spanner cursor for the direct execution path."""
        # Spanner uses execute_update return value, not cursor.rowcount
        return 0

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False

    def _coerce_params(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any] | None":
        return coerce_params(params, json_serializer=self.driver_features.get("json_serializer"))

    def _infer_param_types(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any]":
        return infer_param_types(params)

    def _resolve_column_names(self, fields: Any) -> list[str]:
        return resolve_column_names(fields, self._column_name_cache)


register_driver_profile("spanner", driver_profile)
