"""Spanner driver implementation."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast

import sqlglot as _sqlglot
from google.api_core import exceptions as api_exceptions
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
from sqlspec.exceptions import SQLConversionError
from sqlspec.utils.serializers import from_json

_READ_ONLY_SNAPSHOT_ERROR_MESSAGE = (
    "Cannot execute DML in a read-only Snapshot context. "
    "SpannerSyncConfig.provide_session() opens a write-capable Transaction by default; "
    "the current session must have been opened via SpannerSyncConfig.provide_read_session()."
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from google.api_core.retry import Retry
    from google.cloud.spanner_v1 import DirectedReadOptions, RequestOptions
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.spanner._typing import SpannerConnection
    from sqlspec.builder import QueryBuilder
    from sqlspec.core import ArrowResult, SQLResult, Statement, StatementFilter
    from sqlspec.core.statement import SQL
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, StatementParameters

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

    def insert_or_update(self, table: str, columns: "list[str]", values: "list[list[Any]]") -> None: ...

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
        request_options: "RequestOptions | dict[str, Any] | None" = None,
        directed_read_options: "DirectedReadOptions | None" = None,
        retry: "Retry | None" = None,
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

    def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with optional Spanner per-call request options."""
        execute_options = self._pop_execute_options(kwargs)
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
        execute_options = self._pop_execute_options(kwargs)
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
        execute_options = self._pop_execute_options(kwargs)
        if execute_options is None:
            return super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)
        previous_options = self._pending_execute_options
        self._pending_execute_options = execute_options
        try:
            return super().execute_script(statement, *parameters, statement_config=statement_config, **kwargs)
        finally:
            self._pending_execute_options = previous_options

    def _execute_kwargs(self, *, for_read: bool = False) -> dict[str, Any]:
        kwargs = {key: self.driver_features[key] for key in ("retry", "timeout") if key in self.driver_features}
        request_options = self.driver_features.get("request_options")
        if request_options is not None:
            kwargs["request_options"] = request_options
        directed_read_options = self.driver_features.get("directed_read_options")
        if for_read and directed_read_options is not None:
            kwargs["directed_read_options"] = directed_read_options
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

    def _pop_execute_options(self, kwargs: dict[str, Any]) -> "_PerCallExecuteOptions | None":
        if not any(key in kwargs for key in ("request_options", "directed_read_options", "retry", "timeout")):
            return None
        return _PerCallExecuteOptions(
            request_options=kwargs.pop("request_options", None),
            directed_read_options=kwargs.pop("directed_read_options", None),
            retry=kwargs.pop("retry", None),
            timeout=kwargs.pop("timeout", None),
        )

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
            conn = self.connection
            if not isinstance(conn, Transaction):
                msg = "Arrow import requires a Transaction context."
                raise SQLConversionError(msg)
            chunks = self._chunk_mutation_rows(columns, records)
            if self.driver_features.get("enable_batch_write_api"):
                self._batch_write_mutations(table, columns, chunks)
            else:
                writer = cast("_SpannerWriteProtocol", conn)
                for chunk in chunks:
                    writer.insert_or_update(table, columns, chunk)

        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    def _chunk_mutation_rows(self, columns: "list[str]", records: "list[tuple[Any, ...]]") -> "list[list[list[Any]]]":
        """Coerce Arrow rows into mutation value chunks bounded by Spanner's per-commit cell ceiling."""
        column_count = len(columns)
        max_cells = 20_000
        chunks: list[list[list[Any]]] = []
        values: list[list[Any]] = []
        pending_cells = 0
        for record in records:
            coerced = self._coerce_params({f"p{i}": value for i, value in enumerate(record)}) or {}
            values.append([coerced.get(f"p{i}") for i in range(column_count)])
            pending_cells += column_count
            if pending_cells >= max_cells:
                chunks.append(values)
                values = []
                pending_cells = 0
        if values:
            chunks.append(values)
        return chunks

    def _batch_write_mutations(self, table: str, columns: "list[str]", chunks: "list[list[list[Any]]]") -> None:
        """High-throughput ingest via the Spanner Batch Write API (one mutation group per chunk)."""
        session = getattr(self.connection, "_session", None)
        database = getattr(session, "_database", None)
        if database is None:
            msg = "Spanner Batch Write API requires a database-backed session."
            raise SQLConversionError(msg)
        mutation_groups = database.mutation_groups()
        for chunk in chunks:
            group = mutation_groups.group()
            group.insert_or_update(table, columns, chunk)
        for response in mutation_groups.batch_write():
            status = response.status
            if status is not None and status.code:
                msg = f"Spanner batch_write group failed: {status.message}"
                raise SQLConversionError(msg)

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
