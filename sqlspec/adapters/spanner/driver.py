"""Spanner driver implementation."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Protocol, cast

from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1.transaction import Transaction

from sqlspec.adapters.spanner._typing import SpannerSessionContext
from sqlspec.adapters.spanner.core import (
    coerce_params,
    collect_rows,
    create_arrow_data,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    infer_param_types,
    supports_batch_update,
    supports_write,
)
from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.adapters.spanner.type_converter import SpannerOutputConverter
from sqlspec.core import StatementConfig, create_arrow_result, register_driver_profile
from sqlspec.driver import ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import SQLConversionError
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.spanner._typing import SpannerConnection
    from sqlspec.core import ArrowResult
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
        self, sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> _SpannerResultSetProtocol: ...


class _SpannerWriteProtocol(_SpannerReadProtocol, Protocol):
    committed: "Any | None"

    def execute_update(
        self, sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> int: ...

    def batch_update(
        self, batch: "list[tuple[str, dict[str, Any] | None, dict[str, Any]]]"
    ) -> "tuple[Any, list[int]]": ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


class SpannerExceptionHandler:
    """Map Spanner client exceptions to SQLSpec exceptions.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __exit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "SpannerExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        _ = exc_tb
        if exc_type is None:
            return False

        if isinstance(exc_val, api_exceptions.GoogleAPICallError):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class SpannerSyncCursor:
    """Context manager that yields the active Spanner connection."""

    __slots__ = ("connection",)

    def __init__(self, connection: "SpannerConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "SpannerConnection":
        return self.connection

    def __exit__(self, *_: Any) -> None:
        return None


class SpannerSyncDriver(SyncDriverAdapterBase):
    """Synchronous Spanner driver operating on Snapshot or Transaction contexts."""

    dialect: "DialectType" = "spanner"
    __slots__ = ("_data_dictionary", "_type_converter")

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
        self._data_dictionary: SpannerDataDictionary | None = None

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
            result_set = reader.execute_sql(sql, params=coerced_params, param_types=param_types_map)
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
            data, column_names = collect_rows(rows, fields, self._type_converter)
            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        if supports_write(cursor):
            writer = cast("_SpannerWriteProtocol", cursor)
            row_count = writer.execute_update(sql, params=coerced_params, param_types=param_types_map)
            return self.create_execution_result(cursor, rowcount_override=row_count)

        msg = "Cannot execute DML in a read-only Snapshot context."
        raise SQLConversionError(msg)

    def dispatch_execute_many(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        if not supports_batch_update(cursor):
            msg = "execute_many requires a Transaction context"
            raise SQLConversionError(msg)

        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters or not isinstance(prepared_parameters, list):
            msg = "execute_many requires at least one parameter set"
            raise SQLConversionError(msg)

        batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
        for params in prepared_parameters:
            coerced_params = self._coerce_params(cast("dict[str, Any] | None", params))
            if coerced_params is None:
                coerced_params = {}
            batch_args.append((sql, coerced_params, self._infer_param_types(coerced_params)))

        writer = cast("_SpannerWriteProtocol", cursor)
        _status, row_counts = writer.batch_update(batch_args)
        total_rows = sum(row_counts) if row_counts else 0

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    def dispatch_execute_script(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        is_transaction = supports_write(cursor)
        reader = cast("_SpannerReadProtocol", cursor)

        count = 0
        script_params = cast("dict[str, Any] | None", params)
        for stmt in statements:
            is_select = stmt.upper().strip().startswith("SELECT")
            coerced_params = self._coerce_params(script_params)
            if not is_select and not is_transaction:
                msg = "Cannot execute DML in a read-only Snapshot context."
                raise SQLConversionError(msg)
            if not is_select and is_transaction:
                writer = cast("_SpannerWriteProtocol", cursor)
                writer.execute_update(stmt, params=coerced_params, param_types=self._infer_param_types(coerced_params))
            else:
                _ = list(
                    reader.execute_sql(stmt, params=coerced_params, param_types=self._infer_param_types(coerced_params))
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

    # ─────────────────────────────────────────────────────────────────────────────
    # ARROW API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def select_to_arrow(self, statement: "Any", /, *parameters: "Any", **kwargs: Any) -> "ArrowResult":
        result = self.execute(statement, *parameters, **kwargs)

        return_format = cast("ArrowReturnFormat", kwargs.get("return_format", "table"))
        arrow_data = create_arrow_data(result.data or [], return_format)
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
            for record in records:
                params = {f"p{i}": val for i, val in enumerate(record)}
                coerced = self._coerce_params(params)
                batch_args.append((insert_sql, coerced, self._infer_param_types(coerced)))

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

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False

    def _coerce_params(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any] | None":
        return coerce_params(params, json_serializer=self.driver_features.get("json_serializer"))

    def _infer_param_types(self, params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any]":
        return infer_param_types(params)


register_driver_profile("spanner", driver_profile)
