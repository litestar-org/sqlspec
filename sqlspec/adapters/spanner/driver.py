"""Spanner driver implementation."""

from typing import TYPE_CHECKING, Any, cast

from google.api_core import exceptions as api_exceptions

from sqlspec.adapters.spanner._type_handlers import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.adapters.spanner.type_converter import SpannerTypeConverter
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_statement_config_from_profile,
    create_arrow_result,
    register_driver_profile,
)
from sqlspec.driver import ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import (
    DatabaseConnectionError,
    NotFoundError,
    OperationalError,
    SQLConversionError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
from sqlspec.utils.serializers import from_json
from sqlspec.utils.type_guards import has_attr

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.spanner._types import SpannerConnection
    from sqlspec.core import ArrowResult, SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver import SyncDataDictionaryBase
    from sqlspec.storage import (
        StorageBridgeJob,
        StorageDestination,
        StorageFormat,
        StorageTelemetry,
        SyncStoragePipeline,
    )

__all__ = (
    "SpannerDataDictionary",
    "SpannerExceptionHandler",
    "SpannerSyncCursor",
    "SpannerSyncDriver",
    "spanner_statement_config",
)


class SpannerExceptionHandler:
    """Map Spanner client exceptions to SQLSpec exceptions."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return

        if isinstance(exc_val, api_exceptions.GoogleAPICallError):
            self._map_spanner_exception(exc_val)

    def _map_spanner_exception(self, exc: Any) -> None:
        if isinstance(exc, api_exceptions.AlreadyExists):
            msg = f"Spanner resource already exists: {exc}"
            raise UniqueViolationError(msg) from exc
        if isinstance(exc, api_exceptions.NotFound):
            msg = f"Spanner resource not found: {exc}"
            raise NotFoundError(msg) from exc
        if isinstance(exc, api_exceptions.InvalidArgument):
            msg = f"Invalid Spanner query or argument: {exc}"
            raise SQLParsingError(msg) from exc
        if isinstance(exc, api_exceptions.PermissionDenied):
            msg = f"Spanner permission denied: {exc}"
            raise DatabaseConnectionError(msg) from exc
        if isinstance(exc, (api_exceptions.ServiceUnavailable, api_exceptions.TooManyRequests)):
            msg = f"Spanner service unavailable or rate limited: {exc}"
            raise OperationalError(msg) from exc

        msg = f"Spanner error: {exc}"
        raise SQLSpecError(msg) from exc


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

    def __init__(
        self,
        connection: "SpannerConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = dict(driver_features) if driver_features else {}
        if statement_config is None:
            statement_config = spanner_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)

        json_deserializer = features.get("json_deserializer")
        self._type_converter = SpannerTypeConverter(
            enable_uuid_conversion=features.get("enable_uuid_conversion", True),
            json_deserializer=cast("Callable[[str], Any]", json_deserializer or from_json),
        )
        self._data_dictionary: SyncDataDictionaryBase | None = None

    def with_cursor(self, connection: "SpannerConnection") -> "SpannerSyncCursor":
        return SpannerSyncCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        return SpannerExceptionHandler()

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        _ = cursor
        _ = statement
        return None

    def _execute_statement(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        coerced_params = self._coerce_params(params)
        param_types_map = self._infer_param_types(coerced_params)
        conn = cast("Any", cursor)

        if statement.returns_rows():
            result_set = conn.execute_sql(sql, params=coerced_params, param_types=param_types_map)
            rows = list(result_set)
            metadata = getattr(result_set, "metadata", None)
            row_type = getattr(metadata, "row_type", None)
            fields = getattr(row_type, "fields", None)
            if fields is None:
                msg = "Result set metadata not available."
                raise SQLConversionError(msg)
            column_names = [field.name for field in fields]

            data: list[dict[str, Any]] = []
            for row in rows:
                item: dict[str, Any] = {}
                for index, column in enumerate(column_names):
                    item[column] = self._type_converter.convert_if_detected(row[index])
                data.append(item)

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        if has_attr(conn, "execute_update"):
            row_count = conn.execute_update(sql, params=coerced_params, param_types=param_types_map)
            return self.create_execution_result(cursor, rowcount_override=row_count)

        msg = "Cannot execute DML in a read-only Snapshot context."
        raise SQLConversionError(msg)

    def _execute_script(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        conn = cast("Any", cursor)

        count = 0
        for stmt in statements:
            if has_attr(conn, "execute_update") and not stmt.upper().strip().startswith("SELECT"):
                coerced_params = self._coerce_params(params)
                conn.execute_update(stmt, params=coerced_params, param_types=self._infer_param_types(coerced_params))
            else:
                coerced_params = self._coerce_params(params)
                _ = list(
                    conn.execute_sql(stmt, params=coerced_params, param_types=self._infer_param_types(coerced_params))
                )
            count += 1

        return self.create_execution_result(
            cursor, statement_count=count, successful_statements=count, is_script_result=True
        )

    def _execute_many(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        if not has_attr(cursor, "batch_update"):
            msg = "execute_many requires a Transaction context"
            raise SQLConversionError(msg)
        conn = cast("Any", cursor)

        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters or not isinstance(prepared_parameters, list):
            msg = "execute_many requires at least one parameter set"
            raise SQLConversionError(msg)

        batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
        for params in prepared_parameters:
            coerced_params = self._coerce_params(params)
            if coerced_params is None:
                coerced_params = {}
            batch_args.append((sql, coerced_params, self._infer_param_types(coerced_params)))

        _status, row_counts = conn.batch_update(batch_args)
        total_rows = sum(row_counts) if row_counts else 0

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    def _infer_param_types(self, params: "dict[str, Any] | None") -> "dict[str, Any]":
        """Infer Spanner param_types from Python values."""
        if isinstance(params, (list, tuple)):
            return {}
        return infer_spanner_param_types(params)

    def _coerce_params(self, params: "dict[str, Any] | None") -> "dict[str, Any] | None":
        """Coerce Python types to Spanner-compatible formats."""
        if isinstance(params, (list, tuple)):
            return None
        json_serializer = self.driver_features.get("json_serializer")
        return coerce_params_for_spanner(params, json_serializer=json_serializer)

    def begin(self) -> None:
        return None

    def rollback(self) -> None:
        if has_attr(self.connection, "rollback"):
            self.connection.rollback()

    def commit(self) -> None:
        if has_attr(self.connection, "commit"):
            self.connection.commit()

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        if self._data_dictionary is None:
            self._data_dictionary = SpannerDataDictionary()
        return self._data_dictionary

    def select_to_arrow(self, statement: "Any", /, *parameters: "Any", **kwargs: Any) -> "ArrowResult":
        result = self.execute(statement, *parameters, **kwargs)

        arrow_data = convert_dict_to_arrow(result.data or [], return_format=kwargs.get("return_format", "table"))
        return create_arrow_result(result.statement, arrow_data, rows_affected=result.rows_affected)

    def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, Any] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute query and stream Arrow results to storage."""
        self._require_capability("arrow_export_enabled")
        arrow_result = self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline: SyncStoragePipeline = cast("SyncStoragePipeline", self._storage_pipeline())
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
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into Spanner table via batch mutations."""
        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)

        if overwrite:
            self._truncate_table_sync(table)

        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('@p' + str(i) for i in range(len(columns)))})"
            batch_args: list[tuple[str, dict[str, Any] | None, dict[str, Any]]] = []
            for record in records:
                params = {f"p{i}": val for i, val in enumerate(record)}
                coerced = self._coerce_params(params)
                batch_args.append((insert_sql, coerced, self._infer_param_types(coerced)))

            conn = cast("Any", self.connection)
            if has_attr(conn, "batch_update"):
                conn.batch_update(batch_args)
            else:
                for batch_sql, batch_params, batch_types in batch_args:
                    conn.execute_sql(batch_sql, params=batch_params, param_types=batch_types)

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
        partitioner: "dict[str, Any] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into Spanner table."""
        arrow_table, inbound = self._read_arrow_from_storage_sync(source, file_format=file_format)
        return self.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound)

    def _truncate_table_sync(self, table: str) -> None:
        """Delete all rows from table (Spanner doesn't have TRUNCATE)."""
        delete_sql = f"DELETE FROM {table} WHERE TRUE"
        conn = cast("Any", self.connection)
        if has_attr(conn, "execute_update"):
            conn.execute_update(delete_sql)

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return False


def _build_spanner_profile() -> DriverParameterProfile:
    return DriverParameterProfile(
        name="Spanner",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={ParameterStyle.NAMED_AT},
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        json_serializer_strategy="none",
        default_dialect="spanner",
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=True,
        custom_type_coercions=None,
        extras={},
    )


_SPANNER_PROFILE = _build_spanner_profile()
register_driver_profile("spanner", _SPANNER_PROFILE)

spanner_statement_config = build_statement_config_from_profile(
    _SPANNER_PROFILE, statement_overrides={"dialect": "spanner"}
)
