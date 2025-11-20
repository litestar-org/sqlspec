"""Spanner driver implementation."""

from typing import TYPE_CHECKING, Any, cast

from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1 import param_types

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
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.spanner._types import SpannerConnection
    from sqlspec.core import ArrowResult, SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver import SyncDataDictionaryBase

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
        param_types_map = self._infer_param_types(params)
        conn = cast("Any", cursor)

        if statement.returns_rows():
            result_set = conn.execute_sql(sql, params=params, param_types=param_types_map)
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

        if hasattr(conn, "execute_update"):
            row_count = conn.execute_update(sql, params=params, param_types=param_types_map)
            return self.create_execution_result(cursor, rowcount_override=row_count)

        msg = "Cannot execute DML in a read-only Snapshot context."
        raise SQLConversionError(msg)

    def _execute_script(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        conn = cast("Any", cursor)

        count = 0
        for stmt in statements:
            if hasattr(conn, "execute_update") and not stmt.upper().strip().startswith("SELECT"):
                conn.execute_update(stmt, params=params)
            else:
                _ = list(conn.execute_sql(stmt, params=params))
            count += 1

        return self.create_execution_result(
            cursor, statement_count=count, successful_statements=count, is_script_result=True
        )

    def _execute_many(self, cursor: "SpannerConnection", statement: "SQL") -> ExecutionResult:
        if not hasattr(cursor, "batch_update"):
            msg = "execute_many requires a Transaction context"
            raise SQLConversionError(msg)
        conn = cast("Any", cursor)

        parameter_sets = statement.parameters if isinstance(statement.parameters, list) else []
        if not parameter_sets:
            msg = "execute_many requires at least one parameter set"
            raise SQLConversionError(msg)

        base_params = parameter_sets[0]
        base_statement = self.prepare_statement(
            statement.raw_sql, *[base_params], statement_config=statement.statement_config
        )
        compiled_sql, _ = self._get_compiled_sql(base_statement, self.statement_config)

        batch_inputs: list[dict[str, Any]] = []
        for params in parameter_sets:
            per_statement = self.prepare_statement(
                statement.raw_sql, *[params], statement_config=statement.statement_config
            )
            _, processed_params = self._get_compiled_sql(per_statement, self.statement_config)
            batch_inputs.append(processed_params)

        batch_args = [(compiled_sql, p, self._infer_param_types(p)) for p in batch_inputs]

        row_counts = conn.batch_update(batch_args)
        total_rows = int(sum(int(count) for count in row_counts))

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    def _infer_param_types(self, params: "dict[str, Any] | None") -> "dict[str, Any]":
        if not params:
            return {}

        types: dict[str, Any] = {}
        for key, value in params.items():
            if isinstance(value, bool):
                types[key] = param_types.BOOL
            elif isinstance(value, int):
                types[key] = param_types.INT64
            elif isinstance(value, float):
                types[key] = param_types.FLOAT64
            elif isinstance(value, str):
                types[key] = param_types.STRING
            elif isinstance(value, bytes):
                types[key] = param_types.BYTES
            elif isinstance(value, list):
                if not value:
                    continue
                first_value = value[0]
                if isinstance(first_value, int):
                    types[key] = cast("Any", param_types.Array(param_types.INT64))  # type: ignore[no-untyped-call]
                elif isinstance(first_value, str):
                    types[key] = cast("Any", param_types.Array(param_types.STRING))  # type: ignore[no-untyped-call]
                elif isinstance(first_value, float):
                    types[key] = cast("Any", param_types.Array(param_types.FLOAT64))  # type: ignore[no-untyped-call]
                elif isinstance(first_value, bool):
                    types[key] = cast("Any", param_types.Array(param_types.BOOL))  # type: ignore[no-untyped-call]
            elif isinstance(value, dict) and hasattr(param_types, "JSON"):
                types[key] = param_types.JSON

            if key not in types and hasattr(param_types, "JSON") and isinstance(value, (dict, list)):
                types[key] = param_types.JSON
        return types

    def begin(self) -> None:
        return None

    def rollback(self) -> None:
        if hasattr(self.connection, "rollback"):
            self.connection.rollback()

    def commit(self) -> None:
        if hasattr(self.connection, "commit"):
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


def _build_spanner_profile() -> DriverParameterProfile:
    type_coercions: dict[type, Any] = {dict: to_json, list: to_json, tuple: to_json}
    return DriverParameterProfile(
        name="Spanner",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={ParameterStyle.NAMED_AT},
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        json_serializer_strategy="helper",
        default_dialect="spanner",
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=True,
        custom_type_coercions=type_coercions,
        extras={"type_coercion_overrides": type_coercions},
    )


_SPANNER_PROFILE = _build_spanner_profile()
register_driver_profile("spanner", _SPANNER_PROFILE)

spanner_statement_config = build_statement_config_from_profile(
    _SPANNER_PROFILE, statement_overrides={"dialect": "spanner"}, json_serializer=to_json
)
