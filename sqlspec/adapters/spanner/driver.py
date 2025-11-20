"""Spanner driver implementation."""

import logging
from typing import TYPE_CHECKING, Any

from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1 import param_types

from sqlspec.adapters.spanner._types import SpannerConnection
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
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from sqlspec.core import SQLResult
    from sqlspec.core.statement import SQL

logger = logging.getLogger(__name__)

__all__ = ("SpannerCursor", "SpannerExceptionHandler", "SpannerSyncDriver", "spanner_statement_config")


class SpannerCursor:
    """Spanner cursor context manager.

    Spanner doesn't have a traditional cursor, but we use this to manage
    context if needed (e.g., transaction lifecycle).
    For now, it just passes through the connection (Snapshot/Transaction).
    """

    __slots__ = ("connection",)

    def __init__(self, connection: SpannerConnection) -> None:
        self.connection = connection

    def __enter__(self) -> SpannerConnection:
        return self.connection

    def __exit__(self, *_: Any) -> None:
        pass


class SpannerExceptionHandler:
    """Context manager for handling Spanner exceptions."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        if isinstance(exc_val, api_exceptions.GoogleAPICallError):
            self._map_spanner_exception(exc_val)

    def _map_spanner_exception(self, e: Any) -> None:
        if isinstance(e, api_exceptions.AlreadyExists):
            msg = f"Spanner resource already exists: {e}"
            raise UniqueViolationError(msg) from e
        if isinstance(e, api_exceptions.NotFound):
            msg = f"Spanner resource not found: {e}"
            raise NotFoundError(msg) from e
        if isinstance(e, api_exceptions.InvalidArgument):
            msg = f"Invalid Spanner query/argument: {e}"
            raise SQLParsingError(msg) from e
        if isinstance(e, api_exceptions.PermissionDenied):
            msg = f"Spanner permission denied: {e}"
            raise DatabaseConnectionError(msg) from e
        if isinstance(e, (api_exceptions.ServiceUnavailable, api_exceptions.TooManyRequests)):
            raise OperationalError(f"Spanner service unavailable/rate limited: {e}") from e
        msg = f"Spanner error: {e}"
        raise SQLSpecError(msg) from e


class SpannerSyncDriver(SyncDriverAdapterBase):
    """Spanner Synchronous Driver.

    Operates within a specific Spanner Snapshot or Transaction context.
    """

    dialect: str = "spanner"

    def __init__(
        self,
        connection: SpannerConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        features = driver_features or {}

        if statement_config is None:
            statement_config = spanner_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

        self._type_converter = SpannerTypeConverter(
            enable_uuid_conversion=features.get("enable_uuid_conversion", True),
            json_deserializer=features.get("json_deserializer"),
        )
        self._data_dictionary: Any = None

    def with_cursor(self, connection: SpannerConnection) -> SpannerCursor:
        return SpannerCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        return SpannerExceptionHandler()

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
        return None

    def _execute_statement(self, cursor: SpannerConnection, statement: "SQL") -> ExecutionResult:
        """Execute single SQL statement."""
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        param_types_map = self._infer_param_types(params)

        try:
            if statement.returns_rows():
                result_set = cursor.execute_sql(sql, params=params, param_types=param_types_map)

                rows = list(result_set)
                fields = result_set.metadata.row_type.fields
                column_names = [f.name for f in fields]

                data = []
                for row in rows:
                    item = {}
                    for i, col in enumerate(column_names):
                        val = row[i]
                        item[col] = self._type_converter.convert_if_detected(val)
                    data.append(item)

                return self.create_execution_result(
                    cursor,
                    selected_data=data,
                    column_names=column_names,
                    data_row_count=len(data),
                    is_select_result=True,
                )
            if hasattr(cursor, "execute_update"):
                row_count = cursor.execute_update(sql, params=params, param_types=param_types_map)
                return self.create_execution_result(cursor, rowcount_override=row_count)
            msg = "Cannot execute DML in a Read-Only Snapshot context."
            raise SQLConversionError(msg)

        except Exception:
            raise

    def _execute_script(self, cursor: SpannerConnection, statement: "SQL") -> ExecutionResult:
        """Execute script."""
        sql, params = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        count = 0
        for stmt in statements:
            if hasattr(cursor, "execute_update") and not stmt.upper().strip().startswith("SELECT"):
                cursor.execute_update(stmt, params=params)
            else:
                list(cursor.execute_sql(stmt, params=params))
            count += 1

        return self.create_execution_result(
            cursor, statement_count=count, successful_statements=count, is_script_result=True
        )

    def _execute_many(self, cursor: SpannerConnection, statement: "SQL") -> ExecutionResult:
        """Execute batch DML."""
        if not hasattr(cursor, "batch_update"):
            raise SQLConversionError("execute_many requires a Transaction context")

        first_params = statement.parameters[0] if statement.parameters else {}
        compiled_sql, _ = self._get_compiled_sql(statement.replace(parameters=first_params), self.statement_config)

        batch_inputs = []
        for p in statement.parameters:
            _, processed_params = self._get_compiled_sql(statement.replace(parameters=p), self.statement_config)
            batch_inputs.append(processed_params)

        batch_args = [(compiled_sql, p, self._infer_param_types(p)) for p in batch_inputs]

        row_counts = cursor.batch_update(batch_args)
        total_rows = sum(row_counts)

        return self.create_execution_result(cursor, rowcount_override=total_rows, is_many_result=True)

    def _infer_param_types(self, params: dict[str, Any] | None) -> dict[str, Any]:
        """Infer Spanner param types."""
        if not params:
            return {}

        types = {}
        for k, v in params.items():
            if isinstance(v, bool):
                types[k] = param_types.BOOL
            elif isinstance(v, int):
                types[k] = param_types.INT64
            elif isinstance(v, float):
                types[k] = param_types.FLOAT64
            elif isinstance(v, bytes):
                types[k] = param_types.BYTES
            elif isinstance(v, list):
                if v and isinstance(v[0], int):
                    types[k] = param_types.Array(param_types.INT64)
                elif v and isinstance(v[0], str):
                    types[k] = param_types.Array(param_types.STRING)
        return types

    def begin(self) -> None:
        pass

    def rollback(self) -> None:
        if hasattr(self.connection, "rollback"):
            self.connection.rollback()

    def commit(self) -> None:
        if hasattr(self.connection, "commit"):
            self.connection.commit()

    @property
    def data_dictionary(self) -> Any:
        if self._data_dictionary is None:
            from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary

            self._data_dictionary = SpannerDataDictionary(self)  # type: ignore
        return self._data_dictionary

    def select_to_arrow(self, statement: "Any", /, *parameters: "Any", **kwargs: Any) -> "Any":
        """Execute query and convert results to Arrow format."""
        result = self.select(statement, *parameters, **kwargs)
        from sqlspec.utils.arrow_helpers import convert_dict_to_arrow

        arrow_data = convert_dict_to_arrow(
            result,  # type: ignore
            return_format=kwargs.get("return_format", "table"),
        )
        return create_arrow_result(arrow_data, rows_affected=len(result))  # type: ignore


def _build_spanner_profile() -> DriverParameterProfile:
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
    )


_SPANNER_PROFILE = _build_spanner_profile()
register_driver_profile("spanner", _SPANNER_PROFILE)

spanner_statement_config = build_statement_config_from_profile(
    _SPANNER_PROFILE, statement_overrides={"dialect": "spanner"}, json_serializer=to_json
)
