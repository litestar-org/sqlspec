"""aiomysql adapter compiled helpers."""

import contextlib
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters import mysql_common
from sqlspec.adapters.aiomysql._typing import AiomysqlSSCursor as SSCursor
from sqlspec.adapters.mysql_common import (
    _bool_to_int,
    build_insert_statement,
    build_load_data_statement,
    collect_rows,
    collect_stream_rows,
    create_mapped_exception,
    detect_json_columns,
    detect_json_columns_from_description,
    encode_records_for_local_infile,
    escape_literal_percent,
    format_identifier,
    normalize_execute_many_parameters,
    normalize_execute_parameters,
    normalize_lastrowid,
    resolve_column_names,
    resolve_many_rowcount,
    resolve_row_plan,
    resolve_rowcount,
)
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_uuid_coercions

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = (
    "AiomysqlStreamSource",
    "apply_driver_features",
    "build_insert_statement",
    "build_load_data_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "collect_stream_rows",
    "create_mapped_exception",
    "default_statement_config",
    "detect_json_columns",
    "detect_json_columns_from_description",
    "driver_profile",
    "encode_records_for_local_infile",
    "escape_literal_percent",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "normalize_lastrowid",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_row_plan",
    "resolve_rowcount",
)

_MYSQL_ACCESS_ERROR_DISPATCH = mysql_common._MYSQL_ACCESS_ERROR_DISPATCH
_MYSQL_CONNECTION_ERROR_DISPATCH = mysql_common._MYSQL_CONNECTION_ERROR_DISPATCH
_MYSQL_CONSTRAINT_ERROR_DISPATCH = mysql_common._MYSQL_CONSTRAINT_ERROR_DISPATCH
_MYSQL_MIGRATION_ERROR_CODES = mysql_common._MYSQL_MIGRATION_ERROR_CODES
_MYSQL_SQLSTATE_EXACT_DISPATCH = mysql_common._MYSQL_SQLSTATE_EXACT_DISPATCH
_MYSQL_SQLSTATE_PREFIX_DISPATCH = mysql_common._MYSQL_SQLSTATE_PREFIX_DISPATCH
_MYSQL_TRANSACTION_ERROR_DISPATCH = mysql_common._MYSQL_TRANSACTION_ERROR_DISPATCH


class AiomysqlStreamSource:
    """Compiled async chunk source streaming dict rows from an aiomysql unbuffered SSCursor."""

    __slots__ = ("_chunk_size", "_cursor", "_driver", "_json_type_codes", "_parameters", "_row_plan", "_sql")

    def __init__(self, driver: Any, sql: str, parameters: Any, chunk_size: int, json_type_codes: "set[int]") -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._json_type_codes = json_type_codes
        self._row_plan: tuple[list[str], list[int] | None] | None = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        await self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    async def _start(self) -> None:
        cursor = await self._driver.connection.cursor(SSCursor)
        self._cursor = cursor
        await cursor.execute(self._sql, normalize_execute_parameters(self._parameters))
        self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows = await self._driver._run_with_exception_handler(handler, self._cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        if self._row_plan is None:
            self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)
        deserializer = cast(
            "Callable[[Any], Any]",
            getattr(self._driver, "_json_deserializer", None)
            or self._driver.driver_features.get("json_deserializer", from_json),
        )
        return collect_stream_rows(rows, self._row_plan, deserializer)

    async def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                await cursor.close()


def build_profile() -> "DriverParameterProfile":
    """Create the aiomysql driver parameter profile."""
    coercions: dict[type, Callable[[Any], Any]] = {bool: _bool_to_int, **build_uuid_coercions()}
    return DriverParameterProfile(
        name="aiomysql",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=coercions,
        default_dialect="mysql",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the aiomysql statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "mysql"}, json_serializer=serializer, json_deserializer=deserializer
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply aiomysql driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


driver_profile = build_profile()

default_statement_config = build_statement_config()
