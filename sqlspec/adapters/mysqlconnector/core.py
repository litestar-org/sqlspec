"""MysqlConnector adapter compiled helpers."""

import contextlib
from typing import TYPE_CHECKING, Any, cast

from mysql.connector.conversion import MySQLConverter

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
    "MysqlConnectorAsyncStreamSource",
    "MysqlConnectorSyncStreamSource",
    "SQLSpecMySQLConverter",
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


class SQLSpecMySQLConverter(MySQLConverter):
    """Custom converter that deserializes JSON columns directly during packet decoding."""

    def __init__(self, *args: Any, deserializer: "Callable[[Any], Any]" = from_json, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._deserializer = deserializer

    def _json_to_python(self, value: Any, dsc: Any = None) -> Any:
        raw = super()._json_to_python(value, dsc)
        if raw is not None and isinstance(raw, (str, bytes)):
            try:
                return self._deserializer(raw)
            except Exception:
                return raw
        return raw


class MysqlConnectorSyncStreamSource:
    """Compiled chunk source streaming dict rows from an unbuffered mysql-connector cursor."""

    __slots__ = (
        "_chunk_size",
        "_cursor",
        "_cursor_options",
        "_driver",
        "_json_type_codes",
        "_parameters",
        "_row_plan",
        "_sql",
    )

    def __init__(
        self,
        driver: Any,
        sql: str,
        parameters: Any,
        chunk_size: int,
        json_type_codes: "set[int]",
        cursor_options: "dict[str, Any] | None" = None,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._json_type_codes = json_type_codes
        self._cursor_options = dict(cursor_options or {})
        self._cursor_options["buffered"] = False
        self._row_plan: tuple[list[str], list[int] | None] | None = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        with handler:
            cursor = self._driver.connection.cursor(**self._cursor_options)
            self._cursor = cursor
            cursor.execute(self._sql, normalize_execute_parameters(self._parameters))
            self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows: list[Any] = []
        with handler:
            rows = self._cursor.fetchmany(self._chunk_size)
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

    def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        connection = self._driver.connection
        raw_connection = getattr(connection, "_cnx", None) or connection
        try:
            if getattr(raw_connection, "unread_result", False):
                raw_connection.consume_results()
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()


class MysqlConnectorAsyncStreamSource:
    """Compiled async chunk source streaming dict rows from an unbuffered mysql-connector cursor."""

    __slots__ = (
        "_chunk_size",
        "_cursor",
        "_cursor_options",
        "_driver",
        "_json_type_codes",
        "_parameters",
        "_row_plan",
        "_sql",
    )

    def __init__(
        self,
        driver: Any,
        sql: str,
        parameters: Any,
        chunk_size: int,
        json_type_codes: "set[int]",
        cursor_options: "dict[str, Any] | None" = None,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._json_type_codes = json_type_codes
        self._cursor_options = dict(cursor_options or {})
        self._cursor_options["buffered"] = False
        self._row_plan: tuple[list[str], list[int] | None] | None = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        await self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    async def _start(self) -> None:
        cursor = await self._driver.connection.cursor(**self._cursor_options)
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
        connection = self._driver.connection
        raw_connection = getattr(connection, "_cnx", None) or connection
        try:
            if getattr(raw_connection, "unread_result", False):
                await raw_connection.consume_results()
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    await cursor.close()


def build_profile() -> "DriverParameterProfile":
    """Create the mysql-connector driver parameter profile."""
    coercions: dict[type, Callable[[Any], Any]] = {bool: _bool_to_int, **build_uuid_coercions()}
    return DriverParameterProfile(
        name="mysql-connector",
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
    """Construct the mysql-connector statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "mysql"}, json_serializer=serializer, json_deserializer=deserializer
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply mysql-connector driver feature defaults to statement config."""
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
