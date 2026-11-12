"""Snowflake cursor, session, and row collection tests."""

from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from sqlspec.adapters.snowflake import driver as snowflake_driver_module
from sqlspec.adapters.snowflake._typing import SnowflakeCursor, SnowflakeSessionContext
from sqlspec.adapters.snowflake.core import (
    DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS,
    build_profile,
    build_statement_config,
    collect_rows,
    create_mapped_exception,
    normalize_column_names,
    normalize_connection_params,
    normalize_execute_parameters,
    normalize_many_parameters,
    resolve_column_names,
)
from sqlspec.adapters.snowflake.type_converter import SnowflakeOutputConverter
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    ImproperConfigurationError,
    MissingDependencyError,
    NotFoundError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    UniqueViolationError,
)


def test_cursor_closes_without_masking_query_exception() -> None:
    cursor = Mock()
    cursor.close.side_effect = OSError("close failed")
    connection = Mock(cursor=Mock(return_value=cursor))
    with pytest.raises(ValueError, match="query failed"), SnowflakeCursor(connection) as acquired:
        assert acquired is cursor
        raise ValueError("query failed")
    cursor.close.assert_called_once_with()


def test_session_prepare_failure_releases_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, release = Mock(), Mock()
    driver = Mock()
    monkeypatch.setattr(snowflake_driver_module, "SnowflakeDriver", Mock(return_value=driver))

    def prepare(_: Any) -> Any:
        raise ValueError("prepare failed")

    context = SnowflakeSessionContext(lambda: connection, release, build_statement_config(), {}, prepare)
    with pytest.raises(ValueError, match="prepare failed"):
        with context:
            pytest.fail("failed preparation must not enter the block")
    assert release.call_args.args == (connection,)
    assert release.call_args.kwargs["exc_type"] is ValueError
    driver.close.assert_called_once_with()


def test_session_success_closes_driver_and_releases(monkeypatch: pytest.MonkeyPatch) -> None:
    connection, release, driver = Mock(), Mock(), Mock()
    monkeypatch.setattr(snowflake_driver_module, "SnowflakeDriver", Mock(return_value=driver))
    context = SnowflakeSessionContext(lambda: connection, release, build_statement_config(), {}, lambda value: value)
    with context as session:
        assert session is driver
    release.assert_called_once_with(connection, exc_type=None, exc_val=None, exc_tb=None)
    driver.close.assert_called_once_with()


def test_folded_names_and_variant_rows() -> None:
    description = [("USER_ID", 0), ("OrderTotal", 1), ("PAYLOAD", 5)]
    rows, names, row_format = collect_rows([(1, 2.5, '{"a":1}')], description, {}, converter=SnowflakeOutputConverter())
    assert names == ["user_id", "OrderTotal", "payload"]
    assert rows == [(1, 2.5, {"a": 1})]
    assert row_format == "tuple"
    assert normalize_column_names(["NAME", "camelCase", "1ST_COL"]) == ["name", "camelCase", "1ST_COL"]


@pytest.mark.parametrize(
    ("errno", "state", "expected"),
    [
        (250005, None, PermissionDeniedError),
        (2003, None, NotFoundError),
        (604, None, QueryTimeoutError),
        (250001, None, DatabaseConnectionError),
        (None, "23505", UniqueViolationError),
        (None, "42501", PermissionDeniedError),
        (None, "42000", SQLParsingError),
        (None, "08001", DatabaseConnectionError),
        (None, "22000", DataError),
    ],
)
def test_error_metadata_mapping(errno: int | None, state: str | None, expected: type[Exception]) -> None:
    error = RuntimeError("query failed")
    error.__dict__.update(errno=errno, sqlstate=state, sfqid="query-123")
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, expected)
    assert "[sfqid=query-123]" in str(mapped)
    assert mapped.__cause__ is error


def test_empty_arrow_schema_supports_attribute_metadata() -> None:
    pa = pytest.importorskip("pyarrow")
    from sqlspec.adapters.snowflake.core import build_empty_arrow_table_from_description

    description = [
        SimpleNamespace(name="ID", type_code=0, precision=10, scale=0, is_nullable=False),
        SimpleNamespace(name="TZ", type_code=8, precision=None, scale=None, is_nullable=True),
    ]
    table = build_empty_arrow_table_from_description(description, {})
    assert table.num_rows == 0
    assert table.schema == pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("tz", pa.timestamp("ns", tz="UTC")),
    ])


def test_server_bindings_preserve_aware_datetime_offsets() -> None:
    aware = datetime(2026, 11, 11, 12, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    naive = datetime(2026, 11, 11, 12, 30)
    explicit = ("TIMESTAMP_LTZ", aware)
    assert normalize_execute_parameters([aware, naive, explicit]) == [("TIMESTAMP_TZ", aware), naive, explicit]
    assert normalize_many_parameters([[aware], [naive]]) == [[("TIMESTAMP_TZ", aware)], [naive]]


@pytest.mark.parametrize("paramstyle", ["pyformat", "format"])
def test_client_bindings_keep_native_aware_datetime(paramstyle: str) -> None:
    aware = datetime(2026, 11, 11, 12, 30, tzinfo=UTC)
    parameters = {"created_at": aware}
    assert normalize_execute_parameters(parameters, paramstyle=paramstyle) is parameters
    assert normalize_many_parameters([parameters], paramstyle=paramstyle) == [parameters]


@pytest.mark.parametrize("paramstyle", ["qmark", "numeric"])
def test_public_driver_binds_aware_datetimes_as_timestamp_tz(snowflake_connection: Any, paramstyle: str) -> None:
    from sqlspec.adapters.snowflake import SnowflakeDriver

    aware = datetime(2026, 11, 11, 12, 30, tzinfo=UTC)
    naive = datetime(2026, 11, 11, 12, 30)
    explicit = ("TIMESTAMP_NTZ", aware)
    driver = SnowflakeDriver(snowflake_connection, statement_config=build_statement_config(paramstyle=paramstyle))
    driver.execute(
        "INSERT INTO events(created, local_time, selected_type) VALUES (:created, :local_time, :selected_type)",
        {"created": aware, "local_time": naive, "selected_type": explicit},
    )
    assert list(snowflake_connection.calls[-1][1]) == [("TIMESTAMP_TZ", aware), naive, explicit]
    driver.execute_many("INSERT INTO events(created) VALUES (:created)", [{"created": aware}, {"created": naive}])
    assert [list(row) for row in snowflake_connection.calls[-1][1]] == [[("TIMESTAMP_TZ", aware)], [naive]]


@pytest.mark.parametrize("paramstyle", ["pyformat", "format"])
def test_public_driver_client_modes_keep_datetime_values(snowflake_connection: Any, paramstyle: str) -> None:
    from sqlspec.adapters.snowflake import SnowflakeDriver

    aware = datetime(2026, 11, 11, 12, 30, tzinfo=UTC)
    driver = SnowflakeDriver(snowflake_connection, statement_config=build_statement_config(paramstyle=paramstyle))
    driver.execute("INSERT INTO events(created) VALUES (:created)", {"created": aware})
    assert list(snowflake_connection.calls[-1][1]) == [aware]
    driver.execute_many("INSERT INTO events(created) VALUES (:created)", [{"created": aware}])
    assert [list(row) for row in snowflake_connection.calls[-1][1]] == [[aware]]


def test_server_tuple_bindings_keep_json_codec_for_regular_tuples(snowflake_connection: Any) -> None:
    from sqlspec.adapters.snowflake import SnowflakeDriver

    aware = datetime(2026, 11, 11, 12, 30, tzinfo=UTC)
    explicit = ("TIMESTAMP_NTZ", aware)
    config = build_statement_config(json_serializer=lambda value: f"custom:{','.join(value)}")
    driver = SnowflakeDriver(snowflake_connection, statement_config=config)
    driver.execute(
        "INSERT INTO events(created, tags) VALUES (:created, :tags)", {"created": explicit, "tags": ("blue", "green")}
    )
    assert list(snowflake_connection.calls[-1][1]) == [explicit, "custom:blue,green"]


class _UnsubscriptableResultMetadata:
    __slots__ = ("name", "type_code")

    def __init__(self, name: str, type_code: int) -> None:
        self.name = name
        self.type_code = type_code


@pytest.mark.parametrize("use_metadata_objects", [False, True])
@pytest.mark.parametrize("use_mapping_rows", [False, True])
@pytest.mark.parametrize("enable_json_deserialization", [False, True])
@pytest.mark.parametrize("enable_lowercase_column_names", [False, True])
def test_collect_rows_preserves_values_across_cursor_and_descriptor_modes(
    use_metadata_objects: bool,
    use_mapping_rows: bool,
    enable_json_deserialization: bool,
    enable_lowercase_column_names: bool,
) -> None:
    description: list[Any] = (
        [
            _UnsubscriptableResultMetadata("USER_ID", 0),
            _UnsubscriptableResultMetadata("OrderTotal", 1),
            _UnsubscriptableResultMetadata("PAYLOAD", 5),
        ]
        if use_metadata_objects
        else [("USER_ID", 0), ("OrderTotal", 1), ("PAYLOAD", 5)]
    )
    raw_rows: list[Any] = (
        [{"USER_ID": 7, "OrderTotal": 12.5, "PAYLOAD": '{"ok":true}'}]
        if use_mapping_rows
        else [(7, 12.5, '{"ok":true}')]
    )
    converter = SnowflakeOutputConverter() if enable_json_deserialization else None
    cache: dict[tuple[tuple[str, ...], bool], list[str]] = {}
    rows, names, row_format = collect_rows(
        raw_rows, description, cache, converter=converter, enable_lowercase_column_names=enable_lowercase_column_names
    )
    expected_names = (
        ["user_id", "OrderTotal", "payload"] if enable_lowercase_column_names else ["USER_ID", "OrderTotal", "PAYLOAD"]
    )
    expected_payload: Any = {"ok": True} if enable_json_deserialization else '{"ok":true}'
    assert names == expected_names
    assert row_format == "tuple"
    assert rows == [(7, 12.5, expected_payload)]
    assert (
        resolve_column_names(description, cache, enable_lowercase_column_names=enable_lowercase_column_names)
        == expected_names
    )


def test_column_name_cache_bounds_and_empty_description() -> None:
    cache: dict[tuple[tuple[str, ...], bool], list[str]] = {}
    assert resolve_column_names(None, cache) == []
    for index in range(260):
        resolve_column_names([(f"COL_{index}", 0)], cache)
    assert len(cache) == 256
    assert DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS == 1000


def test_session_context_releases_connection_when_driver_close_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    release = Mock()
    connection = Mock()
    failing_driver = Mock()
    failing_driver.close.side_effect = RuntimeError("close exploded")
    monkeypatch.setattr(snowflake_driver_module, "SnowflakeDriver", Mock(return_value=failing_driver))
    closing_ctx = SnowflakeSessionContext(
        lambda: connection, release, build_statement_config(), {}, lambda value: value
    )
    with pytest.raises(RuntimeError, match="close exploded"), closing_ctx:
        pass
    assert release.call_args.args == (connection,)
    assert release.call_args.kwargs["exc_type"] is RuntimeError


def test_core_validation_edge_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    from sqlspec.utils.module_loader import reset_dependency_cache

    with pytest.raises(ImproperConfigurationError, match="paramstyle"):
        build_profile(paramstyle="named")
    assert normalize_connection_params({"authenticator": "oauth"})["authenticator"] == "oauth"
    assert normalize_connection_params({"username": "alice"})["user"] == "alice"
    with pytest.raises(ImproperConfigurationError, match="auth_type and authenticator conflict"):
        normalize_connection_params({"auth_type": "pat", "authenticator": "oauth"})
    with pytest.raises(ImproperConfigurationError, match="extra must be a mapping"):
        normalize_connection_params({"extra": "invalid"})
    with pytest.raises(ImproperConfigurationError, match="private_key"):
        normalize_connection_params({"private_key": object()})
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives.serialization", None)
    reset_dependency_cache("cryptography.hazmat.primitives.serialization")
    try:
        with pytest.raises(MissingDependencyError, match="cryptography"):
            normalize_connection_params({
                "private_key": "-----BEGIN PRIVATE KEY-----\nZmFrZQ==\n-----END PRIVATE KEY-----"
            })
    finally:
        reset_dependency_cache("cryptography.hazmat.primitives.serialization")
