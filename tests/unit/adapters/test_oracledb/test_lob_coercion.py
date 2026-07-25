"""Unit tests for Oracle LOB and JSON parameter coercion."""

from collections.abc import Callable
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlspec.adapters.oracledb import OracleJson
from sqlspec.adapters.oracledb._typing import OracleAsyncConnection, OracleSyncConnection
from sqlspec.adapters.oracledb.core import (
    OracleAsyncStreamSource,
    OracleSyncStreamSource,
    coerce_large_parameters_async,
    coerce_large_parameters_sync,
    coerce_many_parameters_async,
    coerce_many_parameters_sync,
)
from sqlspec.adapters.oracledb.data_dictionary import OracleVersionCache, OracleVersionInfo
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.utils.serializers import to_json

CLOB_TYPE = "DB_TYPE_CLOB"
BLOB_TYPE = "DB_TYPE_BLOB"
VARCHAR2_LIMIT = 4000
RAW_LIMIT = 2000


def _direct_json(payload: object) -> object:
    return payload


def _wrapped_json(payload: object) -> OracleJson:
    return OracleJson(payload)


@pytest.fixture
def sync_connection() -> MagicMock:
    conn = MagicMock()
    conn.createlob.return_value = MagicMock(name="LOB")
    return conn


@pytest.fixture
def async_connection() -> AsyncMock:
    conn = AsyncMock()
    conn.createlob.return_value = MagicMock(name="LOB")
    return conn


@pytest.mark.parametrize("major", [12, 18, 20])
@pytest.mark.parametrize(
    ("payload", "wrapper"),
    [({"kind": "mapping"}, _direct_json), ([{"kind": "sequence"}], _direct_json), ({"kind": "wrapped"}, _wrapped_json)],
    ids=["dict", "list", "oracle-json"],
)
def test_coerce_json_parameters_sync_pre_native_versions_create_utf8_blob_locator(
    sync_connection: MagicMock, major: int, payload: object, wrapper: "Callable[[object], object]"
) -> None:
    """Oracle 12c-20c JSON values become textual UTF-8 BLOB locators before binding."""
    sync_connection._sqlspec_oracle_major = major
    value = wrapper(payload)

    result = coerce_large_parameters_sync(
        sync_connection,
        {"payload": value},
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, to_json(payload, as_bytes=True))
    assert result["payload"] is sync_connection.createlob.return_value


@pytest.mark.parametrize(
    ("payload", "wrapper"),
    [({"kind": "mapping"}, _direct_json), ([{"kind": "sequence"}], _direct_json), ({"kind": "wrapped"}, _wrapped_json)],
    ids=["dict", "list", "oracle-json"],
)
def test_coerce_json_parameters_sync_native_versions_keep_python_value_for_db_type_json(
    sync_connection: MagicMock, payload: object, wrapper: "Callable[[object], object]"
) -> None:
    """Oracle 21c+ values stay as Python JSON for the DB_TYPE_JSON input handler."""
    sync_connection._sqlspec_oracle_major = 21

    result = coerce_large_parameters_sync(
        sync_connection,
        {"payload": wrapper(payload)},
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    sync_connection.createlob.assert_not_called()
    assert result["payload"] is payload


def test_coerce_json_parameters_sync_uses_connection_version_when_cached_major_is_missing(
    sync_connection: MagicMock,
) -> None:
    """A reacquired Oracle 18c connection still takes the BLOB locator path."""
    payload = {"kind": "reacquired"}
    del sync_connection._sqlspec_oracle_major
    sync_connection.version = "18.0.0.0.0"

    result = coerce_large_parameters_sync(
        sync_connection,
        {"payload": payload},
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, to_json(payload, as_bytes=True))
    assert result["payload"] is sync_connection.createlob.return_value


def test_coerce_json_parameters_sync_prefers_pool_scoped_version_cache(sync_connection: MagicMock) -> None:
    """Resolved config metadata avoids wrapper attributes and version parsing."""
    payload = {"kind": "cached"}
    version_cache = OracleVersionCache()
    version_cache.resolved = True
    version_cache.version = OracleVersionInfo(18)
    del sync_connection._sqlspec_oracle_major
    sync_connection.version = "23.0.0.0.0"

    result = coerce_large_parameters_sync(
        sync_connection,
        {"payload": payload},
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
        version_cache=version_cache,
    )

    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, to_json(payload, as_bytes=True))
    assert result["payload"] is sync_connection.createlob.return_value


def test_coerce_json_parameters_sync_explicit_clob_remains_clob_on_oracle_18c(sync_connection: MagicMock) -> None:
    """OracleClob intent takes precedence over version-aware JSON coercion."""
    from sqlspec.adapters.oracledb import OracleClob

    sync_connection._sqlspec_oracle_major = 18

    result = coerce_large_parameters_sync(
        sync_connection,
        {"payload": OracleClob('{"kind":"clob"}')},
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, '{"kind":"clob"}')
    assert result["payload"] is sync_connection.createlob.return_value


@pytest.mark.anyio
async def test_coerce_json_parameters_async_oracle_18c_creates_utf8_blob_locator(async_connection: AsyncMock) -> None:
    """The async path awaits creation of the same textual JSON BLOB locator."""
    payload = {"kind": "async"}
    async_connection._sqlspec_oracle_major = 18

    result = await coerce_large_parameters_async(
        async_connection,
        {"payload": payload},
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    async_connection.createlob.assert_awaited_once_with(BLOB_TYPE, to_json(payload, as_bytes=True))
    assert result["payload"] is async_connection.createlob.return_value


def test_coerce_many_parameters_sync_applies_json_coercion_to_every_row(sync_connection: MagicMock) -> None:
    """Executemany coercion visits each named-bind row."""
    sync_connection._sqlspec_oracle_major = 18
    rows = [{"id": 1, "payload": {"row": 1}}, {"id": 2, "payload": [{"row": 2}]}]

    result = coerce_many_parameters_sync(
        sync_connection,
        rows,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    assert sync_connection.createlob.call_count == 2
    assert all(row["payload"] is sync_connection.createlob.return_value for row in result)


def test_coerce_many_parameters_sync_preserves_unchanged_rows_without_version_resolution(
    sync_connection: MagicMock,
) -> None:
    """Pass-through batches retain their containers and skip JSON metadata work."""
    rows = [(1, "short", b"x", None, 3.5), (2, "short", b"x", None, 4.5)]

    with patch("sqlspec.adapters.oracledb.core.resolve_oracle_connection_major") as resolve_major:
        result = coerce_many_parameters_sync(
            sync_connection,
            rows,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )

    assert result is rows
    assert all(result_row is source_row for result_row, source_row in zip(result, rows, strict=True))
    resolve_major.assert_not_called()


def test_coerce_many_parameters_sync_resolves_json_storage_once(sync_connection: MagicMock) -> None:
    """One batch shares a single server-storage decision across all JSON values."""
    rows = [{"id": 1, "payload": {"row": 1}}, {"id": 2, "payload": [{"row": 2}]}]

    with patch("sqlspec.adapters.oracledb.core.resolve_oracle_connection_major", return_value=18) as resolve_major:
        coerce_many_parameters_sync(
            sync_connection,
            rows,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )

    resolve_major.assert_called_once_with(sync_connection, None)


@pytest.mark.anyio
async def test_coerce_many_parameters_async_applies_json_coercion_to_every_row(async_connection: AsyncMock) -> None:
    """Async executemany coercion awaits locator creation for every row."""
    async_connection._sqlspec_oracle_major = 18
    rows = [{"id": 1, "payload": {"row": 1}}, {"id": 2, "payload": [{"row": 2}]}]

    result = await coerce_many_parameters_async(
        async_connection,
        rows,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )

    assert async_connection.createlob.await_count == 2
    assert all(row["payload"] is async_connection.createlob.return_value for row in result)


@pytest.mark.anyio
async def test_coerce_many_parameters_async_preserves_unchanged_rows_without_version_resolution(
    async_connection: AsyncMock,
) -> None:
    """Async pass-through batches retain containers and skip JSON metadata work."""
    rows = [(1, "short", b"x", None, 3.5), (2, "short", b"x", None, 4.5)]

    with patch("sqlspec.adapters.oracledb.core.resolve_oracle_connection_major") as resolve_major:
        result = await coerce_many_parameters_async(
            async_connection,
            rows,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )

    assert result is rows
    assert all(result_row is source_row for result_row, source_row in zip(result, rows, strict=True))
    resolve_major.assert_not_called()


@pytest.mark.anyio
async def test_coerce_many_parameters_async_resolves_json_storage_once(async_connection: AsyncMock) -> None:
    """Async batches share one server-storage decision across JSON values."""
    rows = [{"id": 1, "payload": {"row": 1}}, {"id": 2, "payload": [{"row": 2}]}]

    with patch("sqlspec.adapters.oracledb.core.resolve_oracle_connection_major", return_value=18) as resolve_major:
        await coerce_many_parameters_async(
            async_connection,
            rows,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )

    resolve_major.assert_called_once_with(async_connection, None)


def test_oracle_sync_stream_source_coerces_json_filter_before_execute() -> None:
    """Native sync streaming uses the same Oracle 18c BLOB locator bind path."""
    locator = MagicMock(name="BLOB")
    cursor = MagicMock()
    connection = MagicMock()
    connection._sqlspec_oracle_major = 18
    connection.createlob.return_value = locator
    connection.cursor.return_value = cursor
    driver = OracleSyncDriver(cast("OracleSyncConnection", connection))
    source = OracleSyncStreamSource(driver, "SELECT :payload FROM dual", {"payload": {"kind": "stream"}}, 100)

    source.start()

    parameters = cursor.execute.call_args.args[1]
    assert parameters["payload"] is locator


@pytest.mark.anyio
async def test_oracle_async_stream_source_coerces_json_filter_before_execute() -> None:
    """Native async streaming awaits the same Oracle 18c BLOB locator bind path."""
    locator = MagicMock(name="BLOB")
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    connection = MagicMock()
    connection._sqlspec_oracle_major = 18
    connection.createlob = AsyncMock(return_value=locator)
    connection.cursor.return_value = cursor
    driver = OracleAsyncDriver(cast("OracleAsyncConnection", connection))
    source = OracleAsyncStreamSource(driver, "SELECT :payload FROM dual", {"payload": {"kind": "stream"}}, 100)

    await source.start()

    parameters = cursor.execute.call_args.args[1]
    assert parameters["payload"] is locator


def test_coerce_large_parameters_sync_none_parameters_passthrough(sync_connection: MagicMock) -> None:
    result = coerce_large_parameters_sync(
        sync_connection,
        None,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result is None


def test_coerce_large_parameters_sync_list_parameters_passthrough(sync_connection: MagicMock) -> None:
    """A list whose values need no coercion round-trips through value-equal."""
    params = ["a", "b"]
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result == ["a", "b"]
    assert result is params
    sync_connection.createlob.assert_not_called()


def test_coerce_large_parameters_sync_string_under_threshold_no_coercion(sync_connection: MagicMock) -> None:
    params = {"name": "x" * 100}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_not_called()
    assert params["name"] == "x" * 100


def test_coerce_large_parameters_sync_string_exactly_at_threshold_no_coercion(sync_connection: MagicMock) -> None:
    params = {"name": "a" * 4000}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_not_called()


def test_coerce_large_parameters_sync_string_over_threshold_becomes_clob(sync_connection: MagicMock) -> None:
    params = {"content": "a" * 4001}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "a" * 4001)
    assert params["content"] is sync_connection.createlob.return_value


def test_coerce_large_parameters_sync_multibyte_string_under_charcount_but_over_bytecount(
    sync_connection: MagicMock,
) -> None:
    """A string with 2000 CJK chars = 6000 UTF-8 bytes > 4000 byte limit."""
    value = "一" * 2000
    assert len(value) == 2000
    assert len(value.encode("utf-8")) == 6000
    params = {"content": value}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, value)


def test_coerce_large_parameters_sync_bytes_under_threshold_no_coercion(sync_connection: MagicMock) -> None:
    params = {"data": b"\x00" * 100}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_not_called()


def test_coerce_large_parameters_sync_bytes_over_threshold_becomes_blob(sync_connection: MagicMock) -> None:
    params = {"data": b"\x00" * 2001}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"\x00" * 2001)


def test_coerce_large_parameters_sync_bytearray_over_threshold_becomes_blob(sync_connection: MagicMock) -> None:
    params = {"data": bytearray(b"\x01" * 2001)}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once()


def test_coerce_large_parameters_sync_mixed_parameters(sync_connection: MagicMock) -> None:
    params = {
        "small_str": "hello",
        "big_str": "x" * 5000,
        "small_bytes": b"\x00" * 100,
        "big_bytes": b"\xff" * 3000,
        "number": 42,
    }
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert params["small_str"] == "hello"
    assert params["big_str"] is sync_connection.createlob.return_value
    assert params["small_bytes"] == b"\x00" * 100
    assert params["number"] == 42
    assert sync_connection.createlob.call_count == 2


def test_coerce_large_parameters_sync_oracle_clob_wrapper_short_value_routed_to_clob(
    sync_connection: MagicMock,
) -> None:
    """OracleClob bypasses length threshold — explicit user intent."""
    from sqlspec.adapters.oracledb import OracleClob

    params = {"v": OracleClob("short text")}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "short text")
    assert params["v"] is sync_connection.createlob.return_value


def test_coerce_large_parameters_sync_oracle_clob_wrapper_bytes_decoded_to_str(sync_connection: MagicMock) -> None:
    """OracleClob(bytes) is utf-8 decoded before createlob."""
    from sqlspec.adapters.oracledb import OracleClob

    params = {"v": OracleClob(b"some bytes")}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "some bytes")


def test_coerce_large_parameters_sync_oracle_blob_wrapper_short_value_routed_to_blob(
    sync_connection: MagicMock,
) -> None:
    """OracleBlob bypasses length threshold — explicit user intent."""
    from sqlspec.adapters.oracledb import OracleBlob

    params = {"v": OracleBlob(b"short")}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"short")


def test_coerce_large_parameters_sync_oracle_blob_wrapper_str_encoded_to_bytes(sync_connection: MagicMock) -> None:
    """OracleBlob(str) is utf-8 encoded before createlob."""
    from sqlspec.adapters.oracledb import OracleBlob

    params = {"v": OracleBlob("text")}
    coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"text")


def test_coerce_large_parameters_sync_oracle_json_wrapper_unwrapped_to_value(sync_connection: MagicMock) -> None:
    """OracleJson unwraps so the C1 input handler can claim the value."""
    from sqlspec.adapters.oracledb import OracleJson

    params = {"v": OracleJson({"a": 1})}
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result["v"] == {"a": 1}
    sync_connection.createlob.assert_not_called()


def test_coerce_large_parameters_sync_threshold_override_keeps_5000_byte_str_as_varchar2(
    sync_connection: MagicMock,
) -> None:
    """varchar2_byte_limit=32767 (EXTENDED mode) keeps a 5000-byte str as VARCHAR2."""
    long_str = "x" * 5000
    params = {"v": long_str}
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=32767,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result["v"] is long_str
    sync_connection.createlob.assert_not_called()


def test_coerce_large_parameters_sync_oracle_clob_wrapper_unwrapped_in_positional_tuple(
    sync_connection: MagicMock,
) -> None:
    """OracleClob inside a positional tuple is unwrapped + routed to CLOB."""
    from sqlspec.adapters.oracledb import OracleClob

    params = (1, OracleClob("payload"))
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "payload")
    assert result[0] == 1
    assert result[1] is sync_connection.createlob.return_value


def test_coerce_large_parameters_sync_oracle_blob_wrapper_unwrapped_in_positional_list(
    sync_connection: MagicMock,
) -> None:
    """OracleBlob inside a positional list is unwrapped + routed to BLOB."""
    from sqlspec.adapters.oracledb import OracleBlob

    params = [1, OracleBlob(b"bytes")]
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"bytes")
    assert result[0] == 1
    assert result[1] is sync_connection.createlob.return_value


def test_coerce_large_parameters_sync_oracle_json_wrapper_unwrapped_in_positional_tuple(
    sync_connection: MagicMock,
) -> None:
    """OracleJson inside a positional tuple is unwrapped to its inner value."""
    from sqlspec.adapters.oracledb import OracleJson

    params = (1, OracleJson({"a": 1}))
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_not_called()
    assert result[1] == {"a": 1}


def test_coerce_large_parameters_sync_positional_tuple_str_over_threshold_becomes_clob(
    sync_connection: MagicMock,
) -> None:
    """Plain str above threshold inside a positional tuple still routes to CLOB."""
    long_str = "x" * 5000
    params = (1, long_str)
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_called_once_with(CLOB_TYPE, long_str)
    assert result[1] is sync_connection.createlob.return_value


def test_coerce_large_parameters_sync_positional_tuple_bytes_under_threshold_unchanged(
    sync_connection: MagicMock,
) -> None:
    """Small bytes inside a positional tuple are left alone."""
    params = (1, b"short")
    result = coerce_large_parameters_sync(
        sync_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    sync_connection.createlob.assert_not_called()
    assert tuple(result) == params


def test_coerce_large_parameters_sync_positional_empty_tuple_passthrough(sync_connection: MagicMock) -> None:
    """An empty tuple short-circuits like ``None`` / empty dict."""
    result = coerce_large_parameters_sync(
        sync_connection,
        (),
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result == ()
    sync_connection.createlob.assert_not_called()


@pytest.mark.anyio
async def test_coerce_large_parameters_async_none_parameters_passthrough(async_connection: AsyncMock) -> None:
    result = await coerce_large_parameters_async(
        async_connection,
        None,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result is None


@pytest.mark.anyio
async def test_coerce_large_parameters_async_string_over_threshold_becomes_clob(async_connection: AsyncMock) -> None:
    params = {"content": "a" * 4001}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(CLOB_TYPE, "a" * 4001)


@pytest.mark.anyio
async def test_coerce_large_parameters_async_bytes_over_threshold_becomes_blob(async_connection: AsyncMock) -> None:
    params = {"data": b"\x00" * 2001}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"\x00" * 2001)


@pytest.mark.anyio
async def test_coerce_large_parameters_async_multibyte_string_byte_threshold(async_connection: AsyncMock) -> None:
    value = "一" * 2000
    params = {"content": value}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(CLOB_TYPE, value)


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_clob_wrapper_short_value_routed_to_clob(
    async_connection: AsyncMock,
) -> None:
    """OracleClob bypasses length threshold — explicit user intent."""
    from sqlspec.adapters.oracledb import OracleClob

    params = {"v": OracleClob("short text")}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(CLOB_TYPE, "short text")


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_clob_wrapper_bytes_decoded_to_str(
    async_connection: AsyncMock,
) -> None:
    """OracleClob(bytes) is utf-8 decoded before createlob."""
    from sqlspec.adapters.oracledb import OracleClob

    params = {"v": OracleClob(b"some bytes")}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(CLOB_TYPE, "some bytes")


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_blob_wrapper_short_value_routed_to_blob(
    async_connection: AsyncMock,
) -> None:
    """OracleBlob bypasses length threshold — explicit user intent."""
    from sqlspec.adapters.oracledb import OracleBlob

    params = {"v": OracleBlob(b"short")}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"short")


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_blob_wrapper_str_encoded_to_bytes(
    async_connection: AsyncMock,
) -> None:
    """OracleBlob(str) is utf-8 encoded before createlob."""
    from sqlspec.adapters.oracledb import OracleBlob

    params = {"v": OracleBlob("text")}
    await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"text")


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_json_wrapper_unwrapped_to_value(
    async_connection: AsyncMock,
) -> None:
    """OracleJson unwraps so the C1 input handler can claim the value."""
    from sqlspec.adapters.oracledb import OracleJson

    params = {"v": OracleJson({"a": 1})}
    result = await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result["v"] == {"a": 1}
    async_connection.createlob.assert_not_called()


@pytest.mark.anyio
async def test_coerce_large_parameters_async_threshold_override_keeps_5000_byte_str_as_varchar2(
    async_connection: AsyncMock,
) -> None:
    """varchar2_byte_limit=32767 (EXTENDED mode) keeps a 5000-byte str as VARCHAR2."""
    long_str = "x" * 5000
    params = {"v": long_str}
    result = await coerce_large_parameters_async(
        async_connection,
        params,
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=32767,
        raw_byte_limit=RAW_LIMIT,
    )
    assert result["v"] is long_str
    async_connection.createlob.assert_not_called()


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_clob_wrapper_unwrapped_in_positional_tuple(
    async_connection: AsyncMock,
) -> None:
    """OracleClob inside a positional tuple is unwrapped + routed to CLOB."""
    from sqlspec.adapters.oracledb import OracleClob

    result = await coerce_large_parameters_async(
        async_connection,
        (1, OracleClob("payload")),
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(CLOB_TYPE, "payload")
    assert result[0] == 1


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_blob_wrapper_unwrapped_in_positional_list(
    async_connection: AsyncMock,
) -> None:
    """OracleBlob inside a positional list is unwrapped + routed to BLOB."""
    from sqlspec.adapters.oracledb import OracleBlob

    result = await coerce_large_parameters_async(
        async_connection,
        [1, OracleBlob(b"bytes")],
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"bytes")
    assert result[0] == 1


@pytest.mark.anyio
async def test_coerce_large_parameters_async_oracle_json_wrapper_unwrapped_in_positional_tuple(
    async_connection: AsyncMock,
) -> None:
    """OracleJson inside a positional tuple is unwrapped to its inner value."""
    from sqlspec.adapters.oracledb import OracleJson

    result = await coerce_large_parameters_async(
        async_connection,
        (1, OracleJson({"a": 1})),
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_not_called()
    assert result[1] == {"a": 1}


@pytest.mark.anyio
async def test_coerce_large_parameters_async_positional_tuple_str_over_threshold_becomes_clob(
    async_connection: AsyncMock,
) -> None:
    """Plain str above threshold inside a positional tuple still routes to CLOB."""
    long_str = "x" * 5000
    result = await coerce_large_parameters_async(
        async_connection,
        (1, long_str),
        clob_type=CLOB_TYPE,
        blob_type=BLOB_TYPE,
        varchar2_byte_limit=VARCHAR2_LIMIT,
        raw_byte_limit=RAW_LIMIT,
    )
    async_connection.createlob.assert_called_once_with(CLOB_TYPE, long_str)
    assert result[0] == 1
