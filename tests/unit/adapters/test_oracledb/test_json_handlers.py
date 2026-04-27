"""Unit tests for Oracle native JSON type handlers."""

from unittest.mock import Mock

import pytest

from sqlspec.adapters.oracledb import (
    json_converter_in_blob,
    json_converter_in_clob,
    json_converter_out_blob,
    json_converter_out_clob,
    json_input_type_handler,
    json_output_type_handler,
    register_json_handlers,
)


def _mock_cursor_with_major(major: int | None) -> Mock:
    """Build a cursor mock whose connection carries the given Oracle major version."""
    cursor = Mock()
    connection = Mock()
    connection._sqlspec_oracle_major = major
    cursor.connection = connection
    return cursor


def _mock_metadata(type_code: object, type_name: str = "") -> Mock:
    """Build a column-metadata mock matching python-oracledb's FetchInfo."""
    metadata = Mock()
    metadata.type_code = type_code
    metadata.type_name = type_name
    return metadata


def test_json_converter_in_clob_serialises_dict() -> None:
    """CLOB inconverter should serialise dict to a JSON string."""
    payload = {"foo": "bar", "n": 42}
    result = json_converter_in_clob(payload)

    assert isinstance(result, str)
    assert "foo" in result
    assert "bar" in result


def test_json_converter_in_clob_serialises_list() -> None:
    """CLOB inconverter should serialise list to a JSON string."""
    payload = [{"a": 1}, {"b": 2}]
    result = json_converter_in_clob(payload)

    assert isinstance(result, str)
    assert "a" in result and "b" in result


def test_json_converter_in_blob_serialises_dict_to_bytes() -> None:
    """BLOB inconverter should serialise dict to UTF-8 bytes."""
    payload = {"foo": "bar"}
    result = json_converter_in_blob(payload)

    assert isinstance(result, bytes)
    assert b"foo" in result and b"bar" in result


def test_json_converter_out_clob_parses_string() -> None:
    """CLOB outconverter should parse JSON string back to dict."""
    raw = '{"foo": "bar"}'
    result = json_converter_out_clob(raw)

    assert result == {"foo": "bar"}


def test_json_converter_out_blob_parses_bytes() -> None:
    """BLOB outconverter should parse JSON bytes back to dict."""
    raw = b'{"foo": "bar"}'
    result = json_converter_out_blob(raw)

    assert result == {"foo": "bar"}


def test_json_converter_out_clob_handles_none() -> None:
    """CLOB outconverter should pass None through."""
    assert json_converter_out_clob(None) is None


def test_json_converter_out_blob_handles_none() -> None:
    """BLOB outconverter should pass None through."""
    assert json_converter_out_blob(None) is None


def test_input_handler_returns_none_for_non_json_value() -> None:
    """Input handler should not claim ints, strings, or bytes."""
    cursor = _mock_cursor_with_major(21)

    assert json_input_type_handler(cursor, 42, 1) is None
    assert json_input_type_handler(cursor, "plain string", 1) is None
    assert json_input_type_handler(cursor, b"raw bytes", 1) is None
    assert json_input_type_handler(cursor, None, 1) is None


def test_input_handler_routes_dict_to_db_type_json_on_21c_plus() -> None:
    """Dict bound on Oracle 21c+ should use DB_TYPE_JSON."""
    import oracledb

    cursor = _mock_cursor_with_major(21)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, {"foo": "bar"}, 5)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_JSON, arraysize=5)


def test_input_handler_routes_dict_to_db_type_blob_on_19c() -> None:
    """Dict bound on Oracle 19c-20c should use DB_TYPE_BLOB with OSON-encoding inconverter."""
    import oracledb

    cursor = _mock_cursor_with_major(19)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, {"foo": "bar"}, 3)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_BLOB, arraysize=3, inconverter=json_converter_in_blob)


def test_input_handler_routes_dict_to_db_type_clob_on_12c() -> None:
    """Dict bound on Oracle 12c-18c should use DB_TYPE_CLOB with string-encoding inconverter."""
    import oracledb

    cursor = _mock_cursor_with_major(12)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, {"foo": "bar"}, 2)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_CLOB, arraysize=2, inconverter=json_converter_in_clob)


def test_input_handler_defaults_to_db_type_json_when_major_unknown() -> None:
    """When server version is unknown (None), default to DB_TYPE_JSON (21c+ assumption)."""
    import oracledb

    cursor = _mock_cursor_with_major(None)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, {"foo": "bar"}, 1)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_JSON, arraysize=1)


def test_input_handler_routes_list() -> None:
    """list[dict] should be claimed by the JSON handler."""
    import oracledb

    cursor = _mock_cursor_with_major(21)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, [{"a": 1}], 1)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_JSON, arraysize=1)


def test_input_handler_does_not_claim_list_of_floats() -> None:
    """list[float] (vector embedding) MUST NOT be claimed by JSON handler."""
    cursor = _mock_cursor_with_major(21)

    assert json_input_type_handler(cursor, [0.1, 0.2, 0.3], 1) is None


def test_input_handler_does_not_claim_tuple_of_floats() -> None:
    """tuple[float, ...] (vector embedding) MUST NOT be claimed by JSON handler."""
    cursor = _mock_cursor_with_major(21)

    assert json_input_type_handler(cursor, (0.1, 0.2, 0.3), 1) is None


def test_input_handler_claims_tuple_of_dicts() -> None:
    """tuple[dict, ...] should be claimed (it's a JSON-shaped payload)."""
    import oracledb

    cursor = _mock_cursor_with_major(21)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, ({"a": 1}, {"b": 2}), 1)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_JSON, arraysize=1)


def test_output_handler_short_circuits_db_type_json() -> None:
    """Native JSON columns return dict directly via python-oracledb; no conversion needed."""
    import oracledb

    cursor = Mock()
    metadata = _mock_metadata(oracledb.DB_TYPE_JSON, type_name="JSON")

    assert json_output_type_handler(cursor, metadata) is None


def test_output_handler_claims_blob_is_json() -> None:
    """BLOB columns whose type_name carries JSON should be parsed via outconverter."""
    import oracledb

    cursor = Mock()
    cursor.arraysize = 100
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)
    metadata = _mock_metadata(oracledb.DB_TYPE_BLOB, type_name="BLOB CHECK (… IS JSON)")

    result = json_output_type_handler(cursor, metadata)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_BLOB, arraysize=100, outconverter=json_converter_out_blob)


def test_output_handler_claims_clob_is_json() -> None:
    """CLOB columns whose type_name carries JSON should be parsed via outconverter."""
    import oracledb

    cursor = Mock()
    cursor.arraysize = 50
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)
    metadata = _mock_metadata(oracledb.DB_TYPE_CLOB, type_name="CLOB CHECK (… IS JSON)")

    result = json_output_type_handler(cursor, metadata)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_CLOB, arraysize=50, outconverter=json_converter_out_clob)


def test_output_handler_ignores_plain_blob() -> None:
    """BLOB columns without JSON in type_name should not be claimed."""
    import oracledb

    cursor = Mock()
    metadata = _mock_metadata(oracledb.DB_TYPE_BLOB, type_name="BLOB")

    assert json_output_type_handler(cursor, metadata) is None


def test_output_handler_ignores_plain_clob() -> None:
    """CLOB columns without JSON in type_name should not be claimed."""
    import oracledb

    cursor = Mock()
    metadata = _mock_metadata(oracledb.DB_TYPE_CLOB, type_name="CLOB")

    assert json_output_type_handler(cursor, metadata) is None


def test_output_handler_ignores_varchar2() -> None:
    """VARCHAR2 columns should never be claimed by JSON handler."""
    import oracledb

    cursor = Mock()
    metadata = _mock_metadata(oracledb.DB_TYPE_VARCHAR, type_name="VARCHAR2")

    assert json_output_type_handler(cursor, metadata) is None


def test_register_json_handlers_no_existing() -> None:
    """register_json_handlers should install both input and output handlers."""
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    register_json_handlers(connection)

    assert connection.inputtypehandler is not None
    assert connection.outputtypehandler is not None


def test_register_json_handlers_chains_existing() -> None:
    """register_json_handlers should wrap any existing handlers, not overwrite them."""
    existing_input = Mock(return_value=None)
    existing_output = Mock(return_value=None)

    connection = Mock()
    connection.inputtypehandler = existing_input
    connection.outputtypehandler = existing_output

    register_json_handlers(connection)

    assert connection.inputtypehandler is not existing_input
    assert connection.outputtypehandler is not existing_output


def test_input_handler_chain_falls_back_for_non_json_value() -> None:
    """The chained input handler should defer to the existing handler for non-dict/list values."""
    fallback = Mock(return_value="from_fallback")
    connection = Mock()
    connection.inputtypehandler = fallback
    connection.outputtypehandler = None

    register_json_handlers(connection)

    cursor = _mock_cursor_with_major(21)
    result = connection.inputtypehandler(cursor, "not-json", 1)

    fallback.assert_called_once_with(cursor, "not-json", 1)
    assert result == "from_fallback"


def test_input_handler_chain_prioritises_json() -> None:
    """The chained input handler should claim dict before falling through."""
    import oracledb

    fallback = Mock()
    connection = Mock()
    connection.inputtypehandler = fallback
    connection.outputtypehandler = None

    register_json_handlers(connection)

    cursor = _mock_cursor_with_major(21)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = connection.inputtypehandler(cursor, {"foo": "bar"}, 1)

    fallback.assert_not_called()
    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_JSON, arraysize=1)


def test_output_handler_chain_falls_back_for_non_json_column() -> None:
    """The chained output handler should defer to existing for non-JSON columns."""
    import oracledb

    fallback = Mock(return_value="from_fallback")
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = fallback

    register_json_handlers(connection)

    cursor = Mock()
    metadata = _mock_metadata(oracledb.DB_TYPE_VARCHAR, type_name="VARCHAR2")
    result = connection.outputtypehandler(cursor, metadata)

    fallback.assert_called_once_with(cursor, metadata)
    assert result == "from_fallback"


def test_output_handler_chain_prioritises_blob_is_json() -> None:
    """The chained output handler should claim BLOB-IS-JSON before falling through."""
    import oracledb

    fallback = Mock()
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = fallback

    register_json_handlers(connection)

    cursor = Mock()
    cursor.arraysize = 10
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)
    metadata = _mock_metadata(oracledb.DB_TYPE_BLOB, type_name="BLOB IS JSON")

    result = connection.outputtypehandler(cursor, metadata)

    fallback.assert_not_called()
    assert result is cursor_var


def test_module_exports_required_symbols() -> None:
    """The __all__ for _json_handlers must export the public surface."""
    from sqlspec.adapters.oracledb import _json_handlers  # pyright: ignore[reportPrivateUsage]

    assert "register_json_handlers" in _json_handlers.__all__
    assert "json_input_type_handler" in _json_handlers.__all__
    assert "json_output_type_handler" in _json_handlers.__all__
    assert "json_converter_in_blob" in _json_handlers.__all__
    assert "json_converter_in_clob" in _json_handlers.__all__
    assert "json_converter_out_blob" in _json_handlers.__all__
    assert "json_converter_out_clob" in _json_handlers.__all__


def test_roundtrip_clob() -> None:
    """Dict → CLOB inconverter → CLOB outconverter → original dict."""
    payload = {"foo": "bar", "n": 42, "nested": {"x": [1, 2, 3]}}

    serialised = json_converter_in_clob(payload)
    parsed = json_converter_out_clob(serialised)

    assert parsed == payload


def test_roundtrip_blob() -> None:
    """Dict → BLOB inconverter → BLOB outconverter → original dict."""
    payload = {"foo": "bar", "n": 42, "nested": {"x": [1, 2, 3]}}

    serialised = json_converter_in_blob(payload)
    parsed = json_converter_out_blob(serialised)

    assert parsed == payload


def test_roundtrip_clob_list() -> None:
    """list[dict] → CLOB inconverter → CLOB outconverter → original list."""
    payload: list[dict[str, int]] = [{"a": 1}, {"b": 2}, {"c": 3}]

    serialised = json_converter_in_clob(payload)
    parsed = json_converter_out_clob(serialised)

    assert parsed == payload


@pytest.mark.parametrize("major", [12, 19, 21, 23])
def test_input_handler_dispatch_table(major: int) -> None:
    """Across all supported server majors, dict binding succeeds and produces a cursor.var call."""
    cursor = _mock_cursor_with_major(major)
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = json_input_type_handler(cursor, {"k": "v"}, 1)

    assert result is cursor_var
    assert cursor.var.call_count == 1
