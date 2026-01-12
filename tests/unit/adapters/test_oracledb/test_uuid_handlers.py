"""Unit tests for Oracle UUID type handlers."""

import uuid
from unittest.mock import Mock, patch

from sqlspec.adapters.oracledb import (
    register_uuid_handlers,
    uuid_converter_in,
    uuid_converter_out,
    uuid_input_type_handler,
    uuid_output_type_handler,
)


def test_uuid_converter_in() -> None:
    """UUID instances should convert to 16-byte RAW payloads."""

    test_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    result = uuid_converter_in(test_uuid)

    assert isinstance(result, bytes)
    assert len(result) == 16
    assert result == test_uuid.bytes


def test_uuid_converter_out_valid() -> None:
    """16-byte RAW payloads should convert back to UUID."""

    test_uuid = uuid.UUID("87654321-4321-8765-4321-876543218765")
    result = uuid_converter_out(test_uuid.bytes)

    assert isinstance(result, uuid.UUID)
    assert result == test_uuid


def test_uuid_converter_out_none() -> None:
    """None should stay None."""

    assert uuid_converter_out(None) is None


def test_uuid_converter_out_invalid_length() -> None:
    """Bytes with invalid length should be returned unchanged."""

    invalid_bytes = b"12345"
    result = uuid_converter_out(invalid_bytes)

    assert result is invalid_bytes


def test_uuid_converter_out_type_error() -> None:
    """TypeError should fall back to original bytes."""

    payload = b"1234567890123456"
    with patch("uuid.UUID", side_effect=TypeError("Invalid type")):
        result = uuid_converter_out(payload)
    assert result is payload


def test_uuid_converter_out_value_error() -> None:
    """ValueError should fall back to original bytes."""

    payload = b"1234567890123456"
    with patch("uuid.UUID", side_effect=ValueError("Invalid UUID")):
        result = uuid_converter_out(payload)
    assert result is payload


def test_uuid_variants_roundtrip() -> None:
    """Multiple UUID variants should roundtrip via converters."""

    variants = [uuid.uuid1(), uuid.uuid4(), uuid.uuid5(uuid.NAMESPACE_DNS, "example.com")]
    for entry in variants:
        binary = uuid_converter_in(entry)
        assert uuid_converter_out(binary) == entry


def test_input_type_handler_with_uuid() -> None:
    """Input handler should bind UUID values as RAW(16)."""

    import oracledb

    cursor = Mock()
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    result = uuid_input_type_handler(cursor, uuid.uuid4(), 1)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=1, inconverter=uuid_converter_in)


def test_input_type_handler_non_uuid() -> None:
    """Input handler should return None for non-UUID values."""

    cursor = Mock()
    assert uuid_input_type_handler(cursor, "not-a-uuid", 1) is None


def test_output_type_handler_raw16() -> None:
    """Output handler should wrap RAW(16) metadata."""

    import oracledb

    cursor = Mock()
    cursor.arraysize = 10
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    metadata = ("RAW_COL", oracledb.DB_TYPE_RAW, 16, 16, None, None, True)
    result = uuid_output_type_handler(cursor, metadata)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=10, outconverter=uuid_converter_out)


def test_output_type_handler_non_raw16() -> None:
    """Output handler should ignore non RAW(16) columns."""

    import oracledb

    cursor = Mock()
    metadata = ("VARCHAR_COL", oracledb.DB_TYPE_VARCHAR, 36, 36, None, None, True)
    assert uuid_output_type_handler(cursor, metadata) is None


def test_register_uuid_handlers_no_existing() -> None:
    """Registering handlers without existing ones should set both hooks."""

    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    assert connection.inputtypehandler is not None
    assert connection.outputtypehandler is not None


def test_register_uuid_handlers_with_existing() -> None:
    """Registering handlers should chain with existing hooks."""

    existing_input = Mock(return_value=None)
    existing_output = Mock(return_value=None)

    connection = Mock()
    connection.inputtypehandler = existing_input
    connection.outputtypehandler = existing_output

    register_uuid_handlers(connection)

    assert connection.inputtypehandler is not existing_input
    assert connection.outputtypehandler is not existing_output


def test_input_handler_chain_uses_existing_for_non_uuid() -> None:
    """Combined handler should defer to existing handler for non-UUID values."""

    fallback = Mock()
    connection = Mock()
    connection.inputtypehandler = fallback
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    cursor = Mock()
    value = "not-a-uuid"
    result = connection.inputtypehandler(cursor, value, 1)

    fallback.assert_called_once_with(cursor, value, 1)
    assert result is fallback.return_value


def test_input_handler_chain_prioritizes_uuid() -> None:
    """Combined handler should intercept UUID values before fallback handler."""

    import oracledb

    fallback = Mock()
    connection = Mock()
    connection.inputtypehandler = fallback
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    cursor = Mock()
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)
    test_uuid = uuid.uuid4()

    result = connection.inputtypehandler(cursor, test_uuid, 1)

    fallback.assert_not_called()
    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=1, inconverter=uuid_converter_in)


def test_output_handler_chain_uses_existing_for_non_uuid() -> None:
    """Combined output handler should defer to fallback for non RAW(16)."""

    import oracledb

    fallback = Mock()
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = fallback

    register_uuid_handlers(connection)

    cursor = Mock()
    metadata = ("VARCHAR_COL", oracledb.DB_TYPE_VARCHAR, 36, 36, None, None, True)
    result = connection.outputtypehandler(cursor, metadata)

    fallback.assert_called_once_with(cursor, metadata)
    assert result is fallback.return_value


def test_output_handler_chain_prioritizes_raw16() -> None:
    """Combined output handler should intercept RAW(16) columns."""

    import oracledb

    fallback = Mock()
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = fallback

    register_uuid_handlers(connection)

    cursor = Mock()
    cursor.arraysize = 32
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)
    metadata = ("RAW_COL", oracledb.DB_TYPE_RAW, 16, 16, None, None, True)

    result = connection.outputtypehandler(cursor, metadata)

    fallback.assert_not_called()
    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=32, outconverter=uuid_converter_out)
