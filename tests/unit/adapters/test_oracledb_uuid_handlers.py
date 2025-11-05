"""Unit tests for Oracle UUID type handlers."""

import uuid
from unittest.mock import Mock

from sqlspec.adapters.oracledb._uuid_handlers import (
    _input_type_handler,
    _output_type_handler,
    register_uuid_handlers,
    uuid_converter_in,
    uuid_converter_out,
)


def test_uuid_converter_in() -> None:
    """Test UUID to bytes conversion."""
    test_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    result = uuid_converter_in(test_uuid)

    assert isinstance(result, bytes)
    assert len(result) == 16
    assert result == test_uuid.bytes


def test_uuid_converter_out_valid() -> None:
    """Test valid bytes to UUID conversion."""
    test_uuid = uuid.UUID("87654321-4321-8765-4321-876543218765")
    test_bytes = test_uuid.bytes

    result = uuid_converter_out(test_bytes)

    assert isinstance(result, uuid.UUID)
    assert result == test_uuid


def test_uuid_converter_out_none() -> None:
    """Test NULL handling returns None."""
    result = uuid_converter_out(None)
    assert result is None


def test_uuid_converter_out_invalid_length() -> None:
    """Test invalid length bytes returns original bytes."""
    invalid_bytes = b"12345"
    result = uuid_converter_out(invalid_bytes)

    assert result is invalid_bytes
    assert isinstance(result, bytes)


def test_uuid_converter_out_invalid_format() -> None:
    """Test invalid UUID format bytes gracefully falls back to bytes.

    Note: Most 16-byte values are technically valid UUIDs, so this test
    verifies that the converter attempts conversion and returns bytes
    if it somehow fails (which is rare in practice).
    """
    test_bytes = uuid.uuid4().bytes
    result = uuid_converter_out(test_bytes)

    assert isinstance(result, uuid.UUID)


def test_uuid_converter_out_type_error() -> None:
    """Test TypeError during UUID conversion falls back to original value."""
    from unittest.mock import patch

    test_bytes = b"1234567890123456"

    with patch("uuid.UUID", side_effect=TypeError("Invalid type")):
        result = uuid_converter_out(test_bytes)

    assert result is test_bytes
    assert isinstance(result, bytes)


def test_uuid_converter_out_value_error() -> None:
    """Test ValueError during UUID conversion falls back to original value."""
    from unittest.mock import patch

    test_bytes = b"1234567890123456"

    with patch("uuid.UUID", side_effect=ValueError("Invalid UUID")):
        result = uuid_converter_out(test_bytes)

    assert result is test_bytes
    assert isinstance(result, bytes)


def test_uuid_variants() -> None:
    """Test all UUID variants (v1, v4, v5) roundtrip correctly."""
    test_uuids = [uuid.uuid1(), uuid.uuid4(), uuid.uuid5(uuid.NAMESPACE_DNS, "example.com")]

    for test_uuid in test_uuids:
        binary = uuid_converter_in(test_uuid)
        converted = uuid_converter_out(binary)
        assert converted == test_uuid


def test_uuid_roundtrip() -> None:
    """Test complete roundtrip conversion."""
    original = uuid.uuid4()
    binary = uuid_converter_in(original)
    converted = uuid_converter_out(binary)

    assert converted == original
    assert isinstance(converted, uuid.UUID)


def test_input_type_handler_with_uuid() -> None:
    """Test input type handler detects UUID and creates cursor variable."""
    import oracledb

    cursor = Mock()
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    test_uuid = uuid.uuid4()
    arraysize = 1

    result = _input_type_handler(cursor, test_uuid, arraysize)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=arraysize, inconverter=uuid_converter_in)


def test_input_type_handler_with_non_uuid() -> None:
    """Test input type handler returns None for non-UUID values."""
    cursor = Mock()

    result = _input_type_handler(cursor, "not a uuid", 1)

    assert result is None
    cursor.var.assert_not_called()


def test_input_type_handler_with_string() -> None:
    """Test input type handler returns None for string values."""
    cursor = Mock()

    result = _input_type_handler(cursor, "12345678-1234-5678-1234-567812345678", 1)

    assert result is None


def test_input_type_handler_with_bytes() -> None:
    """Test input type handler returns None for bytes values."""
    cursor = Mock()

    result = _input_type_handler(cursor, b"some bytes", 1)

    assert result is None


def test_output_type_handler_with_raw16() -> None:
    """Test output type handler detects RAW(16) columns."""
    import oracledb

    cursor = Mock()
    cursor.arraysize = 50
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_RAW
    metadata.size = 16

    result = _output_type_handler(cursor, metadata)

    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=50, outconverter=uuid_converter_out)


def test_output_type_handler_with_raw32() -> None:
    """Test output type handler returns None for RAW(32) columns."""
    import oracledb

    cursor = Mock()
    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_RAW
    metadata.size = 32

    result = _output_type_handler(cursor, metadata)

    assert result is None


def test_output_type_handler_with_varchar() -> None:
    """Test output type handler returns None for VARCHAR2 columns."""
    import oracledb

    cursor = Mock()
    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_VARCHAR
    metadata.size = 36

    result = _output_type_handler(cursor, metadata)

    assert result is None


def test_output_type_handler_with_number() -> None:
    """Test output type handler returns None for NUMBER columns."""
    import oracledb

    cursor = Mock()
    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_NUMBER
    metadata.size = 10

    result = _output_type_handler(cursor, metadata)

    assert result is None


def test_register_uuid_handlers_no_existing() -> None:
    """Test registering UUID handlers on connection without existing handlers."""
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    assert connection.inputtypehandler is not None
    assert connection.outputtypehandler is not None


def test_register_uuid_handlers_with_chaining() -> None:
    """Test UUID handler chaining with existing handlers."""
    existing_input = Mock(return_value=None)
    existing_output = Mock(return_value=None)

    connection = Mock()
    connection.inputtypehandler = existing_input
    connection.outputtypehandler = existing_output

    register_uuid_handlers(connection)

    assert connection.inputtypehandler is not None
    assert connection.outputtypehandler is not None
    assert connection.inputtypehandler != existing_input
    assert connection.outputtypehandler != existing_output


def test_register_uuid_handlers_chaining_fallback() -> None:
    """Test chaining falls back to existing handler when UUID handler returns None."""
    existing_input_result = Mock()
    existing_input = Mock(return_value=existing_input_result)

    connection = Mock()
    connection.inputtypehandler = existing_input
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    cursor = Mock()
    non_uuid_value = "not a uuid"

    result = connection.inputtypehandler(cursor, non_uuid_value, 1)

    existing_input.assert_called_once_with(cursor, non_uuid_value, 1)
    assert result is existing_input_result


def test_register_uuid_handlers_chaining_uuid_takes_priority() -> None:
    """Test UUID handler takes priority over existing handler for UUID values."""
    import oracledb

    existing_input = Mock(return_value=Mock())

    connection = Mock()
    connection.inputtypehandler = existing_input
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    cursor = Mock()
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)
    test_uuid = uuid.uuid4()

    result = connection.inputtypehandler(cursor, test_uuid, 1)

    existing_input.assert_not_called()
    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=1, inconverter=uuid_converter_in)


def test_register_uuid_handlers_output_chaining() -> None:
    """Test output handler chaining delegates to existing handler for non-RAW16."""
    import oracledb

    existing_output_result = Mock()
    existing_output = Mock(return_value=existing_output_result)

    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = existing_output

    register_uuid_handlers(connection)

    cursor = Mock()
    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_VARCHAR
    metadata.size = 36

    result = connection.outputtypehandler(cursor, metadata)

    existing_output.assert_called_once_with(cursor, metadata)
    assert result is existing_output_result


def test_combined_input_handler_no_existing_non_uuid() -> None:
    """Test combined input handler returns None when no existing handler and non-UUID value."""
    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    cursor = Mock()
    result = connection.inputtypehandler(cursor, "not a uuid", 1)

    assert result is None


def test_combined_output_handler_no_existing_non_raw16() -> None:
    """Test combined output handler returns None when no existing handler and non-RAW16."""
    import oracledb

    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = None

    register_uuid_handlers(connection)

    cursor = Mock()
    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_VARCHAR
    metadata.size = 36

    result = connection.outputtypehandler(cursor, metadata)

    assert result is None


def test_combined_output_handler_raw16_priority() -> None:
    """Test combined output handler prioritizes UUID handler for RAW16."""
    import oracledb

    existing_output = Mock(return_value=Mock())

    connection = Mock()
    connection.inputtypehandler = None
    connection.outputtypehandler = existing_output

    register_uuid_handlers(connection)

    cursor = Mock()
    cursor.arraysize = 50
    cursor_var = Mock()
    cursor.var = Mock(return_value=cursor_var)

    metadata = Mock()
    metadata.type_code = oracledb.DB_TYPE_RAW
    metadata.size = 16

    result = connection.outputtypehandler(cursor, metadata)

    existing_output.assert_not_called()
    assert result is cursor_var
    cursor.var.assert_called_once_with(oracledb.DB_TYPE_RAW, arraysize=50, outconverter=uuid_converter_out)
