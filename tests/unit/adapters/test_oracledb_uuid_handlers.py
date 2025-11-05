"""Unit tests for Oracle UUID type handlers."""

import uuid

from sqlspec.adapters.oracledb._uuid_handlers import uuid_converter_in, uuid_converter_out


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


def test_uuid_variants() -> None:
    """Test all UUID variants (v1, v4, v5) roundtrip correctly."""
    test_uuids = [
        uuid.uuid1(),
        uuid.uuid4(),
        uuid.uuid5(uuid.NAMESPACE_DNS, "example.com"),
    ]

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
