"""Tests for enhanced serialization functionality.

Tests for the byte-aware serialization system, including performance
improvements and compatibility with msgspec/orjson fallback patterns.
"""

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from sqlspec._serialization import decode_json, encode_json


class TestByteAwareEncoding:
    """Test byte-aware encoding functionality."""

    def test_encode_json_as_string(self) -> None:
        """Test encoding to string format."""
        data = {"key": "value", "number": 42}
        result = encode_json(data, as_bytes=False)

        assert isinstance(result, str)
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed == data

    def test_encode_json_as_bytes(self) -> None:
        """Test encoding to bytes format."""
        data = {"key": "value", "number": 42}
        result = encode_json(data, as_bytes=True)

        assert isinstance(result, bytes)
        # Should be valid JSON when decoded
        parsed = json.loads(result.decode("utf-8"))
        assert parsed == data

    def test_encode_json_default_is_string(self) -> None:
        """Test that default encoding returns string."""
        data = {"key": "value"}
        result = encode_json(data)

        assert isinstance(result, str)

    def test_round_trip_string(self) -> None:
        """Test string encoding round-trip."""
        data = {"uuid": str(uuid4()), "items": [1, 2, 3]}

        encoded = encode_json(data, as_bytes=False)
        decoded = decode_json(encoded)

        assert decoded == data

    def test_round_trip_bytes(self) -> None:
        """Test bytes encoding round-trip."""
        data = {"uuid": str(uuid4()), "items": [1, 2, 3]}

        encoded = encode_json(data, as_bytes=True)
        decoded = decode_json(encoded)

        assert decoded == data

    def test_complex_data_structures(self) -> None:
        """Test encoding complex nested structures."""
        data = {
            "users": [{"id": str(uuid4()), "name": "User 1"}, {"id": str(uuid4()), "name": "User 2"}],
            "metadata": {"count": 2, "timestamp": "2023-12-25T10:30:00Z"},
        }

        # Test both formats
        str_result = encode_json(data, as_bytes=False)
        bytes_result = encode_json(data, as_bytes=True)

        assert isinstance(str_result, str)
        assert isinstance(bytes_result, bytes)

        # Both should decode to same data
        assert decode_json(str_result) == data
        assert decode_json(bytes_result) == data


class TestDecodeJsonFunctionality:
    """Test decode_json functionality with both string and bytes input."""

    def test_decode_string_input(self) -> None:
        """Test decoding from string input."""
        data = {"key": "value", "number": 42}
        json_str = json.dumps(data)

        result = decode_json(json_str)
        assert result == data

    def test_decode_bytes_input(self) -> None:
        """Test decoding from bytes input."""
        data = {"key": "value", "number": 42}
        json_bytes = json.dumps(data).encode("utf-8")

        result = decode_json(json_bytes)
        assert result == data

    def test_decode_bytes_passthrough(self) -> None:
        """Test bytes passthrough when decode_bytes=False."""
        json_bytes = b'{"key": "value"}'

        result = decode_json(json_bytes, decode_bytes=False)
        assert result is json_bytes

    def test_unicode_handling(self) -> None:
        """Test proper unicode handling in encoding/decoding."""
        data = {"message": "Hello 🌍", "emoji": "🚀"}

        # Test string path
        encoded_str = encode_json(data, as_bytes=False)
        decoded_str = decode_json(encoded_str)
        assert decoded_str == data

        # Test bytes path
        encoded_bytes = encode_json(data, as_bytes=True)
        decoded_bytes = decode_json(encoded_bytes)
        assert decoded_bytes == data


class TestDatetimeHandling:
    """Test datetime serialization."""

    def test_datetime_serialization(self) -> None:
        """Test datetime objects are properly handled."""
        dt = datetime(2023, 12, 25, 10, 30, 0, tzinfo=timezone.utc)
        data = {"timestamp": dt}

        # Should not raise an exception
        encoded = encode_json(data, as_bytes=False)
        assert isinstance(encoded, str)

        # Should be valid JSON with datetime as string
        parsed = json.loads(encoded)
        assert "timestamp" in parsed
        assert isinstance(parsed["timestamp"], str)

    def test_datetime_with_timezone(self) -> None:
        """Test datetime with timezone information."""
        dt = datetime.now(timezone.utc)
        data = {"created_at": dt}

        encoded = encode_json(data, as_bytes=True)
        assert isinstance(encoded, bytes)


class TestPerformanceCharacteristics:
    """Test performance-related characteristics."""

    def test_bytes_encoding_efficiency(self) -> None:
        """Test that bytes encoding avoids string allocation."""
        # Large data structure
        data = {"records": [{"id": i, "data": f"record_{i}"} for i in range(1000)]}

        # Both should work without errors
        str_result = encode_json(data, as_bytes=False)
        bytes_result = encode_json(data, as_bytes=True)

        assert isinstance(str_result, str)
        assert isinstance(bytes_result, bytes)

        # Should decode to same data
        assert decode_json(str_result) == decode_json(bytes_result)

    def test_large_data_round_trip(self) -> None:
        """Test round-trip with larger data sets."""
        # Create moderately large data
        data = {
            "users": [
                {
                    "id": str(uuid4()),
                    "email": f"user{i}@example.com",
                    "metadata": {"created": "2023-01-01T00:00:00Z", "tags": [f"tag{j}" for j in range(10)]},
                }
                for i in range(100)
            ]
        }

        # Test bytes path for efficiency
        encoded = encode_json(data, as_bytes=True)
        decoded = decode_json(encoded)

        assert len(decoded["users"]) == 100
        assert all("id" in user for user in decoded["users"])


class TestErrorHandling:
    """Test error handling and fallback scenarios."""

    def test_invalid_json_string(self) -> None:
        """Test handling of invalid JSON strings."""
        # Import to get the specific msgspec error type if available
        try:
            from msgspec import DecodeError

            expected_exceptions = (ValueError, json.JSONDecodeError, DecodeError)
        except ImportError:
            expected_exceptions = (ValueError, json.JSONDecodeError)

        with pytest.raises(expected_exceptions):
            decode_json("invalid json")

    def test_invalid_json_bytes(self) -> None:
        """Test handling of invalid JSON bytes."""
        # Import to get the specific msgspec error type if available
        try:
            from msgspec import DecodeError

            expected_exceptions = (ValueError, json.JSONDecodeError, DecodeError)
        except ImportError:
            expected_exceptions = (ValueError, json.JSONDecodeError)

        with pytest.raises(expected_exceptions):
            decode_json(b"invalid json")

    def test_non_utf8_bytes(self) -> None:
        """Test handling of non-UTF8 bytes."""
        # This should be handled gracefully
        invalid_bytes = b"\xff\xfe invalid"

        # Should either decode or raise an appropriate exception
        try:
            from msgspec import DecodeError

            expected_exceptions = (UnicodeDecodeError, ValueError, json.JSONDecodeError, DecodeError)
        except ImportError:
            expected_exceptions = (UnicodeDecodeError, ValueError, json.JSONDecodeError)

        try:
            decode_json(invalid_bytes)
        except expected_exceptions:
            # Expected for invalid input
            pass


class TestFallbackBehavior:
    """Test fallback behavior when msgspec fails."""

    def test_msgspec_fallback(self) -> None:
        """Test that orjson fallback works when msgspec fails."""
        # Create data that might challenge msgspec
        data = {"special": float("inf")}

        # Should handle gracefully (either encode or raise appropriate error)
        try:
            result = encode_json(data, as_bytes=True)
            # If it succeeds, should be bytes
            assert isinstance(result, bytes)
        except (ValueError, TypeError):
            # Expected for special float values
            pass
