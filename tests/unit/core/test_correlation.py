"""Unit tests for CorrelationExtractor."""

import uuid

import pytest

from sqlspec.core import CorrelationExtractor


def test_correlation_extractor_defaults_default_headers_tuple() -> None:
    """DEFAULT_HEADERS should contain expected correlation headers."""
    assert "x-request-id" in CorrelationExtractor.DEFAULT_HEADERS
    assert "x-correlation-id" in CorrelationExtractor.DEFAULT_HEADERS
    assert "traceparent" in CorrelationExtractor.DEFAULT_HEADERS
    assert "x-cloud-trace-context" in CorrelationExtractor.DEFAULT_HEADERS
    assert "x-amzn-trace-id" in CorrelationExtractor.DEFAULT_HEADERS
    assert "x-b3-traceid" in CorrelationExtractor.DEFAULT_HEADERS


def test_correlation_extractor_defaults_default_max_length() -> None:
    """Default max_length should be 128 characters."""
    extractor = CorrelationExtractor()
    long_value = "a" * 200
    headers = {"x-request-id": long_value}
    result = extractor.extract(lambda h: headers.get(h))
    assert len(result) == 128


def test_correlation_extractor_extraction_extracts_primary_header() -> None:
    """Should extract value from primary header."""
    extractor = CorrelationExtractor(primary_header="x-custom-id")
    headers = {"x-custom-id": "custom-123", "x-request-id": "request-456"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "custom-123"


def test_correlation_extractor_extraction_extracts_from_default_headers_in_order() -> None:
    """Should try headers in priority order."""
    extractor = CorrelationExtractor()
    headers = {"x-correlation-id": "corr-123"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "corr-123"


def test_correlation_extractor_extraction_extracts_traceparent_header() -> None:
    """Should extract from W3C traceparent header."""
    extractor = CorrelationExtractor()
    headers = {"traceparent": "00-trace123-span456-01"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "00-trace123-span456-01"


def test_correlation_extractor_extraction_extracts_cloud_trace_context() -> None:
    """Should extract from GCP x-cloud-trace-context header."""
    extractor = CorrelationExtractor()
    headers = {"x-cloud-trace-context": "trace123/span456;o=1"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "trace123/span456;o=1"


def test_correlation_extractor_extraction_extracts_amzn_trace_id() -> None:
    """Should extract from AWS x-amzn-trace-id header."""
    extractor = CorrelationExtractor()
    headers = {"x-amzn-trace-id": "Root=1-abc-def;Parent=xyz"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "Root=1-abc-def;Parent=xyz"


def test_correlation_extractor_extraction_extracts_b3_traceid() -> None:
    """Should extract from Zipkin x-b3-traceid header."""
    extractor = CorrelationExtractor()
    headers = {"x-b3-traceid": "b3trace123"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "b3trace123"


def test_correlation_extractor_extraction_additional_headers_are_checked() -> None:
    """Should check additional_headers when provided."""
    extractor = CorrelationExtractor(additional_headers=("x-my-custom-header",))
    headers = {"x-my-custom-header": "my-custom-value"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "my-custom-value"


def test_correlation_extractor_extraction_primary_header_takes_precedence() -> None:
    """Primary header should take precedence over defaults."""
    extractor = CorrelationExtractor(primary_header="x-primary")
    headers = {"x-primary": "primary-val", "x-request-id": "request-val"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "primary-val"


def test_correlation_extractor_extraction_disabling_auto_trace_headers() -> None:
    """Should not check default trace headers when auto_trace_headers=False."""
    extractor = CorrelationExtractor(auto_trace_headers=False)
    headers = {"x-amzn-trace-id": "aws-trace-123"}
    result = extractor.extract(lambda h: headers.get(h))
    assert result != "aws-trace-123"
    assert len(result) == 36


def test_correlation_extractor_sanitization_strips_whitespace() -> None:
    """Should strip leading/trailing whitespace."""
    extractor = CorrelationExtractor()
    headers = {"x-request-id": "  trimmed-value  "}
    result = extractor.extract(lambda h: headers.get(h))
    assert result == "trimmed-value"


def test_correlation_extractor_sanitization_truncates_long_values() -> None:
    """Should truncate values exceeding max_length."""
    extractor = CorrelationExtractor(max_length=10)
    headers = {"x-request-id": "this-is-a-very-long-correlation-id"}
    result = extractor.extract(lambda h: headers.get(h))
    assert len(result) == 10
    assert result == "this-is-a-"


def test_correlation_extractor_sanitization_custom_max_length() -> None:
    """Should respect custom max_length parameter."""
    extractor = CorrelationExtractor(max_length=50)
    long_value = "x" * 100
    headers = {"x-request-id": long_value}
    result = extractor.extract(lambda h: headers.get(h))
    assert len(result) == 50


def test_correlation_extractor_sanitization_empty_value_triggers_fallback() -> None:
    """Empty values should trigger UUID fallback."""
    extractor = CorrelationExtractor()
    headers = {"x-request-id": "   "}
    result = extractor.extract(lambda h: headers.get(h))
    assert len(result) == 36


def test_correlation_extractor_fallback_generates_uuid_when_no_headers_present() -> None:
    """Should generate UUID when no correlation headers found."""
    extractor = CorrelationExtractor()
    result = extractor.extract(lambda h: None)
    assert len(result) == 36
    assert result.count("-") == 4


def test_correlation_extractor_fallback_generated_uuid_is_valid_format() -> None:
    """Generated fallback should be valid UUID format."""
    extractor = CorrelationExtractor()
    result = extractor.extract(lambda h: None)
    parts = result.split("-")
    assert len(parts) == 5
    assert len(parts[0]) == 8
    assert len(parts[1]) == 4
    assert len(parts[2]) == 4
    assert len(parts[3]) == 4
    assert len(parts[4]) == 12


def test_correlation_extractor_fallback_each_extraction_generates_unique_uuid() -> None:
    """Each fallback extraction should generate unique UUID."""
    extractor = CorrelationExtractor()
    results = {extractor.extract(lambda h: None) for _ in range(10)}
    assert len(results) == 10


def test_correlation_extractor_equality_repr() -> None:
    """Should have informative repr."""
    extractor = CorrelationExtractor(primary_header="x-custom", max_length=64)
    repr_str = repr(extractor)
    assert "CorrelationExtractor" in repr_str
    assert "x-custom" in repr_str


def test_correlation_extractor_equality_equality_same_config() -> None:
    """Extractors with same config should be equal."""
    extractor1 = CorrelationExtractor(primary_header="x-test", max_length=100)
    extractor2 = CorrelationExtractor(primary_header="x-test", max_length=100)
    assert extractor1 == extractor2


def test_correlation_extractor_equality_equality_different_config() -> None:
    """Extractors with different config should not be equal."""
    extractor1 = CorrelationExtractor(primary_header="x-test")
    extractor2 = CorrelationExtractor(primary_header="x-other")
    assert extractor1 != extractor2


def test_correlation_extractor_equality_equality_different_type() -> None:
    """Comparison with non-CorrelationExtractor should return NotImplemented."""
    extractor = CorrelationExtractor()
    assert extractor.__eq__("not an extractor") is NotImplemented


def test_correlation_extractor_equality_hash_is_supported() -> None:
    """CorrelationExtractor should be hashable (immutable after init)."""
    extractor = CorrelationExtractor()
    h = hash(extractor)
    assert isinstance(h, int)


@pytest.mark.parametrize(
    "valid_value",
    [
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "105445aa7843bc8bf206b12000100000/1;o=1",
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
        "req_123:abc.def+45/6",
    ],
)
def test_correlation_extractor_preserves_standard_trace_headers(valid_value: str) -> None:
    """W3C, Cloud Trace, and AWS X-Ray trace formats pass allowlist validation unchanged."""
    extractor = CorrelationExtractor()
    headers = {"traceparent": valid_value}
    assert extractor.extract(headers.get) == valid_value


def test_correlation_extractor_truncates_at_line_break() -> None:
    """Values containing CR or LF are truncated before the first line break."""
    extractor = CorrelationExtractor()
    headers = {"x-request-id": "abc\r\nX-Other: 1"}
    assert extractor.extract(headers.get) == "abc"


@pytest.mark.parametrize("invalid_value", ["abc';--", "a b", "caf\xe9", "\r\nleading-newline"])
def test_correlation_extractor_falls_through_on_disallowed_characters(invalid_value: str) -> None:
    """Invalid characters advance to the next configured header or fall back to a generated UUID."""
    extractor = CorrelationExtractor()
    with_fallback = {"x-request-id": invalid_value, "x-correlation-id": "fallback-id-123"}
    assert extractor.extract(with_fallback.get) == "fallback-id-123"

    only_invalid = {"x-request-id": invalid_value}
    generated = extractor.extract(only_invalid.get)
    assert str(uuid.UUID(generated)) == generated
