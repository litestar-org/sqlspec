"""Unit tests for CorrelationExtractor."""

from sqlspec.core import CorrelationExtractor


class TestCorrelationExtractorDefaults:
    """Tests for default CorrelationExtractor behavior."""

    def test_default_headers_tuple(self) -> None:
        """DEFAULT_HEADERS should contain expected correlation headers."""
        assert "x-request-id" in CorrelationExtractor.DEFAULT_HEADERS
        assert "x-correlation-id" in CorrelationExtractor.DEFAULT_HEADERS
        assert "traceparent" in CorrelationExtractor.DEFAULT_HEADERS
        assert "x-cloud-trace-context" in CorrelationExtractor.DEFAULT_HEADERS
        assert "x-amzn-trace-id" in CorrelationExtractor.DEFAULT_HEADERS
        assert "x-b3-traceid" in CorrelationExtractor.DEFAULT_HEADERS

    def test_default_max_length(self) -> None:
        """Default max_length should be 128 characters."""
        extractor = CorrelationExtractor()
        long_value = "a" * 200
        headers = {"x-request-id": long_value}
        result = extractor.extract(lambda h: headers.get(h))
        assert len(result) == 128


class TestCorrelationExtractorExtraction:
    """Tests for header extraction logic."""

    def test_extracts_primary_header(self) -> None:
        """Should extract value from primary header."""
        extractor = CorrelationExtractor(primary_header="x-custom-id")
        headers = {"x-custom-id": "custom-123", "x-request-id": "request-456"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "custom-123"

    def test_extracts_from_default_headers_in_order(self) -> None:
        """Should try headers in priority order."""
        extractor = CorrelationExtractor()
        headers = {"x-correlation-id": "corr-123"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "corr-123"

    def test_extracts_traceparent_header(self) -> None:
        """Should extract from W3C traceparent header."""
        extractor = CorrelationExtractor()
        headers = {"traceparent": "00-trace123-span456-01"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "00-trace123-span456-01"

    def test_extracts_cloud_trace_context(self) -> None:
        """Should extract from GCP x-cloud-trace-context header."""
        extractor = CorrelationExtractor()
        headers = {"x-cloud-trace-context": "trace123/span456;o=1"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "trace123/span456;o=1"

    def test_extracts_amzn_trace_id(self) -> None:
        """Should extract from AWS x-amzn-trace-id header."""
        extractor = CorrelationExtractor()
        headers = {"x-amzn-trace-id": "Root=1-abc-def;Parent=xyz"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "Root=1-abc-def;Parent=xyz"

    def test_extracts_b3_traceid(self) -> None:
        """Should extract from Zipkin x-b3-traceid header."""
        extractor = CorrelationExtractor()
        headers = {"x-b3-traceid": "b3trace123"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "b3trace123"

    def test_additional_headers_are_checked(self) -> None:
        """Should check additional_headers when provided."""
        extractor = CorrelationExtractor(additional_headers=("x-my-custom-header",))
        headers = {"x-my-custom-header": "my-custom-value"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "my-custom-value"

    def test_primary_header_takes_precedence(self) -> None:
        """Primary header should take precedence over defaults."""
        extractor = CorrelationExtractor(primary_header="x-primary")
        headers = {"x-primary": "primary-val", "x-request-id": "request-val"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "primary-val"

    def test_disabling_auto_trace_headers(self) -> None:
        """Should not check default trace headers when auto_trace_headers=False."""
        extractor = CorrelationExtractor(auto_trace_headers=False)
        headers = {"x-amzn-trace-id": "aws-trace-123"}
        result = extractor.extract(lambda h: headers.get(h))
        assert result != "aws-trace-123"
        assert len(result) == 36  # UUID length


class TestCorrelationExtractorSanitization:
    """Tests for value sanitization."""

    def test_strips_whitespace(self) -> None:
        """Should strip leading/trailing whitespace."""
        extractor = CorrelationExtractor()
        headers = {"x-request-id": "  trimmed-value  "}
        result = extractor.extract(lambda h: headers.get(h))
        assert result == "trimmed-value"

    def test_truncates_long_values(self) -> None:
        """Should truncate values exceeding max_length."""
        extractor = CorrelationExtractor(max_length=10)
        headers = {"x-request-id": "this-is-a-very-long-correlation-id"}
        result = extractor.extract(lambda h: headers.get(h))
        assert len(result) == 10
        assert result == "this-is-a-"

    def test_custom_max_length(self) -> None:
        """Should respect custom max_length parameter."""
        extractor = CorrelationExtractor(max_length=50)
        long_value = "x" * 100
        headers = {"x-request-id": long_value}
        result = extractor.extract(lambda h: headers.get(h))
        assert len(result) == 50

    def test_empty_value_triggers_fallback(self) -> None:
        """Empty values should trigger UUID fallback."""
        extractor = CorrelationExtractor()
        headers = {"x-request-id": "   "}
        result = extractor.extract(lambda h: headers.get(h))
        assert len(result) == 36  # UUID format


class TestCorrelationExtractorFallback:
    """Tests for UUID fallback generation."""

    def test_generates_uuid_when_no_headers_present(self) -> None:
        """Should generate UUID when no correlation headers found."""
        extractor = CorrelationExtractor()
        result = extractor.extract(lambda h: None)
        assert len(result) == 36
        assert result.count("-") == 4

    def test_generated_uuid_is_valid_format(self) -> None:
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

    def test_each_extraction_generates_unique_uuid(self) -> None:
        """Each fallback extraction should generate unique UUID."""
        extractor = CorrelationExtractor()
        results = {extractor.extract(lambda h: None) for _ in range(10)}
        assert len(results) == 10


class TestCorrelationExtractorEquality:
    """Tests for equality and representation."""

    def test_repr(self) -> None:
        """Should have informative repr."""
        extractor = CorrelationExtractor(primary_header="x-custom", max_length=64)
        repr_str = repr(extractor)
        assert "CorrelationExtractor" in repr_str
        assert "x-custom" in repr_str

    def test_equality_same_config(self) -> None:
        """Extractors with same config should be equal."""
        extractor1 = CorrelationExtractor(primary_header="x-test", max_length=100)
        extractor2 = CorrelationExtractor(primary_header="x-test", max_length=100)
        assert extractor1 == extractor2

    def test_equality_different_config(self) -> None:
        """Extractors with different config should not be equal."""
        extractor1 = CorrelationExtractor(primary_header="x-test")
        extractor2 = CorrelationExtractor(primary_header="x-other")
        assert extractor1 != extractor2

    def test_equality_different_type(self) -> None:
        """Comparison with non-CorrelationExtractor should return NotImplemented."""
        extractor = CorrelationExtractor()
        assert extractor.__eq__("not an extractor") is NotImplemented

    def test_hash_is_supported(self) -> None:
        """CorrelationExtractor should be hashable (immutable after init)."""
        extractor = CorrelationExtractor()
        h = hash(extractor)
        assert isinstance(h, int)
