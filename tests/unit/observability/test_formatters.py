"""Unit tests for cloud log formatters."""

from typing import TYPE_CHECKING

from sqlspec.observability import AWSLogFormatter, AzureLogFormatter, GCPLogFormatter

if TYPE_CHECKING:
    import pytest


def test_gcp_log_formatter_basic_format() -> None:
    """Should format basic log entry with severity and message."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "Query executed")
    assert entry["severity"] == "INFO"
    assert entry["message"] == "Query executed"


def test_gcp_log_formatter_severity_mapping() -> None:
    """Should map log levels to GCP severity."""
    formatter = GCPLogFormatter()
    assert formatter.format("DEBUG", "msg")["severity"] == "DEBUG"
    assert formatter.format("INFO", "msg")["severity"] == "INFO"
    assert formatter.format("WARNING", "msg")["severity"] == "WARNING"
    assert formatter.format("ERROR", "msg")["severity"] == "ERROR"
    assert formatter.format("CRITICAL", "msg")["severity"] == "CRITICAL"


def test_gcp_log_formatter_unknown_severity_defaults_to_default() -> None:
    """Unknown severity should default to DEFAULT."""
    formatter = GCPLogFormatter()
    entry = formatter.format("UNKNOWN", "msg")
    assert entry["severity"] == "DEFAULT"


def test_gcp_log_formatter_trace_id_with_project() -> None:
    """Should format trace ID with project prefix."""
    formatter = GCPLogFormatter(project_id="my-project")
    entry = formatter.format("INFO", "msg", trace_id="abc123")
    assert entry["logging.googleapis.com/trace"] == "projects/my-project/traces/abc123"


def test_gcp_log_formatter_trace_id_without_project(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Should omit trace when no project_id."""
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", trace_id="abc123")
    assert "logging.googleapis.com/trace" not in entry


def test_gcp_log_formatter_span_id() -> None:
    """Should include span ID when provided."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", span_id="span123")
    assert entry["logging.googleapis.com/spanId"] == "span123"


def test_gcp_log_formatter_correlation_id_in_labels() -> None:
    """Should include correlation_id in labels."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", correlation_id="corr123")
    assert entry["logging.googleapis.com/labels"]["correlation_id"] == "corr123"


def test_gcp_log_formatter_duration_ms() -> None:
    """Should include duration_ms when provided."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", duration_ms=15.5)
    assert entry["duration_ms"] == 15.5


def test_gcp_log_formatter_source_location() -> None:
    """Should include source location when provided."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", source_file="test.py", source_line=42, source_function="test_func")
    source = entry["logging.googleapis.com/sourceLocation"]
    assert source["file"] == "test.py"
    assert source["line"] == "42"
    assert source["function"] == "test_func"


def test_gcp_log_formatter_partial_source_location() -> None:
    """Should include only provided source location fields."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", source_file="test.py")
    source = entry["logging.googleapis.com/sourceLocation"]
    assert source["file"] == "test.py"
    assert "line" not in source
    assert "function" not in source


def test_gcp_log_formatter_extra_fields() -> None:
    """Should include extra fields in output."""
    formatter = GCPLogFormatter()
    entry = formatter.format("INFO", "msg", extra={"custom_field": "value"})
    assert entry["custom_field"] == "value"


def test_gcp_log_formatter_repr() -> None:
    """Should have informative repr."""
    formatter = GCPLogFormatter(project_id="my-project")
    assert "GCPLogFormatter" in repr(formatter)
    assert "my-project" in repr(formatter)


def test_gcp_log_formatter_equality() -> None:
    """Formatters with same project_id should be equal."""
    f1 = GCPLogFormatter(project_id="project")
    f2 = GCPLogFormatter(project_id="project")
    f3 = GCPLogFormatter(project_id="other")
    assert f1 == f2
    assert f1 != f3


def test_gcp_log_formatter_hash() -> None:
    """Formatter should be hashable."""
    formatter = GCPLogFormatter(project_id="project")
    assert isinstance(hash(formatter), int)


def test_aws_log_formatter_basic_format() -> None:
    """Should format basic log entry with level and message."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "Query executed")
    assert entry["level"] == "INFO"
    assert entry["message"] == "Query executed"
    assert "timestamp" in entry


def test_aws_log_formatter_level_mapping() -> None:
    """Should map log levels to AWS conventions."""
    formatter = AWSLogFormatter()
    assert formatter.format("DEBUG", "msg")["level"] == "DEBUG"
    assert formatter.format("INFO", "msg")["level"] == "INFO"
    assert formatter.format("WARNING", "msg")["level"] == "WARN"
    assert formatter.format("ERROR", "msg")["level"] == "ERROR"
    assert formatter.format("CRITICAL", "msg")["level"] == "FATAL"


def test_aws_log_formatter_unknown_level_defaults_to_info() -> None:
    """Unknown level should default to INFO."""
    formatter = AWSLogFormatter()
    entry = formatter.format("UNKNOWN", "msg")
    assert entry["level"] == "INFO"


def test_aws_log_formatter_correlation_id_as_request_id() -> None:
    """Should map correlation_id to requestId."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "msg", correlation_id="corr123")
    assert entry["requestId"] == "corr123"


def test_aws_log_formatter_xray_trace_id() -> None:
    """Should include X-Ray trace ID."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "msg", trace_id="1-abc-def")
    assert entry["xray_trace_id"] == "1-abc-def"


def test_aws_log_formatter_xray_segment_id() -> None:
    """Should include X-Ray segment ID from span_id."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "msg", span_id="segment123")
    assert entry["xray_segment_id"] == "segment123"


def test_aws_log_formatter_duration_ms() -> None:
    """Should include duration_ms when provided."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "msg", duration_ms=15.5)
    assert entry["duration_ms"] == 15.5


def test_aws_log_formatter_extra_fields() -> None:
    """Should include extra fields in output."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "msg", extra={"custom_field": "value"})
    assert entry["custom_field"] == "value"


def test_aws_log_formatter_timestamp_is_iso8601() -> None:
    """Timestamp should be ISO 8601 format."""
    formatter = AWSLogFormatter()
    entry = formatter.format("INFO", "msg")
    timestamp = entry["timestamp"]
    assert "T" in timestamp
    assert "+" in timestamp or "Z" in timestamp


def test_aws_log_formatter_repr() -> None:
    """Should have informative repr."""
    formatter = AWSLogFormatter()
    assert "AWSLogFormatter" in repr(formatter)


def test_aws_log_formatter_equality() -> None:
    """All AWSLogFormatters should be equal (no config)."""
    f1 = AWSLogFormatter()
    f2 = AWSLogFormatter()
    assert f1 == f2


def test_aws_log_formatter_hash() -> None:
    """Formatter should be hashable."""
    formatter = AWSLogFormatter()
    assert isinstance(hash(formatter), int)


def test_azure_log_formatter_basic_format() -> None:
    """Should format basic log entry with severityLevel and message."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "Query executed")
    assert entry["severityLevel"] == 1
    assert entry["message"] == "Query executed"


def test_azure_log_formatter_severity_mapping() -> None:
    """Should map log levels to Azure severity numbers."""
    formatter = AzureLogFormatter()
    assert formatter.format("DEBUG", "msg")["severityLevel"] == 0
    assert formatter.format("INFO", "msg")["severityLevel"] == 1
    assert formatter.format("WARNING", "msg")["severityLevel"] == 2
    assert formatter.format("ERROR", "msg")["severityLevel"] == 3
    assert formatter.format("CRITICAL", "msg")["severityLevel"] == 4


def test_azure_log_formatter_unknown_severity_defaults_to_info() -> None:
    """Unknown severity should default to INFO (1)."""
    formatter = AzureLogFormatter()
    entry = formatter.format("UNKNOWN", "msg")
    assert entry["severityLevel"] == 1


def test_azure_log_formatter_operation_id_from_trace_id() -> None:
    """Should map trace_id to operation_Id."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "msg", trace_id="trace123")
    assert entry["operation_Id"] == "trace123"


def test_azure_log_formatter_operation_parent_id_from_span_id() -> None:
    """Should map span_id to operation_ParentId."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "msg", span_id="span123")
    assert entry["operation_ParentId"] == "span123"


def test_azure_log_formatter_correlation_id_in_properties() -> None:
    """Should include correlation_id in properties."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "msg", correlation_id="corr123")
    assert entry["properties"]["correlationId"] == "corr123"


def test_azure_log_formatter_duration_ms_in_properties() -> None:
    """Should include duration_ms in properties."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "msg", duration_ms=15.5)
    assert entry["properties"]["durationMs"] == 15.5


def test_azure_log_formatter_extra_fields_in_properties() -> None:
    """Should include extra fields in properties."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "msg", extra={"custom_field": "value"})
    assert entry["properties"]["custom_field"] == "value"


def test_azure_log_formatter_no_properties_when_empty() -> None:
    """Should omit properties when no extra fields provided."""
    formatter = AzureLogFormatter()
    entry = formatter.format("INFO", "msg")
    assert "properties" not in entry


def test_azure_log_formatter_repr() -> None:
    """Should have informative repr."""
    formatter = AzureLogFormatter()
    assert "AzureLogFormatter" in repr(formatter)


def test_azure_log_formatter_equality() -> None:
    """All AzureLogFormatters should be equal (no config)."""
    f1 = AzureLogFormatter()
    f2 = AzureLogFormatter()
    assert f1 == f2


def test_azure_log_formatter_hash() -> None:
    """Formatter should be hashable."""
    formatter = AzureLogFormatter()
    assert isinstance(hash(formatter), int)


def test_formatter_case_insensitivity_gcp_case_insensitive() -> None:
    """GCP formatter should handle case-insensitive levels."""
    formatter = GCPLogFormatter()
    assert formatter.format("info", "msg")["severity"] == "INFO"
    assert formatter.format("Info", "msg")["severity"] == "INFO"
    assert formatter.format("INFO", "msg")["severity"] == "INFO"


def test_formatter_case_insensitivity_aws_case_insensitive() -> None:
    """AWS formatter should handle case-insensitive levels."""
    formatter = AWSLogFormatter()
    assert formatter.format("info", "msg")["level"] == "INFO"
    assert formatter.format("warning", "msg")["level"] == "WARN"


def test_formatter_case_insensitivity_azure_case_insensitive() -> None:
    """Azure formatter should handle case-insensitive levels."""
    formatter = AzureLogFormatter()
    assert formatter.format("info", "msg")["severityLevel"] == 1
    assert formatter.format("error", "msg")["severityLevel"] == 3
