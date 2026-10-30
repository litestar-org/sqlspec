"""Configure structured GCP Cloud Logging and AWS CloudWatch log formatters."""

from sqlspec.observability import AWSLogFormatter, GCPLogFormatter, ObservabilityConfig

__all__ = ("main",)


def main() -> tuple[ObservabilityConfig, ObservabilityConfig]:
    """Create ObservabilityConfig instances for GCP and AWS structured log output."""
    gcp_logs = ObservabilityConfig(cloud_formatter=GCPLogFormatter())
    aws_logs = ObservabilityConfig(cloud_formatter=AWSLogFormatter())
    assert gcp_logs.cloud_formatter is not None
    assert aws_logs.cloud_formatter is not None
    return gcp_logs, aws_logs


if __name__ == "__main__":
    print(main())
