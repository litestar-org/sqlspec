"""Configure structured GCP Cloud Logging and AWS CloudWatch log formatters."""

# --8<-- [start:example]
from sqlspec import observability as obs

gcp_logs = obs.ObservabilityConfig(cloud_formatter=obs.GCPLogFormatter())
aws_logs = obs.ObservabilityConfig(cloud_formatter=obs.AWSLogFormatter())
# --8<-- [end:example]

__all__ = ("aws_logs", "gcp_logs", "main")


def main() -> tuple[obs.ObservabilityConfig, obs.ObservabilityConfig]:
    """Create ObservabilityConfig instances for GCP and AWS structured log output."""
    assert gcp_logs.cloud_formatter is not None
    assert aws_logs.cloud_formatter is not None
    return gcp_logs, aws_logs


if __name__ == "__main__":
    print(main())
