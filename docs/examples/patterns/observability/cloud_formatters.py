from __future__ import annotations

__all__ = ("test_cloud_formatters",)


def test_cloud_formatters() -> None:
    # start-example
    from sqlspec.observability import AWSLogFormatter, GCPLogFormatter, ObservabilityConfig

    gcp_logs = ObservabilityConfig(cloud_formatter=GCPLogFormatter())
    aws_logs = ObservabilityConfig(cloud_formatter=AWSLogFormatter())
    # end-example

    assert gcp_logs.cloud_formatter is not None
    assert aws_logs.cloud_formatter is not None
