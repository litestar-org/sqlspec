"""Bind request correlation IDs with CorrelationContext and framework middleware."""

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.utils.correlation import CorrelationContext

__all__ = ("main",)


def main() -> str:
    """Configure correlation middleware and scope a request correlation identifier."""
    # --8<-- [start:example]
    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"enable_correlation_middleware": True}},
        )
    )

    with CorrelationContext.context("req-123") as correlation_id:
        active_id = CorrelationContext.get()
        assert active_id == "req-123"
    # --8<-- [end:example]

    assert CorrelationContext.get() is None
    return correlation_id


if __name__ == "__main__":
    print(main())
