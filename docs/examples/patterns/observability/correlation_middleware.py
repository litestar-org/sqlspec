"""Bind request correlation IDs with CorrelationContext and framework middleware."""

# --8<-- [start:example]
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.utils.correlation import CorrelationContext

spec = SQLSpec()
conn_cfg = {"database": ":memory:"}
ext_cfg = {"litestar": {"enable_correlation_middleware": True}}
config = SqliteConfig(connection_config=conn_cfg, extension_config=ext_cfg)
spec.add_config(config)

with CorrelationContext.context("req-123") as correlation_id:
    active_id = CorrelationContext.get()
    assert active_id == "req-123"
# --8<-- [end:example]

__all__ = ("main",)


def main() -> str:
    """Configure correlation middleware and scope a request correlation identifier."""
    assert CorrelationContext.get() is None
    return correlation_id


if __name__ == "__main__":
    print(main())
