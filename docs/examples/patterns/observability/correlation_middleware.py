from __future__ import annotations

from pathlib import Path

from sqlspec.utils.correlation import CorrelationContext

__all__ = ("test_correlation_middleware",)


def test_correlation_middleware(tmp_path: Path) -> None:
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            connection_config={"database": str(tmp_path / "observability.db")},
            extension_config={"litestar": {"enable_correlation_middleware": True}},
        )
    )

    with CorrelationContext.context("req-123") as correlation_id:
        print(correlation_id)
    # end-example

    assert CorrelationContext.get() is None
