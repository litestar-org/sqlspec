"""Tests for correlation context handling in SQL loader."""

from pathlib import Path

from sqlspec.loader import SQLFileLoader
from sqlspec.utils.correlation import CorrelationContext


def test_loader_does_not_clear_correlation_context(tmp_path: Path) -> None:
    path = tmp_path / "queries.sql"
    path.write_text("-- name: ping\nSELECT 1;\n", encoding="utf-8")

    CorrelationContext.set("outer")
    try:
        loader = SQLFileLoader()
        loader.load_sql(path)
        assert CorrelationContext.get() == "outer"
    finally:
        CorrelationContext.clear()
