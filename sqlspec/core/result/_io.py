"""IO helpers for result conversion and optional dependency loading."""

from typing import TYPE_CHECKING, Any

from sqlspec.utils.module_loader import ensure_pandas, ensure_polars

if TYPE_CHECKING:
    from sqlspec.typing import PandasDataFrame, PolarsDataFrame

__all__ = ("rows_to_pandas", "rows_to_polars")


def rows_to_pandas(data: "list[dict[str, Any]]") -> "PandasDataFrame":
    """Convert rows into a pandas DataFrame."""
    ensure_pandas()

    import pandas as pd

    return pd.DataFrame(data)


def rows_to_polars(data: "list[dict[str, Any]]") -> "PolarsDataFrame":
    """Convert rows into a Polars DataFrame."""
    ensure_polars()

    import polars as pl

    return pl.DataFrame(data)
