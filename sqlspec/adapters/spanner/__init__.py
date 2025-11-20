"""Google Cloud Spanner Adapter."""

from sqlglot.dialects.dialect import Dialect

from sqlspec.adapters.spanner.config import (
    SpannerConfig,
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
)
from sqlspec.adapters.spanner.dialect import Spanner
from sqlspec.adapters.spanner.driver import SpannerSyncDriver

# Register the custom Spanner dialect with sqlglot
Dialect.classes["spanner"] = Spanner

__all__ = (
    "SpannerConfig",
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncDriver",
)
