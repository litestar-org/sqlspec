"""Google Cloud Spanner Adapter."""

from sqlglot.dialects.dialect import Dialect

from sqlspec.adapters.spanner.config import (
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
    SpannerSyncConfig,
)
from sqlspec.adapters.spanner.dialect import Spanner
from sqlspec.adapters.spanner.driver import SpannerSyncDriver

Dialect.classes["spanner"] = Spanner

__all__ = (
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncConfig",
    "SpannerSyncDriver",
)
