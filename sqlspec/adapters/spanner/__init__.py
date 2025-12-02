"""Google Cloud Spanner Adapter."""

from sqlglot.dialects.dialect import Dialect

from sqlspec.adapters.spanner import dialect
from sqlspec.adapters.spanner.config import (
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
    SpannerSyncConfig,
)
from sqlspec.adapters.spanner.driver import SpannerSyncDriver

Dialect.classes["spanner"] = dialect.Spanner
Dialect.classes["spangres"] = dialect.Spangres

__all__ = (
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncConfig",
    "SpannerSyncDriver",
    "dialect",
)
