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
from sqlspec.adapters.spanner.type_converter import (
    bytes_to_spanner,
    coerce_params_for_spanner,
    infer_spanner_param_types,
    spanner_json,
    spanner_to_bytes,
    spanner_to_uuid,
    uuid_to_spanner,
)

Dialect.classes["spanner"] = dialect.Spanner
Dialect.classes["spangres"] = dialect.Spangres

__all__ = (
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncConfig",
    "SpannerSyncDriver",
    "bytes_to_spanner",
    "coerce_params_for_spanner",
    "dialect",
    "infer_spanner_param_types",
    "spanner_json",
    "spanner_to_bytes",
    "spanner_to_uuid",
    "uuid_to_spanner",
)
