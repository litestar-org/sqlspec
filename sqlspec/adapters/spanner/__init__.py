"""Google Cloud Spanner Adapter."""

from sqlspec.adapters.spanner._typing import (
    SpannerAsyncConnection,
    SpannerAsyncCursor,
    SpannerSyncConnection,
    SpannerSyncCursor,
)
from sqlspec.adapters.spanner.config import (
    SpannerAsyncConfig,
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
    SpannerSyncConfig,
    build_connection_config,
)
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.adapters.spanner.driver import (
    SpannerAsyncDriver,
    SpannerAsyncExceptionHandler,
    SpannerSyncDriver,
    SpannerSyncExceptionHandler,
)
from sqlspec.adapters.spanner.type_converter import (
    bytes_to_spanner,
    coerce_params_for_spanner,
    infer_spanner_param_types,
    spanner_json,
    spanner_to_bytes,
    spanner_to_uuid,
    uuid_to_spanner,
)

__all__ = (
    "SpannerAsyncConfig",
    "SpannerAsyncConnection",
    "SpannerAsyncCursor",
    "SpannerAsyncDriver",
    "SpannerAsyncExceptionHandler",
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncConfig",
    "SpannerSyncConnection",
    "SpannerSyncCursor",
    "SpannerSyncDriver",
    "SpannerSyncExceptionHandler",
    "build_connection_config",
    "bytes_to_spanner",
    "coerce_params_for_spanner",
    "default_statement_config",
    "infer_spanner_param_types",
    "spanner_json",
    "spanner_to_bytes",
    "spanner_to_uuid",
    "uuid_to_spanner",
)
