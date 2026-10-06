"""Google Cloud Spanner Adapter."""

from sqlspec.adapters.spanner._typing import (
    SpannerAsyncConnection,
    SpannerAsyncCursor,
    SpannerAsyncSessionContext,
    SpannerConnection,
    SpannerSessionContext,
    SpannerSyncConnection,
    SpannerSyncCursor,
    SpannerSyncSessionContext,
)
from sqlspec.adapters.spanner.config import (
    SpannerAsyncConfig,
    SpannerAsyncConnectionContext,
    SpannerConnectionContext,
    SpannerConnectionParams,
    SpannerDriverFeatures,
    SpannerPoolParams,
    SpannerSyncConfig,
    SpannerSyncConnectionContext,
    build_connection_config,
)
from sqlspec.adapters.spanner.core import default_statement_config
from sqlspec.adapters.spanner.data_dictionary import (
    SpannerAsyncDataDictionary,
    SpannerDataDictionary,
    SpannerSyncDataDictionary,
)
from sqlspec.adapters.spanner.driver import (
    SpannerAsyncDriver,
    SpannerAsyncExceptionHandler,
    SpannerExceptionHandler,
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
    "SpannerAsyncConnectionContext",
    "SpannerAsyncCursor",
    "SpannerAsyncDataDictionary",
    "SpannerAsyncDriver",
    "SpannerAsyncExceptionHandler",
    "SpannerAsyncSessionContext",
    "SpannerConnection",
    "SpannerConnectionContext",
    "SpannerConnectionParams",
    "SpannerDataDictionary",
    "SpannerDriverFeatures",
    "SpannerExceptionHandler",
    "SpannerPoolParams",
    "SpannerSessionContext",
    "SpannerSyncConfig",
    "SpannerSyncConnection",
    "SpannerSyncConnectionContext",
    "SpannerSyncCursor",
    "SpannerSyncDataDictionary",
    "SpannerSyncDriver",
    "SpannerSyncExceptionHandler",
    "SpannerSyncSessionContext",
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
