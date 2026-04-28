"""Serialization utilities for SQLSpec."""

from sqlspec.utils.serializers._json import DEFAULT_TYPE_ENCODERS, TypeEncodersMap
from sqlspec.utils.serializers._json import decode_json as from_json
from sqlspec.utils.serializers._json import encode_json as to_json
from sqlspec.utils.serializers._numpy import numpy_array_dec_hook, numpy_array_enc_hook, numpy_array_predicate
from sqlspec.utils.serializers._schema import (
    SchemaSerializer,
    get_collection_serializer,
    get_serializer_metrics,
    reset_serializer_cache,
    schema_dump,
    serialize_collection,
)

__all__ = (
    "DEFAULT_TYPE_ENCODERS",
    "SchemaSerializer",
    "TypeEncodersMap",
    "from_json",
    "get_collection_serializer",
    "get_serializer_metrics",
    "numpy_array_dec_hook",
    "numpy_array_enc_hook",
    "numpy_array_predicate",
    "reset_serializer_cache",
    "schema_dump",
    "serialize_collection",
    "to_json",
)
