"""Dialect configuration for bigquery."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.bigquery.config import (
    BIGQUERY_CONFIG,
    BIGQUERY_VERSION_PATTERN,
    format_bigquery_information_schema_tables,
    format_bigquery_schema_prefix,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.bigquery.dictionary import (
        BigQueryAsyncDataDictionary,
        BigQueryMetadataScope,
        BigQuerySyncDataDictionary,
    )

__all__ = (
    "BIGQUERY_CONFIG",
    "BIGQUERY_VERSION_PATTERN",
    "BigQueryAsyncDataDictionary",
    "BigQueryMetadataScope",
    "BigQuerySyncDataDictionary",
    "format_bigquery_information_schema_tables",
    "format_bigquery_schema_prefix",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "BigQueryAsyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.bigquery.dictionary",
        "BigQueryAsyncDataDictionary",
    ),
    "BigQueryMetadataScope": ("sqlspec.data_dictionary.dialects.bigquery.dictionary", "BigQueryMetadataScope"),
    "BigQuerySyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.bigquery.dictionary",
        "BigQuerySyncDataDictionary",
    ),
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = _EXPORTS[name]
    module = import_module(module_path)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
