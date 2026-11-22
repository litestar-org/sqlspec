"""Dialect configuration for spanner."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.spanner.config import SPANNER_CONFIG, SPANNER_VERSION_PATTERN

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.spanner.dictionary import (
        SpannerAsyncDataDictionary,
        SpannerSyncDataDictionary,
    )

__all__ = ("SPANNER_CONFIG", "SPANNER_VERSION_PATTERN", "SpannerAsyncDataDictionary", "SpannerSyncDataDictionary")

_EXPORTS: dict[str, tuple[str, str]] = {
    "SpannerAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.spanner.dictionary", "SpannerAsyncDataDictionary"),
    "SpannerSyncDataDictionary": ("sqlspec.data_dictionary.dialects.spanner.dictionary", "SpannerSyncDataDictionary"),
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
