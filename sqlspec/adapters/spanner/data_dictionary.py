"""Spanner-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.spanner.dictionary import (
    SpannerAsyncDataDictionary as _SpannerAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.spanner.dictionary import SpannerSyncDataDictionary as _SpannerSyncDataDictionary

__all__ = ("SpannerAsyncDataDictionary", "SpannerSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SpannerSyncDataDictionary(_SpannerSyncDataDictionary):
    """Spanner-specific sync data dictionary."""

    dialect: ClassVar[str] = "spanner"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SpannerAsyncDataDictionary(_SpannerAsyncDataDictionary):
    """Spanner-specific async data dictionary."""

    dialect: ClassVar[str] = "spanner"
