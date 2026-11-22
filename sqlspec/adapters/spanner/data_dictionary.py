"""Spanner-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.spanner import dictionary as spanner_dictionary

__all__ = ("SpannerAsyncDataDictionary", "SpannerSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SpannerSyncDataDictionary(spanner_dictionary.SpannerSyncDataDictionary):
    """Spanner-specific sync data dictionary."""

    dialect: ClassVar[str] = "spanner"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SpannerAsyncDataDictionary(spanner_dictionary.SpannerAsyncDataDictionary):
    """Spanner-specific async data dictionary."""

    dialect: ClassVar[str] = "spanner"
