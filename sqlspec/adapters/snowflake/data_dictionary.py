"""Snowflake-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.snowflake.dictionary import (
    SnowflakeSyncDataDictionary as _SnowflakeSyncDataDictionary,
)

__all__ = ("SnowflakeSyncDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SnowflakeSyncDataDictionary(_SnowflakeSyncDataDictionary):
    """Snowflake-specific sync data dictionary."""

    dialect: ClassVar[str] = "snowflake"
