"""Snowflake-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.snowflake import dictionary as snowflake_dictionary

__all__ = ("SnowflakeSyncDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SnowflakeSyncDataDictionary(snowflake_dictionary.SnowflakeSyncDataDictionary):
    """Snowflake-specific sync data dictionary."""

    dialect: ClassVar[str] = "snowflake"
