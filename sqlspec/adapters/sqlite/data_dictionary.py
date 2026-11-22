"""SQLite-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.sqlite import SqliteSyncDataDictionary

__all__ = ("SqliteDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SqliteDataDictionary(SqliteSyncDataDictionary):
    """SQLite-specific sync data dictionary."""

    dialect: ClassVar[str] = "sqlite"
