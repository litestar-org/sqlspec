"""DuckDB-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.duckdb import DuckDBSyncDataDictionary

__all__ = ("DuckDBDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class DuckDBDataDictionary(DuckDBSyncDataDictionary):
    """DuckDB-specific sync data dictionary."""

    dialect: ClassVar[str] = "duckdb"
