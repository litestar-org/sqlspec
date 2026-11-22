"""SAP SQL Anywhere database data dictionary implementation."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.sqlanywhere import dictionary as sqlanywhere_dictionary
from sqlspec.data_dictionary.dialects.sqlanywhere.config import SQLANYWHERE_CONFIG, SqlanywhereVersionInfo
from sqlspec.data_dictionary.dialects.sqlanywhere.dictionary import (
    build_column_metadata,
    build_foreign_key_metadata,
    build_index_metadata,
    parse_sqlanywhere_version,
)

__all__ = (
    "SQLANYWHERE_CONFIG",
    "SqlanywhereDataDictionary",
    "SqlanywhereSyncDataDictionary",
    "SqlanywhereVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_sqlanywhere_version",
)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SqlanywhereSyncDataDictionary(sqlanywhere_dictionary.SqlanywhereSyncDataDictionary):
    """Synchronous SAP SQL Anywhere data dictionary."""

    dialect: ClassVar[str] = "sqlanywhere"


SqlanywhereDataDictionary = SqlanywhereSyncDataDictionary
