"""IBM Db2 database data dictionary implementation."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.db2.config import DB2_CONFIG, Db2VersionInfo
from sqlspec.data_dictionary.dialects.db2.dictionary import Db2AsyncDataDictionary as _Db2AsyncDataDictionary
from sqlspec.data_dictionary.dialects.db2.dictionary import Db2SyncDataDictionary as _Db2SyncDataDictionary
from sqlspec.data_dictionary.dialects.db2.dictionary import (
    build_column_metadata,
    build_foreign_key_metadata,
    build_index_metadata,
    parse_db2_version,
)

__all__ = (
    "DB2_CONFIG",
    "Db2AsyncDataDictionary",
    "Db2SyncDataDictionary",
    "Db2VersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_db2_version",
)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class Db2SyncDataDictionary(_Db2SyncDataDictionary):
    """IBM Db2 sync data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "db2"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class Db2AsyncDataDictionary(_Db2AsyncDataDictionary):
    """IBM Db2 async data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "db2"
