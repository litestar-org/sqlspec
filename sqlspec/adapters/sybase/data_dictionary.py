"""Sybase ASE database data dictionary implementation."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.sybase import dictionary as sybase_dictionary
from sqlspec.data_dictionary.dialects.sybase.config import SYBASE_CONFIG, SybaseVersionInfo
from sqlspec.data_dictionary.dialects.sybase.dictionary import (
    build_column_metadata,
    build_foreign_key_metadata,
    build_index_metadata,
    parse_sybase_version,
)

__all__ = (
    "SYBASE_CONFIG",
    "SybaseDataDictionary",
    "SybaseSyncDataDictionary",
    "SybaseVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_sybase_version",
)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SybaseSyncDataDictionary(sybase_dictionary.SybaseSyncDataDictionary):
    """Sybase ASE sync data dictionary for metadata reflection."""

    dialect: ClassVar[str] = "sybase"


SybaseDataDictionary = SybaseSyncDataDictionary
