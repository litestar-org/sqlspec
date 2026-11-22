"""Teradata Vantage database data dictionary implementation."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.teradata import dictionary as teradata_dictionary
from sqlspec.data_dictionary.dialects.teradata.config import TERADATA_CONFIG, TeradataVersionInfo
from sqlspec.data_dictionary.dialects.teradata.dictionary import (
    build_column_metadata,
    build_foreign_key_metadata,
    build_index_metadata,
    parse_teradata_version,
)

__all__ = (
    "TERADATA_CONFIG",
    "TeradataDataDictionary",
    "TeradataSyncDataDictionary",
    "TeradataVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "parse_teradata_version",
)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class TeradataSyncDataDictionary(teradata_dictionary.TeradataSyncDataDictionary):
    """Synchronous Teradata Vantage data dictionary."""

    dialect: ClassVar[str] = "teradata"


TeradataDataDictionary = TeradataSyncDataDictionary
