"""Oracle-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.oracle.config import (
    OracleVersionCache,
    resolve_oracle_connection_compatible_major,
    resolve_oracle_connection_major,
)
from sqlspec.data_dictionary.dialects.oracle.dictionary import (
    JSONStorageType,
    OracleAsyncDataDictionary,
    OracleSyncDataDictionary,
    OracleVersionInfo,
    storage_type_from_version,
)

__all__ = (
    "JSONStorageType",
    "OracleVersionCache",
    "OracleVersionInfo",
    "OracledbAsyncDataDictionary",
    "OracledbSyncDataDictionary",
    "resolve_oracle_connection_compatible_major",
    "resolve_oracle_connection_major",
    "storage_type_from_version",
)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class OracledbSyncDataDictionary(OracleSyncDataDictionary):
    """Oracle-specific sync data dictionary."""

    dialect: ClassVar[str] = "oracle"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class OracledbAsyncDataDictionary(OracleAsyncDataDictionary):
    """Oracle-specific async data dictionary."""

    dialect: ClassVar[str] = "oracle"
