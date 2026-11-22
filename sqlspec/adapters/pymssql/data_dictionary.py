"""pymssql data dictionary."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mssql import MssqlSyncDataDictionary, MssqlVersionInfo

__all__ = ("MssqlVersionInfo", "PymssqlSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PymssqlSyncDataDictionary(MssqlSyncDataDictionary):
    """MSSQL sync data dictionary for pymssql."""

    dialect: ClassVar[str] = "mssql"
