"""mssql-python data dictionary."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mssql import MssqlSyncDataDictionary, MssqlVersionInfo

__all__ = ("MssqlPythonSyncDataDictionary", "MssqlVersionInfo")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MssqlPythonSyncDataDictionary(MssqlSyncDataDictionary):
    """MSSQL sync data dictionary for mssql-python."""

    dialect: ClassVar[str] = "mssql"
