"""MySQL-specific data dictionary for metadata queries via pymysql."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mysql import MySQLSyncDataDictionary

__all__ = ("PyMysqlDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PyMysqlDataDictionary(MySQLSyncDataDictionary):
    """MySQL-specific sync data dictionary for pymysql."""

    dialect: ClassVar[str] = "mysql"
