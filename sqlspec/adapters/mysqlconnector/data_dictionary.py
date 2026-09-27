"""MySQL-specific data dictionary for metadata queries via mysql-connector."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mysql import MySQLAsyncDataDictionary, MySQLSyncDataDictionary

__all__ = ("MysqlConnectorAsyncDataDictionary", "MysqlConnectorSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MysqlConnectorSyncDataDictionary(MySQLSyncDataDictionary):
    """MySQL-specific sync data dictionary for mysql-connector."""

    dialect: ClassVar[str] = "mysql"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MysqlConnectorAsyncDataDictionary(MySQLAsyncDataDictionary):
    """MySQL-specific async data dictionary for mysql-connector."""

    dialect: ClassVar[str] = "mysql"
