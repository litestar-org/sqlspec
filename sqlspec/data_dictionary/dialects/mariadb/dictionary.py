"""Shared MariaDB data dictionary implementations for sync and async drivers."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mysql.dictionary import MySQLAsyncDataDictionary, MySQLSyncDataDictionary

__all__ = ("MariaDBAsyncDataDictionary", "MariaDBSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MariaDBAsyncDataDictionary(MySQLAsyncDataDictionary):
    """MariaDB-specific async data dictionary base."""

    dialect: ClassVar[str] = "mariadb"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class MariaDBSyncDataDictionary(MySQLSyncDataDictionary):
    """MariaDB-specific sync data dictionary base."""

    dialect: ClassVar[str] = "mariadb"
