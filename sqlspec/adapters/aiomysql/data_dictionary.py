"""MySQL-specific data dictionary for metadata queries via aiomysql."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mysql import MySQLAsyncDataDictionary

__all__ = ("AiomysqlDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AiomysqlDataDictionary(MySQLAsyncDataDictionary):
    """MySQL-specific async data dictionary for aiomysql."""

    dialect: ClassVar[str] = "mysql"
