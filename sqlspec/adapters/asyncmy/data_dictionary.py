"""MySQL-specific data dictionary for metadata queries via asyncmy."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.mysql import MySQLAsyncDataDictionary

__all__ = ("AsyncmyDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AsyncmyDataDictionary(MySQLAsyncDataDictionary):
    """MySQL-specific async data dictionary for asyncmy."""

    dialect: ClassVar[str] = "mysql"
