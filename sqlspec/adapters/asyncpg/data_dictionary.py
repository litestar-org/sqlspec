"""PostgreSQL-specific data dictionary for metadata queries via asyncpg."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.postgres import PostgresAsyncDataDictionary

__all__ = ("AsyncpgDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class AsyncpgDataDictionary(PostgresAsyncDataDictionary):
    """PostgreSQL-specific async data dictionary for asyncpg."""

    dialect: ClassVar[str] = "postgres"
