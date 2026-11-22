"""PostgreSQL-specific data dictionary for metadata queries via psqlpy."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.postgres import PostgresAsyncDataDictionary

__all__ = ("PsqlpyDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PsqlpyDataDictionary(PostgresAsyncDataDictionary):
    """PostgreSQL-specific async data dictionary for psqlpy."""

    dialect: ClassVar[str] = "postgres"
