"""PostgreSQL-specific data dictionary for metadata queries via psycopg."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.postgres import PostgresAsyncDataDictionary, PostgresSyncDataDictionary

__all__ = ("PsycopgAsyncDataDictionary", "PsycopgSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PsycopgSyncDataDictionary(PostgresSyncDataDictionary):
    """PostgreSQL-specific sync data dictionary for psycopg."""

    dialect: ClassVar[str] = "postgres"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class PsycopgAsyncDataDictionary(PostgresAsyncDataDictionary):
    """PostgreSQL-specific async data dictionary for psycopg."""

    dialect: ClassVar[str] = "postgres"
