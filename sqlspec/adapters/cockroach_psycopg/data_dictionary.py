"""CockroachDB-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.cockroachdb import CockroachDBAsyncDataDictionary, CockroachDBSyncDataDictionary

__all__ = ("CockroachPsycopgAsyncDataDictionary", "CockroachPsycopgSyncDataDictionary")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachPsycopgSyncDataDictionary(CockroachDBSyncDataDictionary):
    """CockroachDB sync data dictionary."""

    dialect: ClassVar[str] = "cockroachdb"


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachPsycopgAsyncDataDictionary(CockroachDBAsyncDataDictionary):
    """CockroachDB async data dictionary."""

    dialect: ClassVar[str] = "cockroachdb"
