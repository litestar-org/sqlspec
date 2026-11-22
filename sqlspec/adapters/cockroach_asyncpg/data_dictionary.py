"""CockroachDB AsyncPG data dictionary."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.cockroachdb import CockroachDBAsyncDataDictionary

__all__ = ("CockroachAsyncpgDataDictionary",)


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class CockroachAsyncpgDataDictionary(CockroachDBAsyncDataDictionary):
    """CockroachDB async data dictionary (AsyncPG)."""

    dialect: ClassVar[str] = "cockroachdb"
