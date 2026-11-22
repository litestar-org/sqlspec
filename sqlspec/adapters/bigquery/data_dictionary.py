"""BigQuery-specific data dictionary for metadata queries."""

from typing import ClassVar

from mypy_extensions import mypyc_attr

from sqlspec.data_dictionary.dialects.bigquery import BigQueryMetadataScope, BigQuerySyncDataDictionary

__all__ = ("BigQueryDataDictionary", "BigQueryMetadataScope")


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class BigQueryDataDictionary(BigQuerySyncDataDictionary):
    """BigQuery-specific sync data dictionary."""

    dialect: ClassVar[str] = "bigquery"
