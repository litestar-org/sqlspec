"""Unit tests for data dictionary metadata types."""

from sqlspec.data_dictionary import TableStatisticsMetadata


def test_table_statistics_metadata_constructible() -> None:
    """TableStatisticsMetadata should accept the full native statistics shape."""
    entry: TableStatisticsMetadata = {
        "catalog_name": "main",
        "schema_name": "public",
        "table_name": "items",
        "column_name": None,
        "statistic_key": 6,
        "statistic_name": "adbc.statistic.row_count",
        "statistic_value": 42,
        "is_approximate": True,
    }

    assert entry["statistic_name"] == "adbc.statistic.row_count"
    assert entry["column_name"] is None


def test_table_statistics_metadata_partial() -> None:
    """TableStatisticsMetadata should remain optional for incremental construction."""
    entry: TableStatisticsMetadata = {"table_name": "items", "statistic_key": 1}

    assert entry["table_name"] == "items"
