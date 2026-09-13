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


def test_get_optimal_type_varchar_length() -> None:
    """get_optimal_type formats length when provided and falls back to text when omitted."""
    from sqlspec.data_dictionary import get_dialect_config

    pg = get_dialect_config("postgres")
    assert pg.get_optimal_type("varchar", length=255) == "VARCHAR(255)"
    assert pg.get_optimal_type("varchar") == "TEXT"

    ms = get_dialect_config("mssql")
    assert ms.get_optimal_type("varchar", length=100) == "NVARCHAR(100)"
    assert ms.get_optimal_type("varchar") == "NVARCHAR(MAX)"

    sl = get_dialect_config("sqlite")
    assert sl.get_optimal_type("varchar", length=255) == "TEXT"
    assert sl.get_optimal_type("varchar") == "TEXT"
