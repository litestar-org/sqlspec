"""Tests for SpannerDataDictionary metadata extensions and PostgreSQL mode."""

from typing import Any, cast

from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.data_dictionary import MetadataSource, MetadataSupport, SystemMetadataResult


class MockSpannerDriver:
    """Mock driver to record query executions and parameters."""

    def __init__(self) -> None:
        self.last_query: Any = None
        self.last_params: dict[str, Any] = {}

    def select(self, query: Any, **params: Any) -> list[dict[str, Any]]:
        """Capture query and return mock records."""
        self.last_query = query
        self.last_params = params
        return [{"CONSTRAINT_NAME": "PK_Albums", "TABLE_NAME": "Albums", "CONSTRAINT_TYPE": "PRIMARY KEY"}]


def test_get_constraints_by_table() -> None:
    """get_constraints queries constraints for a specific table."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    results = dictionary.get_constraints(cast(Any, driver), table="Albums")

    assert len(results) == 1
    assert driver.last_params.get("table_name") == "Albums"
    assert "TABLE_CONSTRAINTS" in str(driver.last_query)


def test_get_constraints_by_schema() -> None:
    """get_constraints queries constraints for an entire schema."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    results = dictionary.get_constraints(cast(Any, driver), schema="catalog")

    assert len(results) == 1
    assert driver.last_params.get("schema_name") == "catalog"
    assert driver.last_params.get("table_name") is None


def test_get_sequences() -> None:
    """get_sequences queries sequence metadata."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    results = dictionary.get_sequences(cast(Any, driver), sequence_name="OrderSeq")

    assert len(results) == 1
    assert driver.last_params.get("sequence_name") == "OrderSeq"
    assert "INFORMATION_SCHEMA.SEQUENCES" in str(driver.last_query)


def test_get_change_streams() -> None:
    """get_change_streams queries change stream metadata."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    results = dictionary.get_change_streams(cast(Any, driver), stream_name="StreamAll")

    assert len(results) == 1
    assert driver.last_params.get("change_stream_name") == "StreamAll"
    assert "INFORMATION_SCHEMA.CHANGE_STREAMS" in str(driver.last_query)


def test_get_system_metadata_query_stats() -> None:
    """get_system_metadata queries SPANNER_SYS.QUERY_STATS_TOP_MINUTE when enabled."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    result = dictionary.get_system_metadata(cast(Any, driver), "query_stats_top", include_performance=True, limit=25)

    assert isinstance(result, SystemMetadataResult)
    assert result.source == MetadataSource.SYSTEM_VIEW
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert driver.last_params.get("limit") == 25
    assert "SPANNER_SYS.QUERY_STATS_TOP_MINUTE" in str(driver.last_query)


def test_get_system_metadata_table_sizes() -> None:
    """get_system_metadata queries SPANNER_SYS.TABLE_SIZES_STATS_1HOUR."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    result = dictionary.get_system_metadata(cast(Any, driver), "table_sizes", include_system=True, table="Albums")

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.SUPPORTED
    assert driver.last_params.get("table_name") == "Albums"
    assert "SPANNER_SYS.TABLE_SIZES_STATS_1HOUR" in str(driver.last_query)


def test_get_system_metadata_gated_by_default() -> None:
    """get_system_metadata returns gated status without opt-in flags."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    result = dictionary.get_system_metadata(cast(Any, driver), "query_stats_top")

    assert isinstance(result, SystemMetadataResult)
    assert result.capability.support == MetadataSupport.GATED


def test_metadata_capabilities_postgresql_mode() -> None:
    """get_metadata_capabilities reports SUPPORTED for standard domains in PostgreSQL mode."""
    dictionary = SpannerDataDictionary()
    driver = MockSpannerDriver()
    profile = dictionary.get_metadata_capabilities(driver, mode="postgresql")

    for domain in ("tables", "columns", "indexes", "constraints", "foreign_keys", "sequences", "change_streams"):
        capability = profile.get(domain)
        assert capability is not None
        assert capability.support == MetadataSupport.SUPPORTED
        assert capability.source == MetadataSource.INFORMATION_SCHEMA


def test_postgresql_mode_data_dictionary_routing() -> None:
    """SpannerDataDictionary initialized with mode='postgresql' routes all queries to postgresql pack."""
    dictionary = SpannerDataDictionary(mode="postgresql")
    driver = MockSpannerDriver()

    dictionary.get_tables(cast(Any, driver))
    assert ":schema_name::text" in str(driver.last_query)
    assert "information_schema.tables" in str(driver.last_query)

    dictionary.get_columns(cast(Any, driver), table="users")
    assert ":table_name::text" in str(driver.last_query)
    assert "information_schema.columns" in str(driver.last_query)

    dictionary.get_constraints(cast(Any, driver), table="users")
    assert "information_schema.table_constraints" in str(driver.last_query)

    dictionary.get_sequences(cast(Any, driver))
    assert "information_schema.sequences" in str(driver.last_query)

    dictionary.get_change_streams(cast(Any, driver))
    assert "information_schema.change_streams" in str(driver.last_query)
