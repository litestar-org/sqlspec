"""Unit tests for BigQuery and Spanner replacement metadata packs."""

from typing import Any, cast

from sqlspec.adapters.bigquery import data_dictionary as bigquery_data_dictionary
from sqlspec.adapters.spanner.data_dictionary import SpannerDataDictionary
from sqlspec.data_dictionary import (
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    get_data_dictionary_loader,
)


class _FakeBigQueryDriver:
    """Minimal sync driver recording data-dictionary statements."""

    def __init__(self) -> None:
        self.select_calls: list[tuple[Any, dict[str, Any]]] = []

    def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
        """Record SELECT calls and return no rows."""
        self.select_calls.append((statement, kwargs))
        return []


class _FakeSpannerDatabase:
    """Minimal Spanner database wrapper for Admin API DDL tests."""

    def __init__(self) -> None:
        self.get_ddl_calls = 0

    def get_ddl(self) -> list[str]:
        """Return static DDL statements."""
        self.get_ddl_calls += 1
        return ["CREATE TABLE Singers (SingerId INT64) PRIMARY KEY (SingerId)"]


class _FakeSpannerDriver:
    """Minimal Spanner driver exposing the database object."""

    def __init__(self) -> None:
        self.database = _FakeSpannerDatabase()


def test_bigquery_qualifier_parses_project_dataset_region() -> None:
    """BigQuery metadata scope keeps project, dataset, and region separate."""
    assert hasattr(bigquery_data_dictionary, "BigQueryMetadataScope")
    BigQueryDataDictionary = bigquery_data_dictionary.BigQueryDataDictionary
    BigQueryMetadataScope = bigquery_data_dictionary.BigQueryMetadataScope

    scope = BigQueryMetadataScope.from_schema("analytics.events", region="us")

    assert scope.project == "analytics"
    assert scope.dataset == "events"
    assert scope.region == "us"
    assert scope.dataset_information_schema_table("TABLES") == "`analytics.events.INFORMATION_SCHEMA.TABLES`"
    assert (
        scope.region_information_schema_table("JOBS_BY_PROJECT")
        == "`analytics.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`"
    )

    dictionary = BigQueryDataDictionary()
    driver = _FakeBigQueryDriver()

    dictionary.get_tables(cast(Any, driver), schema="analytics.events")

    query_text = str(driver.select_calls[0][0])
    assert "`analytics.events.INFORMATION_SCHEMA.TABLES`" in query_text
    assert driver.select_calls[0][1]["schema_name"] == "events"


def test_bigquery_indexes_include_search_and_vector_indexes() -> None:
    """BigQuery exposes search and vector index query packs instead of an empty stub."""
    loader = get_data_dictionary_loader()

    search_query = loader.get_domain_query("bigquery", "indexes", "search_by_dataset")
    vector_query = loader.get_domain_query("bigquery", "indexes", "vector_by_dataset")

    assert search_query.is_supported is True
    assert vector_query.is_supported is True
    assert search_query.query_text is not None
    assert vector_query.query_text is not None
    assert "{search_indexes_table}" in search_query.query_text
    assert "{search_index_columns_table}" in search_query.query_text
    assert "{vector_indexes_table}" in vector_query.query_text
    assert "{vector_index_columns_table}" in vector_query.query_text


def test_bigquery_information_schema_reports_billing_and_region_warnings() -> None:
    """BigQuery metadata capabilities surface billing, cache, region, and IAM caveats."""
    profile = bigquery_data_dictionary.BigQueryDataDictionary().get_metadata_capabilities(cast(Any, object()))

    objects = profile.get("objects")
    system = profile.get("system")
    privileges = profile.get("privileges")

    assert objects.support == MetadataSupport.SUPPORTED
    assert MetadataRisk.BILLED in objects.risks
    assert any("not cached" in warning for warning in objects.warnings)
    assert any("dataset or region" in warning for warning in objects.warnings)
    assert system.source == MetadataSource.SYSTEM_VIEW
    assert MetadataRisk.PRIVILEGED in system.risks
    assert MetadataRisk.BILLED in system.risks
    assert any("region" in warning for warning in system.warnings)
    assert MetadataRisk.REDACTED in privileges.risks
    assert any("inherited IAM" in warning for warning in privileges.warnings)


def test_spanner_mode_does_not_reuse_googlesql_for_postgresql() -> None:
    """PostgreSQL-dialect Spanner metadata stays gated until live coverage exists."""
    loader = get_data_dictionary_loader()

    googlesql = loader.get_domain_query("spanner", "tables", "by_schema", mode="googlesql")
    postgresql = loader.get_domain_query("spanner", "tables", "by_schema", mode="postgresql")
    dictionary = SpannerDataDictionary()
    profile = dictionary.get_metadata_capabilities(cast(Any, object()), mode="postgresql")
    alias_profile = dictionary.get_metadata_capabilities(cast(Any, object()), mode="spanner_postgresql")

    assert googlesql.is_supported is True
    assert googlesql.mode == "googlesql"
    assert postgresql.is_supported is False
    assert postgresql.mode == "postgresql"
    assert postgresql.capability.support == MetadataSupport.UNSUPPORTED
    assert profile.get("tables").support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.VERSION_GATED in profile.get("tables").risks
    assert alias_profile.get("tables") == profile.get("tables")
    assert any(
        "PostgreSQL-dialect Spanner metadata requires live runtime coverage" in warning
        for warning in profile.get("tables").warnings
    )


def test_spanner_get_ddl_uses_admin_api_capability() -> None:
    """Spanner native DDL is modeled as Admin API metadata, not SQL text."""
    driver = _FakeSpannerDriver()
    dictionary = SpannerDataDictionary()

    result = dictionary.get_ddl(cast(Any, driver), "Singers")
    ddl_capability = dictionary.get_metadata_capabilities(cast(Any, driver)).get("ddl")

    assert result.status == MetadataSupport.SUPPORTED
    assert result.source == MetadataSource.NATIVE_API
    assert result.fidelity == MetadataFidelity.NATIVE
    assert result.ddl == "CREATE TABLE Singers (SingerId INT64) PRIMARY KEY (SingerId)"
    assert driver.database.get_ddl_calls == 1
    assert ddl_capability.source == MetadataSource.NATIVE_API
    assert MetadataRisk.PRIVILEGED in ddl_capability.risks
