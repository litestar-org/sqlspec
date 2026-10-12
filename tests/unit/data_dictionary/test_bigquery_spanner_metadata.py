"""Unit tests for BigQuery and Spanner metadata packs."""

from types import SimpleNamespace
from typing import Any, cast

from sqlspec.adapters.bigquery import data_dictionary as bigquery_data_dictionary
from sqlspec.adapters.spanner.data_dictionary import SpannerSyncDataDictionary
from sqlspec.data_dictionary import (
    MetadataFidelity,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    SystemMetadataRedactionPolicy,
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


class _FakeDatabaseAdminApi:
    """Minimal Database Admin API returning static DDL statements."""

    def __init__(self) -> None:
        self.requested: list[str] = []

    def get_database_ddl(self, *, database: str) -> SimpleNamespace:
        self.requested.append(database)
        return SimpleNamespace(statements=["CREATE TABLE Singers (SingerId INT64) PRIMARY KEY (SingerId)"])


class _FakeSpannerDatabase:
    """Minimal Spanner database exposing its instance client's Admin API."""

    def __init__(self) -> None:
        self.name = "projects/p/instances/i/databases/d"
        self.admin_api = _FakeDatabaseAdminApi()
        self._instance = SimpleNamespace(_client=SimpleNamespace(database_admin_api=self.admin_api))


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
    """PostgreSQL-dialect Spanner metadata routes to PostgreSQL query templates and reports supported."""
    loader = get_data_dictionary_loader()

    googlesql = loader.get_domain_query("spanner", "tables", "by_schema", mode="googlesql")
    postgresql = loader.get_domain_query("spanner", "tables", "by_schema", mode="postgresql")
    dictionary = SpannerSyncDataDictionary()
    profile = dictionary.get_metadata_capabilities(cast(Any, object()), mode="postgresql")
    alias_profile = dictionary.get_metadata_capabilities(cast(Any, object()), mode="spanner_postgresql")
    unknown_profile = dictionary.get_metadata_capabilities(cast(Any, object()), mode="unknown")

    assert googlesql.is_supported is True
    assert googlesql.mode == "googlesql"
    assert postgresql.is_supported is True
    assert postgresql.mode == "postgresql"
    assert postgresql.capability.support == MetadataSupport.SUPPORTED
    assert profile.get("tables").support == MetadataSupport.SUPPORTED
    assert alias_profile.get("tables") == profile.get("tables")
    assert googlesql.sql != postgresql.sql
    assert unknown_profile.get("tables").support == MetadataSupport.UNSUPPORTED
    assert MetadataRisk.VERSION_GATED in unknown_profile.get("tables").risks


def test_spanner_nullable_schema_queries_cast_bind_as_string() -> None:
    """Spanner must not infer a nullable schema bind as both INT64 and STRING."""
    dictionary = SpannerSyncDataDictionary()

    for domain in ("tables", "columns", "indexes", "foreign_keys"):
        query_text = dictionary.get_query_text(domain, "by_schema")

        assert "CAST(:schema_name AS STRING) IS NULL" in query_text
        assert ":schema_name IS NULL" not in query_text


def test_spanner_get_ddl_uses_admin_api_capability() -> None:
    """Spanner native DDL is modeled as Admin API metadata, not SQL text."""
    driver = _FakeSpannerDriver()
    dictionary = SpannerSyncDataDictionary()

    result = dictionary.get_ddl(cast(Any, driver), "Singers")
    ddl_capability = dictionary.get_metadata_capabilities(cast(Any, driver)).get("ddl")

    assert result.status == MetadataSupport.SUPPORTED
    assert result.source == MetadataSource.NATIVE_API
    assert result.fidelity == MetadataFidelity.NATIVE
    assert result.ddl == "CREATE TABLE Singers (SingerId INT64) PRIMARY KEY (SingerId)"
    assert driver.database.admin_api.requested == ["projects/p/instances/i/databases/d"]
    assert ddl_capability.source == MetadataSource.NATIVE_API
    assert MetadataRisk.PRIVILEGED in ddl_capability.risks


def test_spanner_googlesql_queries_use_lowercase_aliases_and_safe_binds() -> None:
    """Spanner GoogleSQL query packs must use lowercase aliases and :schema_name binds."""
    loader = get_data_dictionary_loader()
    objects_sql = loader.get_domain_query_text("spanner", "objects", "by_schema", mode="googlesql")
    constraints_sql = loader.get_domain_query_text("spanner", "constraints", "by_schema", mode="googlesql")
    sequences_sql = loader.get_domain_query_text("spanner", "sequences", "by_schema", mode="googlesql")
    system_sql = loader.get_domain_query_text("spanner", "system", "query_stats_top", mode="googlesql")

    assert objects_sql is not None
    assert "@schema_name" not in objects_sql
    assert "CAST(:schema_name AS STRING) IS NULL" in objects_sql
    assert "AS table_name" in objects_sql
    assert constraints_sql is not None and "AS constraint_name" in constraints_sql
    assert sequences_sql is not None and "AS sequence_name" in sequences_sql
    assert system_sql is not None and "AS query_text" in system_sql

    redacted_row, redacted_fields = SystemMetadataRedactionPolicy(redact_sql_text=True).redact_row({
        "text": "SELECT 1",
        "query_text": "SELECT 2",
    })
    assert redacted_row["text"] == "[REDACTED]"
    assert redacted_row["query_text"] == "[REDACTED]"
    assert set(redacted_fields) == {"text", "query_text"}


def test_bigquery_tables_and_constraints_avoid_self_fk_recursion_and_cartesian_joins() -> None:
    """BigQuery tables and constraints SQL avoid self-FK infinite recursion and composite Cartesian joins."""
    loader = get_data_dictionary_loader()
    tables_sql = loader.get_domain_query_text("bigquery", "tables", "by_schema")
    constraints_sql = loader.get_domain_query_text("bigquery", "constraints", "by_dataset")

    assert tables_sql is not None
    assert "pk_kcu.table_name <> kcu.table_name" in tables_sql
    assert "LEFT JOIN dependency_levels" in tables_sql
    assert constraints_sql is not None
    assert "pk_kcu.ordinal_position" in constraints_sql
    assert "ccu.column_name = kcu.column_name" in constraints_sql
