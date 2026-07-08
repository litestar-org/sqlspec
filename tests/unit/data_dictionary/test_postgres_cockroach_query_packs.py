"""PostgreSQL-family replacement data-dictionary query-pack contracts."""

import pytest

from sqlspec.data_dictionary import DataDictionaryLoader, MetadataRisk, MetadataSource, MetadataSupport

POSTGRES_DOMAIN_QUERIES = (
    ("schemas", "list"),
    ("objects", "by_schema"),
    ("tables", "by_schema"),
    ("columns", "by_schema"),
    ("constraints", "by_schema"),
    ("indexes", "by_schema"),
    ("views", "by_schema"),
    ("materialized_views", "by_schema"),
    ("sequences", "by_schema"),
    ("routines", "by_schema"),
    ("triggers", "by_schema"),
    ("comments", "by_schema"),
    ("privileges", "by_schema"),
    ("dependencies", "by_schema"),
    ("extensions", "list"),
    ("partitions", "by_schema"),
    ("ddl", "by_object"),
    ("system", "settings"),
    ("system", "table_stats"),
    ("system", "pg_stat_statements"),
)

COCKROACH_STABLE_DOMAIN_QUERIES = (
    ("schemas", "list"),
    ("objects", "by_schema"),
    ("tables", "by_schema"),
    ("columns", "by_schema"),
    ("constraints", "by_schema"),
    ("indexes", "by_schema"),
    ("views", "by_schema"),
    ("sequences", "by_schema"),
    ("comments", "by_schema"),
    ("privileges", "by_schema"),
    ("dependencies", "by_schema"),
    ("ddl", "by_object"),
)


@pytest.mark.parametrize(("domain", "query_name"), POSTGRES_DOMAIN_QUERIES)
def test_postgres_replacement_domain_query_pack_is_available(domain: str, query_name: str) -> None:
    """PostgreSQL exposes every C3 metadata domain through direct domain paths."""
    query = DataDictionaryLoader().get_domain_query("postgresql", domain, query_name)

    assert query.is_supported is True
    assert query.dialect == "postgres"
    assert query.domain == domain
    assert query.name == query_name
    assert query.capability.source == MetadataSource.CATALOG
    assert query.query_text is not None
    assert "pg_catalog" in query.query_text or domain == "system"


def test_postgres_query_pack_uses_catalogs_needed_for_ddl_grade_metadata() -> None:
    """DDL-grade PostgreSQL domains use pg_catalog objects, not information_schema-only queries."""
    loader = DataDictionaryLoader()

    columns = loader.get_domain_query_text("postgres", "columns", "by_schema")
    constraints = loader.get_domain_query_text("postgres", "constraints", "by_schema")
    indexes = loader.get_domain_query_text("postgres", "indexes", "by_schema")
    dependencies = loader.get_domain_query_text("postgres", "dependencies", "by_schema")
    ddl = loader.get_domain_query_text("postgres", "ddl", "by_object")

    assert columns is not None and "pg_catalog.pg_attribute" in columns
    assert columns is not None and "pg_catalog.format_type" in columns
    assert constraints is not None and "pg_catalog.pg_constraint" in constraints
    assert constraints is not None and "pg_catalog.pg_get_constraintdef" in constraints
    assert indexes is not None and "pg_catalog.pg_index" in indexes
    assert indexes is not None and "pg_catalog.pg_get_indexdef" in indexes
    assert indexes is not None and "ix.indispartial" not in indexes
    assert (
        indexes is not None
        and "pg_catalog.pg_get_expr(ix.indpred, ix.indrelid)::text IS NOT NULL AS is_partial" in indexes
    )
    assert indexes is not None and "::text" in indexes
    assert dependencies is not None and "pg_catalog.pg_depend" in dependencies
    assert ddl is not None and "pg_catalog.pg_get_" in ddl


@pytest.mark.parametrize(("domain", "query_name"), COCKROACH_STABLE_DOMAIN_QUERIES)
def test_cockroach_replacement_stable_query_pack_is_available(domain: str, query_name: str) -> None:
    """CockroachDB default metadata uses stable information_schema/pg_catalog surfaces."""
    query = DataDictionaryLoader().get_domain_query("cockroach", domain, query_name)

    assert query.is_supported is True
    assert query.dialect == "cockroachdb"
    assert query.domain == domain
    assert query.query_text is not None
    assert "crdb_internal" not in query.query_text


def test_cockroach_crdb_internal_query_is_feature_gated_by_default() -> None:
    """CockroachDB internal metadata must be explicit opt-in, not default metadata."""
    query = DataDictionaryLoader().get_domain_query(
        "cockroachdb", "crdb_internal", "ranges", required_features=("supports_crdb_internal_metadata",)
    )

    assert query.is_supported is False
    assert query.sql is None
    assert query.capability.support == MetadataSupport.UNSUPPORTED
    assert query.capability.risks == (MetadataRisk.VERSION_GATED,)
    assert query.capability.warnings == ("cockroachdb/crdb_internal/ranges requires supports_crdb_internal_metadata",)
