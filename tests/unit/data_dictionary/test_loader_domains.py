"""Unit tests for namespaced data-dictionary query loading."""

from sqlspec.core import SQL
from sqlspec.data_dictionary import (
    DataDictionaryLoader,
    MetadataQuery,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    VersionInfo,
)


def test_domain_query_lookup() -> None:
    """Domain query lookup resolves dialect aliases to direct domain paths."""
    loader = DataDictionaryLoader()

    query = loader.get_domain_query("postgresql", "objects", "by_schema")

    assert isinstance(query, MetadataQuery)
    assert query.dialect == "postgres"
    assert query.domain == "objects"
    assert query.name == "by_schema"
    assert query.mode is None
    assert query.is_supported is True
    assert query.capability.support == MetadataSupport.SUPPORTED
    assert isinstance(query.sql, SQL)
    assert query.query_text is not None
    assert "information_schema.tables" in query.query_text
    assert loader.get_domain_query_text("postgres", "objects", "by_schema") == query.query_text


def test_missing_domain_returns_unsupported_status() -> None:
    """Missing domain query files report unsupported instead of false-empty metadata."""
    loader = DataDictionaryLoader()

    query = loader.get_domain_query("postgres", "privileges", "by_schema")

    assert query.is_supported is False
    assert query.sql is None
    assert query.query_text is None
    assert query.capability.support == MetadataSupport.UNSUPPORTED
    assert query.capability.source == MetadataSource.UNKNOWN
    assert query.capability.warnings == ("No data-dictionary query found for postgres/privileges/by_schema",)


def test_dialect_mode_dispatch() -> None:
    """Server products with multiple SQL modes can dispatch distinct packs."""
    loader = DataDictionaryLoader()

    googlesql = loader.get_domain_query("spanner", "objects", "by_schema", mode="googlesql")
    postgresql = loader.get_domain_query("spanner", "objects", "by_schema", mode="postgresql")

    assert googlesql.is_supported is True
    assert postgresql.is_supported is True
    assert googlesql.dialect == "spanner"
    assert googlesql.mode == "googlesql"
    assert postgresql.mode == "postgresql"
    assert googlesql.query_text is not None
    assert postgresql.query_text is not None
    assert googlesql.query_text != postgresql.query_text
    assert "INFORMATION_SCHEMA.TABLES" in googlesql.query_text
    assert "information_schema.tables" in postgresql.query_text


def test_version_and_feature_gates_return_unsupported_status() -> None:
    """Version and feature gates are reported through the query capability."""
    loader = DataDictionaryLoader()

    query = loader.get_domain_query(
        "postgres",
        "objects",
        "by_schema",
        version=VersionInfo(8, 3, 0),
        required_features=("supports_window_functions",),
    )

    assert query.is_supported is False
    assert query.sql is None
    assert query.capability.support == MetadataSupport.UNSUPPORTED
    assert query.capability.risks == (MetadataRisk.VERSION_GATED,)
    assert query.capability.warnings == ("postgres/objects/by_schema requires supports_window_functions >= 8.4.0",)


def test_domain_query_batch_lookup_reuses_domain_results() -> None:
    """Batch lookup returns one structured result per requested query name."""
    loader = DataDictionaryLoader()

    queries = loader.get_domain_queries("postgres", "objects", ("by_schema", "missing"))

    assert tuple(queries) == ("by_schema", "missing")
    assert queries["by_schema"].is_supported is True
    assert queries["missing"].is_supported is False
