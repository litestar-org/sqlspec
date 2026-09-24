"""Tests for Spanner foreign keys query performance and predicate pushdown."""

from sqlspec.data_dictionary import get_data_dictionary_loader


def test_foreign_keys_googlesql_predicate_pushdown() -> None:
    """GoogleSQL foreign keys by_table query pushes table_name into fk_columns CTE."""
    loader = get_data_dictionary_loader()
    query = loader.get_domain_query("spanner", "foreign_keys", "by_table", mode="googlesql")

    assert query.is_supported is True
    assert query.sql is not None
    sql_text = query.sql.raw_sql

    assert "fk_columns AS (" in sql_text
    assert "pk_columns AS (" in sql_text

    fk_part = sql_text[sql_text.index("fk_columns AS (") : sql_text.index("pk_columns AS (")]
    assert "CAST(:table_name AS STRING) IS NULL OR table_name = :table_name" in fk_part

    pk_part = sql_text[sql_text.index("pk_columns AS (") : sql_text.index("SELECT\n    fk.table_name")]
    assert "table_name = :table_name" not in pk_part


def test_foreign_keys_postgresql_predicate_pushdown() -> None:
    """PostgreSQL foreign keys by_table query pushes table_name into fk_columns CTE."""
    loader = get_data_dictionary_loader()
    query = loader.get_domain_query("spanner", "foreign_keys", "by_table", mode="postgresql")

    assert query.is_supported is True
    assert query.sql is not None
    sql_text = query.sql.raw_sql

    assert "fk_columns AS (" in sql_text
    assert "pk_columns AS (" in sql_text

    fk_part = sql_text[sql_text.index("fk_columns AS (") : sql_text.index("pk_columns AS (")]
    assert ":table_name::text IS NULL OR table_name = :table_name" in fk_part

    pk_part = sql_text[sql_text.index("pk_columns AS (") : sql_text.index("SELECT\n    fk.table_name")]
    assert "table_name = :table_name" not in pk_part
