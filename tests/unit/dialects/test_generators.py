"""Tests for dialect generator rendering with sqlglot[c] compatibility.

Verifies that the conditional TRANSFORMS/subclass approach in _generators.py
produces correct SQL output for all extension operators and Spanner DDL.
"""

from sqlglot import parse_one

import sqlspec.dialects  # noqa: F401

# ---------------------------------------------------------------------------
# PGVector operator rendering
# ---------------------------------------------------------------------------


def test_pgvector_l2_distance() -> None:
    sql = "SELECT embedding <-> '[1,2,3]' FROM items"
    rendered = parse_one(sql, dialect="pgvector").sql(dialect="pgvector")
    assert "<->" in rendered


def test_pgvector_cosine_distance() -> None:
    sql = "SELECT embedding <=> '[1,2,3]' FROM items"
    rendered = parse_one(sql, dialect="pgvector").sql(dialect="pgvector")
    assert "<=>" in rendered


def test_pgvector_inner_product_distance() -> None:
    sql = "SELECT embedding <#> '[1,2,3]' FROM items"
    rendered = parse_one(sql, dialect="pgvector").sql(dialect="pgvector")
    assert "<#>" in rendered


# ---------------------------------------------------------------------------
# ParadeDB operator rendering
# ---------------------------------------------------------------------------


def test_paradedb_bm25_search() -> None:
    sql = "SELECT * FROM items WHERE description @@@ 'shoes'"
    rendered = parse_one(sql, dialect="paradedb").sql(dialect="paradedb")
    assert "@@@" in rendered


def test_paradedb_inherits_pgvector_operators() -> None:
    sql = "SELECT embedding <=> '[1,2,3]' FROM items"
    rendered = parse_one(sql, dialect="paradedb").sql(dialect="paradedb")
    assert "<=>" in rendered


# ---------------------------------------------------------------------------
# Spanner DDL rendering
# ---------------------------------------------------------------------------


def test_spanner_interleave_in_parent() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    """
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_spanner_row_deletion_policy() -> None:
    sql = """
    CREATE TABLE logs (
        id STRING(36),
        created_at TIMESTAMP,
        PRIMARY KEY (id)
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
    """
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "ROW DELETION POLICY" in rendered
    assert "OLDER_THAN(created_at" in rendered


def test_spanner_ttl() -> None:
    sql = """
    CREATE TABLE ttl_table (
        id INT64,
        expires_at TIMESTAMP,
        PRIMARY KEY (id)
    ) TTL INTERVAL '5 days' ON expires_at
    """
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "TTL INTERVAL" in rendered
    assert "expires_at" in rendered


# ---------------------------------------------------------------------------
# Spangres DDL rendering
# ---------------------------------------------------------------------------


def test_spangres_row_deletion_policy() -> None:
    sql = """
    CREATE TABLE events (
        id VARCHAR PRIMARY KEY,
        created_at TIMESTAMP
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))
    """
    rendered = parse_one(sql, dialect="spangres").sql(dialect="spangres")
    assert "ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))" in rendered
