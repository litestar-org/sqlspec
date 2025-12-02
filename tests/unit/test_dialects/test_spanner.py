"""Dialect unit tests for the custom Spanner dialect."""

from sqlglot import parse_one

import sqlspec.adapters.spanner


def _render(sql: str) -> str:
    assert sqlspec.adapters.spanner

    return parse_one(sql, dialect="spanner").sql(dialect="spanner")


def test_parse_and_generate_interleave_clause() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table" in rendered


def test_parse_interleave_with_on_delete_cascade() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_parse_ttl_clause_roundtrip() -> None:
    sql = """
    CREATE TABLE orders (
        order_id INT64,
        created_at TIMESTAMP,
        PRIMARY KEY (order_id)
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))
    """
    rendered = _render(sql)
    assert "ROW DELETION POLICY" in rendered
    assert "OLDER_THAN(created_at" in rendered


def test_roundtrip_interleave_and_ttl_together() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        expires_at TIMESTAMP,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
      ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION" in rendered
    assert "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 7 DAY))" in rendered


def test_interleave_on_delete_cascade() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_interleave_without_on_delete() -> None:
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent_table
    """
    rendered = _render(sql)
    assert "INTERLEAVE IN PARENT parent_table" in rendered
    assert "ON DELETE" not in rendered


def test_row_deletion_policy_interval_literal() -> None:
    sql = """
    CREATE TABLE logs (
        id STRING(36),
        created_at TIMESTAMP,
        PRIMARY KEY (id)
    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '90 days'))
    """
    rendered = _render(sql)
    assert "ROW DELETION POLICY" in rendered
    assert "INTERVAL '90 days'" in rendered


def test_legacy_ttl_pg_style_still_parses() -> None:
    sql = """
    CREATE TABLE ttl_table (
        id INT64,
        expires_at TIMESTAMP,
        PRIMARY KEY (id)
    ) TTL INTERVAL '5 days' ON expires_at
    """
    rendered = _render(sql)
    assert "TTL INTERVAL '5 days' ON expires_at" in rendered
