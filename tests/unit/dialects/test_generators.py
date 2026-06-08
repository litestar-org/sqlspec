"""Tests for dialect generator rendering with sqlglot[c] compatibility.

Verifies that the conditional TRANSFORMS/subclass approach in _generators.py
produces correct SQL output for all extension operators and Spanner DDL.
"""

from pathlib import Path

from sqlglot import parse_one

from sqlspec.dialects.spanner._generators import _get_dialect_name


def test_get_dialect_name_returns_generator_dialect_class_name() -> None:

    class CustomDialect:
        pass

    class CustomGenerator:
        dialect = CustomDialect()

    assert _get_dialect_name(CustomGenerator()) == "CustomDialect"


def test_get_dialect_name_handles_none_dialect_like_existing_boilerplate() -> None:

    class CustomGenerator:
        dialect = None

    assert _get_dialect_name(CustomGenerator()) == "NoneType"


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


def test_paradedb_bm25_search() -> None:
    sql = "SELECT * FROM items WHERE description @@@ 'shoes'"
    rendered = parse_one(sql, dialect="paradedb").sql(dialect="paradedb")
    assert "@@@" in rendered


def test_paradedb_inherits_pgvector_operators() -> None:
    sql = "SELECT embedding <=> '[1,2,3]' FROM items"
    rendered = parse_one(sql, dialect="paradedb").sql(dialect="paradedb")
    assert "<=>" in rendered


def test_spanner_interleave_in_parent() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE\n    "
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "INTERLEAVE IN PARENT parent_table ON DELETE CASCADE" in rendered


def test_spanner_interleave_round_trips_after_generator_helper_extraction() -> None:
    sql = "\n    CREATE TABLE child (\n        parent_id STRING(36),\n        child_id INT64,\n        PRIMARY KEY (parent_id, child_id)\n    ) INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION\n    "
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION" in rendered


def test_spanner_row_deletion_policy() -> None:
    sql = "\n    CREATE TABLE logs (\n        id STRING(36),\n        created_at TIMESTAMP,\n        PRIMARY KEY (id)\n    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))\n    "
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "ROW DELETION POLICY" in rendered
    assert "OLDER_THAN(created_at" in rendered


def test_spanner_ttl() -> None:
    sql = "\n    CREATE TABLE ttl_table (\n        id INT64,\n        expires_at TIMESTAMP,\n        PRIMARY KEY (id)\n    ) TTL INTERVAL '5 days' ON expires_at\n    "
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spanner")
    assert "TTL INTERVAL" in rendered
    assert "expires_at" in rendered


def test_spangres_row_deletion_policy() -> None:
    sql = "\n    CREATE TABLE events (\n        id VARCHAR PRIMARY KEY,\n        created_at TIMESTAMP\n    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))\n    "
    rendered = parse_one(sql, dialect="spangres").sql(dialect="spangres")
    assert "ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL '30 days'))" in rendered


def test_spangres_row_deletion_policy_round_trips_after_generator_helper_extraction() -> None:
    sql = "\n    CREATE TABLE events (\n        id VARCHAR PRIMARY KEY,\n        created_at TIMESTAMP\n    ) ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 30 DAY))\n    "
    rendered = parse_one(sql, dialect="spangres").sql(dialect="spangres")
    assert "ROW DELETION POLICY" in rendered
    assert "OLDER_THAN(created_at, INTERVAL 30 DAY)" in rendered


def test_compat_removed_dialects_compat_module_removed() -> None:
    assert not Path("sqlspec/dialects/_compat.py").exists()


def test_compat_removed_dialects_compat_not_in_mypyc_include() -> None:
    assert "sqlspec/dialects/_compat.py" not in Path("pyproject.toml").read_text()
    assert "sqlspec/dialects/_compat.py" not in Path("Makefile").read_text()
