from sqlglot import parse_one

from sqlspec.adapters.spanner.dialect import Spanner


def test_parse_interleave_in_parent() -> None:
    """Test parsing INTERLEAVE IN PARENT clause."""
    sql = """
    CREATE TABLE child (
        parent_id STRING(36),
        child_id INT64,
        PRIMARY KEY (parent_id, child_id)
    ) INTERLEAVE IN PARENT parent ON DELETE CASCADE
    """
    ast = parse_one(sql, read=Spanner)
    # Properties attached to Schema (ast.this)
    assert ast.this.args.get("interleave_parent").this.name == "parent"
    assert ast.this.args.get("interleave_on_delete") == "CASCADE"


def test_parse_ttl_clause() -> None:
    """Test parsing TTL INTERVAL clause."""
    sql = """
    CREATE TABLE orders (
        order_id INT64,
        created_at TIMESTAMP,
        PRIMARY KEY (order_id)
    ) TTL INTERVAL '30 days' ON created_at
    """
    ast = parse_one(sql, read=Spanner)
    # Verify TTL property parsed correctly
    assert ast.sql(dialect=Spanner)
    properties = ast.args.get("properties")
    ttl_prop = None
    if properties is not None and getattr(properties, "expressions", None):
        ttl_prop = next((p for p in properties.expressions if getattr(p.this, "name", "") == "TTL"), None)
    assert ttl_prop is not None


def test_generate_interleave_syntax() -> None:
    """Test generating INTERLEAVE IN PARENT clause."""
    sql = """CREATE TABLE child (
  parent_id STRING(36),
  child_id INT64,
  PRIMARY KEY (parent_id, child_id)
)
INTERLEAVE IN PARENT parent ON DELETE CASCADE"""

    ast = parse_one(sql, read=Spanner)
    generated = ast.sql(dialect=Spanner)
    assert "INTERLEAVE IN PARENT parent ON DELETE CASCADE" in generated


def test_generate_ttl_syntax() -> None:
    """Test generating TTL clause."""
    sql = """CREATE TABLE orders (
  order_id INT64,
  created_at TIMESTAMP,
  PRIMARY KEY (order_id)
)
TTL INTERVAL '30 days' ON created_at"""

    ast = parse_one(sql, read=Spanner)
    generated = ast.sql(dialect=Spanner)
    assert "TTL INTERVAL '30 days' ON created_at" in generated


def test_roundtrip_interleaved_table() -> None:
    """Parse -> Generate -> Parse should be idempotent."""
    original_sql = "CREATE TABLE child (id INT64, PRIMARY KEY (id)) INTERLEAVE IN PARENT parent"
    ast = parse_one(original_sql, read=Spanner)
    generated = ast.sql(dialect=Spanner)
    reparsed = parse_one(generated, read=Spanner)
    assert generated == reparsed.sql(dialect=Spanner)
