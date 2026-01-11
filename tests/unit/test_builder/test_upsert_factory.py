"""Unit tests for sql.upsert() factory method."""

from sqlspec import sql
from sqlspec.builder import Insert, Merge


def test_upsert_returns_merge_for_postgres() -> None:
    """Test sql.upsert() returns MERGE builder for PostgreSQL."""
    builder = sql.upsert("products", dialect="postgres")
    assert isinstance(builder, Merge)
    assert builder.dialect_name == "postgres"


def test_upsert_returns_merge_for_oracle() -> None:
    """Test sql.upsert() returns MERGE builder for Oracle."""
    builder = sql.upsert("products", dialect="oracle")
    assert isinstance(builder, Merge)
    assert builder.dialect_name == "oracle"


def test_upsert_returns_merge_for_bigquery() -> None:
    """Test sql.upsert() returns MERGE builder for BigQuery."""
    builder = sql.upsert("products", dialect="bigquery")
    assert isinstance(builder, Merge)
    assert builder.dialect_name == "bigquery"


def test_upsert_returns_insert_for_sqlite() -> None:
    """Test sql.upsert() returns INSERT builder for SQLite."""
    builder = sql.upsert("products", dialect="sqlite")
    assert isinstance(builder, Insert)
    assert builder.dialect_name == "sqlite"


def test_upsert_returns_insert_for_duckdb() -> None:
    """Test sql.upsert() returns INSERT builder for DuckDB."""
    builder = sql.upsert("products", dialect="duckdb")
    assert isinstance(builder, Insert)
    assert builder.dialect_name == "duckdb"


def test_upsert_returns_insert_for_mysql() -> None:
    """Test sql.upsert() returns INSERT builder for MySQL."""
    builder = sql.upsert("products", dialect="mysql")
    assert isinstance(builder, Insert)
    assert builder.dialect_name == "mysql"


def test_upsert_uses_factory_default_dialect() -> None:
    """Test sql.upsert() uses factory default dialect when not specified."""
    factory_with_postgres = sql.__class__(dialect="postgres")
    builder = factory_with_postgres.upsert("products")
    assert isinstance(builder, Merge)
    assert builder.dialect_name == "postgres"


def test_upsert_postgres_builder_chain() -> None:
    """Test sql.upsert() with full PostgreSQL MERGE builder chain."""
    builder = sql.upsert("products", dialect="postgres")
    assert isinstance(builder, Merge)

    query = (
        builder
        .using([{"id": 1, "name": "Product 1"}], alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    built = query.build()
    assert "MERGE INTO products" in built.sql
    assert "WHEN MATCHED THEN UPDATE" in built.sql
    assert "WHEN NOT MATCHED THEN INSERT" in built.sql


def test_upsert_sqlite_builder_chain() -> None:
    """Test sql.upsert() with full SQLite INSERT ON CONFLICT builder chain."""
    builder = sql.upsert("products", dialect="sqlite")
    assert isinstance(builder, Insert)

    query = builder.values(id=1, name="Product 1").on_conflict("id").do_update(name="EXCLUDED.name")

    built = query.build()
    assert "INSERT INTO" in built.sql
    assert "products" in built.sql
    assert "ON CONFLICT" in built.sql
    assert "DO UPDATE" in built.sql
