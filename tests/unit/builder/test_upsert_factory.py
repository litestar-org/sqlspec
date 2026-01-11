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


def test_upsert_with_empty_table_name_passes() -> None:
    """Test sql.upsert() with empty table name passes (validated later)."""
    builder = sql.upsert("", dialect="postgres")
    assert isinstance(builder, Merge)


def test_upsert_with_whitespace_only_table_name_passes() -> None:
    """Test sql.upsert() with whitespace-only table name passes (validated later)."""
    builder = sql.upsert("   ", dialect="postgres")
    assert isinstance(builder, Merge)


def test_upsert_with_invalid_dialect_returns_insert() -> None:
    """Test sql.upsert() with unsupported dialect returns INSERT."""
    builder = sql.upsert("products", dialect="invalid_dialect")
    assert isinstance(builder, Insert)


def test_upsert_with_none_dialect_uses_factory_default() -> None:
    """Test sql.upsert() with None dialect uses factory default."""
    factory_with_postgres = sql.__class__(dialect="postgres")
    builder = factory_with_postgres.upsert("products", dialect=None)
    assert isinstance(builder, Merge)
    assert builder.dialect_name == "postgres"


def test_upsert_case_insensitive_dialect() -> None:
    """Test sql.upsert() handles case-insensitive dialect names."""
    builder_upper = sql.upsert("products", dialect="POSTGRES")
    builder_lower = sql.upsert("products", dialect="postgres")
    builder_mixed = sql.upsert("products", dialect="PostgreSQL")

    assert all(isinstance(b, Merge) for b in [builder_upper, builder_lower, builder_mixed])


def test_upsert_dialect_aliases() -> None:
    """Test sql.upsert() recognizes dialect aliases."""
    pg_builder = sql.upsert("products", dialect="postgresql")
    assert isinstance(pg_builder, Merge)
    assert pg_builder.dialect_name in ("postgres", "postgresql")


def test_upsert_preserves_table_name() -> None:
    """Test sql.upsert() preserves table name correctly."""
    builder = sql.upsert("my_products_table", dialect="postgres")
    assert isinstance(builder, Merge)

    query = (
        builder
        .using({"id": 1, "name": "Test"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    built = query.build()
    assert "my_products_table" in built.sql


def test_upsert_returns_new_instance_each_time() -> None:
    """Test sql.upsert() returns new builder instance each call."""
    builder1 = sql.upsert("products", dialect="postgres")
    builder2 = sql.upsert("products", dialect="postgres")

    assert builder1 is not builder2
    assert isinstance(builder1, Merge)
    assert isinstance(builder2, Merge)


def test_upsert_merge_supports_all_when_clauses() -> None:
    """Test sql.upsert() MERGE builder supports all WHEN clause types."""
    builder = sql.upsert("products", dialect="postgres")
    assert isinstance(builder, Merge)

    query = (
        builder
        .using({"id": 1, "name": "Test"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
        .when_not_matched_by_source_then_delete()
    )

    built = query.build()
    assert "WHEN MATCHED THEN UPDATE" in built.sql
    assert "WHEN NOT MATCHED THEN INSERT" in built.sql
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in built.sql


def test_upsert_insert_supports_on_conflict() -> None:
    """Test sql.upsert() INSERT builder supports ON CONFLICT."""
    builder = sql.upsert("products", dialect="sqlite")
    assert isinstance(builder, Insert)

    query = builder.values(id=1, name="Product 1").on_conflict("id").do_update(name="EXCLUDED.name")

    built = query.build()
    assert "INSERT INTO" in built.sql
    assert "ON CONFLICT" in built.sql
    assert "DO UPDATE" in built.sql


def test_upsert_insert_supports_on_duplicate_key() -> None:
    """Test sql.upsert() INSERT builder supports MySQL ON DUPLICATE KEY."""
    builder = sql.upsert("products", dialect="mysql")
    assert isinstance(builder, Insert)

    query = builder.values(id=1, name="Product 1").on_duplicate_key_update(name="VALUES(name)")

    built = query.build()
    assert "INSERT INTO" in built.sql
    assert "ON DUPLICATE KEY UPDATE" in built.sql


def test_upsert_sql_server_returns_insert() -> None:
    """Test sql.upsert() returns INSERT for SQL Server (not in merge_supported list)."""
    builder = sql.upsert("products", dialect="tsql")
    assert isinstance(builder, Insert)


def test_upsert_teradata_returns_insert() -> None:
    """Test sql.upsert() returns INSERT for Teradata (not in merge_supported list)."""
    builder = sql.upsert("products", dialect="teradata")
    assert isinstance(builder, Insert)


def test_upsert_with_complex_table_name() -> None:
    """Test sql.upsert() handles complex table names (schema.table)."""
    builder = sql.upsert("schema.products", dialect="postgres")
    assert isinstance(builder, Merge)

    query = (
        builder
        .using({"id": 1, "name": "Test"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    built = query.build()
    assert "schema.products" in built.sql or "schema" in built.sql


def test_upsert_multiple_different_dialects() -> None:
    """Test sql.upsert() with multiple different dialects in sequence."""
    pg_builder = sql.upsert("products", dialect="postgres")
    sqlite_builder = sql.upsert("products", dialect="sqlite")
    oracle_builder = sql.upsert("products", dialect="oracle")

    assert isinstance(pg_builder, Merge)
    assert isinstance(sqlite_builder, Insert)
    assert isinstance(oracle_builder, Merge)

    assert id(pg_builder) != id(oracle_builder)


def test_upsert_does_not_mutate_factory() -> None:
    """Test sql.upsert() does not mutate factory state."""
    factory = sql.__class__(dialect="postgres")
    original_dialect_obj = factory.dialect

    factory.upsert("products", dialect="postgres")
    factory.upsert("products", dialect="oracle")
    factory.upsert("products", dialect="sqlite")

    assert factory.dialect is original_dialect_obj


def test_upsert_with_quoted_table_name() -> None:
    """Test sql.upsert() handles quoted table names."""
    builder = sql.upsert('"Products"', dialect="postgres")
    assert isinstance(builder, Merge)

    query = (
        builder
        .using({"id": 1, "name": "Test"}, alias="src")
        .on("t.id = src.id")
        .when_matched_then_update(name="src.name")
        .when_not_matched_then_insert(id="src.id", name="src.name")
    )

    built = query.build()
    assert '"Products"' in built.sql or "Products" in built.sql
