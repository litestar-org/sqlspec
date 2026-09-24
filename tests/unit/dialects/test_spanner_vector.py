"""Unit tests for Cloud Spanner Vector Search functions, types, and DDL."""

from sqlglot import exp, parse_one

from sqlspec.dialects.spanner._expressions import ApproxCosineDistance, CosineDistance, DotProduct, EuclideanDistance


def test_cosine_distance_parsing_and_generation() -> None:
    """Verify COSINE_DISTANCE parses into typed AST node and round-trips."""
    sql = "SELECT COSINE_DISTANCE(v1, v2) AS dist FROM items"
    parsed = parse_one(sql, dialect="spanner")
    col = parsed.find(CosineDistance)
    assert col is not None
    assert col.name == ""
    assert isinstance(col, CosineDistance)
    rendered = parsed.sql(dialect="spanner")
    assert "COSINE_DISTANCE(v1, v2)" in rendered


def test_euclidean_distance_parsing_and_generation() -> None:
    """Verify EUCLIDEAN_DISTANCE parses into typed AST node and round-trips."""
    sql = "SELECT EUCLIDEAN_DISTANCE(v1, v2) AS dist FROM items"
    parsed = parse_one(sql, dialect="spanner")
    col = parsed.find(EuclideanDistance)
    assert col is not None
    assert isinstance(col, EuclideanDistance)
    rendered = parsed.sql(dialect="spanner")
    assert "EUCLIDEAN_DISTANCE(v1, v2)" in rendered


def test_dot_product_parsing_and_generation() -> None:
    """Verify DOT_PRODUCT parses into typed AST node and round-trips."""
    sql = "SELECT DOT_PRODUCT(v1, v2) AS dist FROM items"
    parsed = parse_one(sql, dialect="spanner")
    col = parsed.find(DotProduct)
    assert col is not None
    assert isinstance(col, DotProduct)
    rendered = parsed.sql(dialect="spanner")
    assert "DOT_PRODUCT(v1, v2)" in rendered


def test_approx_cosine_distance_with_options() -> None:
    """Verify APPROX_COSINE_DISTANCE parses options and round-trips."""
    sql = "SELECT APPROX_COSINE_DISTANCE(v1, v2, 100) AS dist FROM items"
    parsed = parse_one(sql, dialect="spanner")
    col = parsed.find(ApproxCosineDistance)
    assert col is not None
    assert isinstance(col, ApproxCosineDistance)
    rendered = parsed.sql(dialect="spanner")
    assert "APPROX_COSINE_DISTANCE(v1, v2, 100)" in rendered


def test_vector_transpile_spanner_to_spangres() -> None:
    """Verify vector distance functions transpile cleanly from Spanner to Spangres."""
    sql = "SELECT COSINE_DISTANCE(v1, v2) AS dist FROM items"
    rendered = parse_one(sql, dialect="spanner").sql(dialect="spangres")
    assert "COSINE_DISTANCE(v1, v2)" in rendered


def test_float32_array_type_parsing_and_generation() -> None:
    """Verify ARRAY<FLOAT32> column type parses and generates properly in Spanner."""
    sql = "CREATE TABLE items (id INT64, embedding ARRAY<FLOAT32>) PRIMARY KEY (id)"
    parsed = parse_one(sql, dialect="spanner")
    rendered = parsed.sql(dialect="spanner")
    assert "ARRAY<FLOAT32>" in rendered


def test_create_vector_index_basic() -> None:
    """Verify CREATE VECTOR INDEX parses and generates canonical DDL."""
    sql = (
        "CREATE VECTOR INDEX item_embeddings_idx ON items (embedding_col) "
        "OPTIONS (dimension = 1536, distance_type = 'COSINE')"
    )
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Index)
    assert parsed.args.get("kind") == "VECTOR"
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE VECTOR INDEX item_embeddings_idx ON items (embedding_col)" in rendered
    assert "dimension = 1536" in rendered
    assert "distance_type = 'COSINE'" in rendered


def test_create_vector_index_with_where_predicate() -> None:
    """Verify CREATE VECTOR INDEX with WHERE clause parses and generates correctly."""
    sql = (
        "CREATE VECTOR INDEX item_embeddings_idx ON items (embedding_col) "
        "WHERE in_stock IS TRUE OPTIONS (dimension = 1536, distance_type = 'COSINE')"
    )
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Index)
    assert parsed.args.get("where") is not None
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE VECTOR INDEX item_embeddings_idx ON items (embedding_col)" in rendered
    assert "WHERE in_stock IS TRUE" in rendered
    assert "dimension = 1536" in rendered
