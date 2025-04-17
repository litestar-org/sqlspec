"""Test DuckDB connection configuration."""

from sqlspec.adapters.duckdb.config import DuckDB


def test_connection() -> None:
    """Test connection components."""
    # Test direct connection
    config = DuckDB(database=":memory:")

    with config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()  # pyright: ignore
        assert result is not None
        assert result[0] == 1
        cur.close()

    # Test session management
    with config.provide_session() as session:
        assert session is not None
        # Test basic query through session
        result = session.select_value("SELECT 1", {})
