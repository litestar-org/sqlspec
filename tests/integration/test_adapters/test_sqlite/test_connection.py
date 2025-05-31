"""Test SQLite connection configuration."""

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.statement.result import SelectResult


@pytest.mark.xdist_group("sqlite")
def test_connection() -> None:
    """Test connection components."""
    # Test direct connection
    config = SqliteConfig(database=":memory:")

    with config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result == (1,)
        cur.close()

    # Test session management
    with config.provide_session() as session:
        assert session is not None
        # Test basic query through session
        select_result = session.execute("SELECT 1")
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.column_names is not None
        result = select_result.rows[0][select_result.column_names[0]]
        assert result == 1
