"""Example 3: From SQL Files."""

from pathlib import Path


def test_sql_file_loader() -> None:
    """Test loading SQL from files."""
    # start-example
    from sqlspec.loader import SQLFileLoader

    loader = SQLFileLoader()
    queries_path = Path(__file__).resolve().parents[1] / "queries" / "users.sql"
    loader.load_sql(queries_path)
    sql = loader.get_sql("get_user_by_id")
    # end-example

    # Verify SQL object was created
    assert sql is not None

