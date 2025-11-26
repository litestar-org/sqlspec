from pathlib import Path


def test_integration_with_sqlspec(tmp_path: Path) -> None:
    tmp_sql_dir = tmp_path / "sql"
    tmp_sql_dir.mkdir()
    sql_file = tmp_sql_dir / "queries.sql"
    sql_file.write_text("""
    -- name: get_user_by_id
    SELECT * FROM users WHERE id = :user_id;
    """)
    # start-example
    from sqlspec import SQLSpec
    from sqlspec.loader import SQLFileLoader

    # Create loader
    loader = SQLFileLoader()
    loader.load_sql(tmp_path / "sql/")

    # Create SQLSpec with loader
    spec = SQLSpec(loader=loader)

    # Access loader via SQLSpec
    user_query = spec._sql_loader.get_sql("get_user_by_id")
    # end-example
    # Dummy asserts for doc example
    assert user_query is not None
