from pathlib import Path

from sqlspec import SQLFileLoader

__all__ = ("create_loader", "test_loader_loads_queries" )


def create_loader(tmp_path: Path) -> tuple[SQLFileLoader, list[str]]:
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()

    sql_file_1 = sql_dir / "queries" / "users.sql"
    sql_file_1.parent.mkdir()
    sql_file_1.write_text("""
    -- name: get_user_by_id)
    SELECT * FROM users WHERE id = :user_id;
    -- name: list_active_users
    SELECT * FROM users WHERE active = 1;
    -- name: create_user
    INSERT INTO users (name, email) VALUES (:name, :email);
    """)
    # start-example
    from sqlspec.loader import SQLFileLoader

    # Create loader
    loader = SQLFileLoader()

    # Load SQL files
    loader.load_sql(sql_file_1)

    # Or load from a directory
    loader.load_sql(sql_dir)

    # List available queries
    queries = loader.list_queries()
    print(queries)  # ['get_user_by_id', 'list_active_users', 'create_user', ...]
    # end-example
    return loader, queries


def test_loader_loads_queries(tmp_path: Path) -> None:

    loader, queries = create_loader(tmp_path)
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
    assert hasattr(loader, "list_queries")
    assert isinstance(queries, list)
