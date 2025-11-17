from pathlib import Path

__all__ = ("test_example_4", )


def test_example_4(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example4.db"
    config = SqliteConfig(
        pool_config={
            "database": database.name,
            "timeout": 5.0,
            "check_same_thread": False,
            "cached_statements": 100,
            "uri": False,
        }
    )
    with db.provide_session(config) as session:
        create_table_query = """CREATE TABLE if not exists users(id int primary key,name text,email text, status text, created_at timestamp)"""
        _ = session.execute(create_table_query)

        # start-example
        def search_users(name=None, email=None, status=None):
            query = sql.select("id", "name", "email", "status").from_("users")
            params = []

            if name:
                query = query.where("name LIKE ?")
                params.append(f"%{name}%")

            if email:
                query = query.where("email = ?")
                params.append(email)

            if status:
                query = query.where("status = ?")
                params.append(status)

            return session.execute(query, *params)

        # Usage
        search_users(name="Alice", status="active")
        # end-example
