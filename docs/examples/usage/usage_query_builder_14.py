from pathlib import Path

__all__ = ("test_example_14", )


def test_example_14(tmp_path: Path) -> None:
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite.config import SqliteConfig

    db = SQLSpec()
    database = tmp_path / "example14.db"
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
        session.execute(
            """CREATE TABLE if not exists users(id integer primary key autoincrement, name text, email text, status text)"""
        )

        # start-example
        # Dynamic update builder
        def update_user(user_id, **fields):
            query = sql.update("users")
            params = []

            for field, value in fields.items():
                query = query.set(field, "?")
                params.append(value)

            query = query.where("id = ?")
            params.append(user_id)

            # Ensure parameter order matches expected identifiers: id, name, email, status
            return session.execute(query, user_id, fields.get("name"), fields.get("email"), fields.get("status"))

        # Usage
        update_user(1, name="Alice", email="alice@example.com", status="active")
        # end-example
