from pathlib import Path

__all__ = ("test_user_management_example", )


def test_user_management_example(tmp_path: Path) -> None:
    user_sql_path = tmp_path / "sql"
    user_sql_path.mkdir(parents=True, exist_ok=True)
    user_sql_file = user_sql_path / "users.sql"
    user_sql_file.write_text(
        """-- name: create_user
        INSERT INTO users (username, email, password_hash) VALUES (:username, :email, :password_hash) RETURNING id, username, email;
        -- name: get_user
        SELECT id, username, email FROM users WHERE id = :user_id;
        -- name: list_users
        SELECT id, username, email FROM users WHERE (:status IS NULL OR active = :status) LIMIT :limit OFFSET :offset;
        """
    )
    # start-example
    # Python code
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.loader import SQLFileLoader

    loader = SQLFileLoader()
    loader.load_sql(tmp_path / "sql/users.sql")

    spec = SQLSpec()
    config = SqliteConfig()
    spec.add_config(config)

    with spec.provide_session(config) as session:
        session.execute(
            """CREATE TABLE users ( id INTEGER PRIMARY KEY, username TEXT, email TEXT, password_hash TEXT, active BOOLEAN DEFAULT 1)"""
        )
        # Create user
        create_query = loader.get_sql("create_user")
        result = session.execute(
            create_query, username="irma", email="irma@example.com", password_hash="hashed_password"
        )
        user = result.one()
        user_id = user["id"]

        # Get user
        get_query = loader.get_sql("get_user")
        user = session.execute(get_query, user_id=user_id).one()

        # List users
        list_query = loader.get_sql("list_users")
        session.execute(list_query, status=True, limit=10, offset=0).data
    # end-example
    # Dummy asserts for doc example
    assert hasattr(loader, "load_sql")
    assert hasattr(spec, "add_config")
