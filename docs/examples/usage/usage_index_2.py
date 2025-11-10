__all__ = ("test_index_2",)


def test_index_2() -> None:
    # start-example
    from sqlspec import SQLSpec, sql
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig())
    query = sql.select("id", "name", "email").from_("users").where("active = ?")

    with db_manager.provide_session(db) as session:
        session.execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                active BOOLEAN NOT NULL DEFAULT 1
            )
            """
        )
        session.execute(
            """
            INSERT INTO users VALUES
                (1, 'alice', 'alice@example.com', 1),
                (2, 'bob', 'bob@example.com', 0),
                (3, 'carol', 'carol@example.com', 1)
            """
        )
        users = session.select(query, True)  # noqa: FBT003
    # end-example

    names = [user["name"] for user in users]
    assert names == ["alice", "carol"]
