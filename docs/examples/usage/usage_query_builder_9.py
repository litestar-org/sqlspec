def test_example_9():
    # start-example
    from sqlspec import sql

    # Single row insert
    query = sql.insert("users").columns("name", "email").values("?", "?")
    # SQL: INSERT INTO users (name, email) VALUES (?, ?)

    result = session.execute(query, "Alice", "alice@example.com")
    # end-example
