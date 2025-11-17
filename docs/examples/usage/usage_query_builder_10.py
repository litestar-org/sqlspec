def test_example_10():
    # start-example
    # Multiple value sets
    query = (
        sql.insert("users")
        .columns("name", "email")
        .values("?", "?")
        .values("?", "?")
        .values("?", "?")
    )

    session.execute(
        query,
        "Alice", "alice@example.com",
        "Bob", "bob@example.com",
        "Charlie", "charlie@example.com"
    )
    # end-example
