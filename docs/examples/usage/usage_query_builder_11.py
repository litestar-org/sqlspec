def test_example_11():
    # start-example
    # PostgreSQL RETURNING clause
    query = (
        sql.insert("users")
        .columns("name", "email")
        .values("?", "?")
        .returning("id", "created_at")
    )

    result = session.execute(query, "Alice", "alice@example.com")
    new_user = result.one()
    print(f"Created user ID: {new_user['id']}")
    # end-example
