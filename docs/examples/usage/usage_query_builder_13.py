def test_example_13():
    # start-example
    # Update multiple columns
    query = (
        sql.update("users")
        .set("name", "?")
        .set("email", "?")
        .set("updated_at", "CURRENT_TIMESTAMP")
        .where("id = ?")
    )

    session.execute(query, "New Name", "newemail@example.com", 1)
    # end-example
