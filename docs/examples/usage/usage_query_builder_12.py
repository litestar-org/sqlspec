def test_example_12():
    # start-example
    from sqlspec import sql

    # Update with WHERE
    query = (
        sql.update("users")
        .set("email", "?")
        .where("id = ?")
    )
    # SQL: UPDATE users SET email = ? WHERE id = ?

    result = session.execute(query, "newemail@example.com", 1)
    print(f"Updated {result.rows_affected} rows")
    # end-example
