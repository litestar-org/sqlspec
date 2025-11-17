def test_example_15():
    # start-example
    from sqlspec import sql

    # Delete with WHERE
    query = sql.delete("users").where("id = ?")
    # SQL: DELETE FROM users WHERE id = ?

    result = session.execute(query, 1)
    print(f"Deleted {result.rows_affected} rows")
    # end-example
