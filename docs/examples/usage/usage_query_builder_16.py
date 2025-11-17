def test_example_16():
    # start-example
    # Delete with multiple conditions
    query = (
        sql.delete("users")
        .where("status = ?")
        .where("last_login < ?")
    )

    session.execute(query, "inactive", datetime.date(2024, 1, 1))
    # end-example
