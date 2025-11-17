def test_example_18():
    # start-example
    # Drop table
    query = sql.drop_table("users")

    # Drop if exists
    query = sql.drop_table("users").if_exists()

    session.execute(query)
    # end-example
