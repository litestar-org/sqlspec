def test_example_25():
    # start-example
    # Prefer this for simple, static queries:
    result = session.execute("SELECT * FROM users WHERE id = ?", 1)

    # Over this:
    query = sql.select("*").from_("users").where("id = ?")
    result = session.execute(query, 1)
    # end-example
