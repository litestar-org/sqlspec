def test_example_29():
    # start-example
    from sqlspec import sql

    # Check generated SQL during development
    query = sql.select("*").from_("users").where("id = ?")
    print(query)  # Shows generated SQL
    # end-example
