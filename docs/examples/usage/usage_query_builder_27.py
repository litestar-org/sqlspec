def test_example_27():
    # start-example
    from sqlspec import sql

    # Always use placeholders for user input
    search_term = user_input  # From user
    query = sql.select("*").from_("users").where("name LIKE ?")
    result = session.execute(query, f"%{search_term}%")
    # end-example
