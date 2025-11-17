def test_example_1():
    # start-example
    from sqlspec import sql

    # Build SELECT query
    query = sql.select("id", "name", "email").from_("users").where("status = ?").order_by("created_at DESC").limit(10)

    # Execute with session
    result = session.execute(query, "active")
    # end-example
