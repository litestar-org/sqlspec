def test_example_3():
    # start-example
    # Simple WHERE
    query = sql.select("*").from_("users").where("status = ?")

    # Multiple conditions (AND)
    query = (
        sql.select("*")
        .from_("users")
        .where("status = ?")
        .where("created_at > ?")
    )
    # SQL: SELECT * FROM users WHERE status = ? AND created_at > ?

    # OR conditions
    query = (
        sql.select("*")
        .from_("users")
        .where("status = ? OR role = ?")
    )

    # IN clause
    query = sql.select("*").from_("users").where("id IN (?, ?, ?)")
    # end-example
