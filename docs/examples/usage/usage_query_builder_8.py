def test_example_8():
    # start-example
    # Subquery in WHERE
    subquery = sql.select("id").from_("orders").where("total > ?")
    query = (
        sql.select("*")
        .from_("users")
        .where(f"id IN ({subquery})")
    )

    # Subquery in FROM
    subquery = (
        sql.select("user_id", "COUNT(*) as order_count")
        .from_("orders")
        .group_by("user_id")
    )
    query = (
        sql.select("u.name", "o.order_count")
        .from_("users u")
        .join(f"({subquery}) o", "u.id = o.user_id")
    )
    # end-example
