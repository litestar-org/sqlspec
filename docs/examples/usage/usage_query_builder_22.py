def test_example_22():
    # start-example
    # WITH clause
    cte = sql.select("user_id", "COUNT(*) as order_count").from_("orders").group_by("user_id")

    query = (
        sql.select("u.name", "c.order_count")
        .with_("user_orders", cte)
        .from_("users u")
        .join("user_orders c", "u.id = c.user_id")
    )
    # end-example
