def test_example_5():
    # start-example
    # INNER JOIN
    query = (
        sql.select("u.id", "u.name", "o.total")
        .from_("users u")
        .join("orders o", "u.id = o.user_id")
    )
    # SQL: SELECT u.id, u.name, o.total FROM users u
    #      INNER JOIN orders o ON u.id = o.user_id

    # LEFT JOIN
    query = (
        sql.select("u.id", "u.name", "COUNT(o.id) as order_count")
        .from_("users u")
        .left_join("orders o", "u.id = o.user_id")
        .group_by("u.id", "u.name")
    )

    # Multiple JOINs
    query = (
        sql.select("u.name", "o.id", "p.name as product")
        .from_("users u")
        .join("orders o", "u.id = o.user_id")
        .join("order_items oi", "o.id = oi.order_id")
        .join("products p", "oi.product_id = p.id")
    )
    # end-example
