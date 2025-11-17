def test_example_7():
    # start-example
    # COUNT
    query = sql.select("COUNT(*) as total").from_("users")

    # GROUP BY
    query = (
        sql.select("status", "COUNT(*) as count")
        .from_("users")
        .group_by("status")
    )

    # HAVING
    query = (
        sql.select("user_id", "COUNT(*) as order_count")
        .from_("orders")
        .group_by("user_id")
        .having("COUNT(*) > ?")
    )

    # Multiple aggregations
    query = (
        sql.select(
            "DATE(created_at) as date",
            "COUNT(*) as orders",
            "SUM(total) as revenue"
        )
        .from_("orders")
        .group_by("DATE(created_at)")
    )
    # end-example
