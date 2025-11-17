def test_example_32():
    # start-example
    from sqlspec import sql

    # Before: Raw SQL
    result = session.execute("""
        SELECT u.id, u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.status = ?
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > ?
        ORDER BY order_count DESC
        LIMIT ?
    """, "active", 5, 10)

    # After: Query Builder
    query = (
        sql.select("u.id", "u.name", "COUNT(o.id) as order_count")
        .from_("users u")
        .left_join("orders o", "u.id = o.user_id")
        .where("u.status = ?")
        .group_by("u.id", "u.name")
        .having("COUNT(o.id) > ?")
        .order_by("order_count DESC")
        .limit("?")
    )
    result = session.execute(query, "active", 5, 10)
    # end-example
