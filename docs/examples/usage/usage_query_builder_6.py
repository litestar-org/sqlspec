def test_example_6():
    # start-example
    # ORDER BY
    query = sql.select("*").from_("users").order_by("created_at DESC")

    # Multiple order columns
    query = (
        sql.select("*")
        .from_("users")
        .order_by("status ASC", "created_at DESC")
    )

    # LIMIT and OFFSET
    query = sql.select("*").from_("users").limit(10).offset(20)

    # Pagination helper
    def paginate(page=1, per_page=20):
        offset = (page - 1) * per_page
        return (
            sql.select("*")
            .from_("users")
            .order_by("id")
            .limit(per_page)
            .offset(offset)
        )
    # end-example
