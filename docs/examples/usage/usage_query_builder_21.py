def test_example_21():
    # start-example
    from sqlspec import sql

    case_expr = (
        sql.case()
        .when("status = 'active'", "'Active User'")
        .when("status = 'pending'", "'Pending Approval'")
        .else_("'Inactive'")
    )

    query = sql.select("id", "name", f"{case_expr} as status_label").from_("users")
    # end-example
