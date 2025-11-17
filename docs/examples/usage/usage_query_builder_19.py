def test_example_19():
    # start-example
    # Create index
    query = (
        sql.create_index("idx_users_email")
        .on("users")
        .columns("email")
    )

    # Unique index
    query = (
        sql.create_index("idx_users_email")
        .on("users")
        .columns("email")
        .unique()
    )

    session.execute(query)
    # end-example
