def test_example_14():
    # start-example
    from sqlspec import sql

    # Dynamic update builder
    def update_user(user_id, **fields):
        query = sql.update("users")
        params = []

        for field, value in fields.items():
            query = query.set(field, "?")
            params.append(value)

        query = query.where("id = ?")
        params.append(user_id)

        return session.execute(query, *params)

    # Usage
    update_user(1, name="Alice", email="alice@example.com", status="active")
    # end-example
