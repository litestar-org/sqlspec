def test_example_4():
    # start-example
    from sqlspec import sql

    def search_users(name=None, email=None, status=None):
        query = sql.select("id", "name", "email", "status").from_("users")
        params = []

        if name:
            query = query.where("name LIKE ?")
            params.append(f"%{name}%")

        if email:
            query = query.where("email = ?")
            params.append(email)

        if status:
            query = query.where("status = ?")
            params.append(status)

        return session.execute(query, *params)

    # Usage
    users = search_users(name="Alice", status="active")
    # end-example
