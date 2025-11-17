def test_example_24():
    # start-example
    from sqlspec import sql

    class UserQueries:
        @staticmethod
        def by_id():
            return sql.select("*").from_("users").where("id = ?")

        @staticmethod
        def by_email():
            return sql.select("*").from_("users").where("email = ?")

        @staticmethod
        def search(filters):
            query = sql.select("*").from_("users")
            params = []

            if "name" in filters:
                query = query.where("name LIKE ?")
                params.append(f"%{filters['name']}%")

            if "status" in filters:
                query = query.where("status = ?")
                params.append(filters["status"])

            return query, params

    # Usage
    user = session.execute(UserQueries.by_id(), 1).one()
    query, params = UserQueries.search({"name": "Alice", "status": "active"})
    result = session.execute(query, *params)
    users = result.all()
    # end-example
