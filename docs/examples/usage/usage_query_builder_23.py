def test_example_23():
    # start-example
    from sqlspec import sql

    # Base query
    base_query = sql.select("id", "name", "email", "status").from_("users")

    # Add filters based on context
    def active_users():
        return base_query.where("status = 'active'")

    def recent_users(days=7):
        return base_query.where("created_at >= ?")

    # Use in different contexts
    active = session.execute(active_users())
    recent = session.execute(recent_users(), datetime.date.today() - datetime.timedelta(days=7))
    # end-example
