def test_example_2():
    # start-example
    from sqlspec import sql

    # Simple select
    query = sql.select("*").from_("users")
    # SQL: SELECT * FROM users

    # Specific columns
    query = sql.select("id", "name", "email").from_("users")
    # SQL: SELECT id, name, email FROM users

    # With table alias
    query = sql.select("u.id", "u.name").from_("users u")
    # SQL: SELECT u.id, u.name FROM users u
    # end-example
