def test_example_17():
    # start-example
    from sqlspec import sql

    # Create table
    query = (
        sql.create_table("users")
        .column("id", "INTEGER PRIMARY KEY")
        .column("name", "TEXT NOT NULL")
        .column("email", "TEXT UNIQUE NOT NULL")
        .column("created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    )

    session.execute(query)
    # end-example
