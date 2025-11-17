def test_example_31():
    # start-example
    # PostgreSQL JSON operators (use raw SQL)
    session.execute("SELECT data->>'name' FROM events WHERE data @> ?", json_filter)
    # end-example
