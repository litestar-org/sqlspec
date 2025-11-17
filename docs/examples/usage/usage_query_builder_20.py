def test_example_20():
    # start-example
    query = sql.select(
        "id",
        "name",
        "salary",
        "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank"
    ).from_("employees")
    # end-example
