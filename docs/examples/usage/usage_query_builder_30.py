def test_example_30():
    # start-example
    # This is easier to read as raw SQL:
    result = session.execute("""
        WITH ranked_users AS (
            SELECT id, name,
                   ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at DESC) as rn
            FROM users
        )
        SELECT * FROM ranked_users WHERE rn <= 5
    """)
    # end-example
