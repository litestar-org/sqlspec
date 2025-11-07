# Test module converted from docs example - code-block 12
"""Minimal smoke test for drivers_and_querying example 12."""


from sqlspec.adapters.bigquery import BigQueryDriver


def test_example_12_bigquery_config(bigquery_session: BigQueryDriver) -> None:


    from sqlspec import SQLSpec
    spec = SQLSpec()

    result = bigquery_session.execute("SELECT 1 AS value")

    # Create the test table

    create_table_query = """
    CREATE TABLE events (
        timestamp TIMESTAMP,
        event_type STRING
    )
    """
    bigquery_session.execute_script(create_table_query)

    result = bigquery_session.execute("""
       SELECT DATE(timestamp) as date,
              COUNT(*) as events
       FROM events
       WHERE timestamp >= @start_date
       GROUP BY date
   """, start_date=datetime.date(2025, 1, 1))
