# Test module converted from docs example - code-block 12
"""Minimal smoke test for drivers_and_querying example 12."""

from sqlspec.adapters.bigquery.driver import BigQueryDriver


def test_example_12_bigquery_config(bigquery_service: BigQueryDriver) -> None:
    # start-example
    import datetime

    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials

    from sqlspec import SQLSpec
    from sqlspec.adapters.bigquery.config import BigQueryConfig

    config = BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        }
    )
    spec = SQLSpec()
    with spec.provide_session(config) as bigquery_session:
        bigquery_session.execute("SELECT 1 AS value")

        # Create the test table

        create_table_query = """
        CREATE or replace TABLE events (
            timestamp TIMESTAMP,
            event_type STRING
        )
        """
        bigquery_session.execute_script(create_table_query)

        print("Executing test query...")
        bigquery_session.execute(
            """
           SELECT DATE(timestamp) as date,
                  COUNT(*) as events
           FROM events
           WHERE timestamp >= @start_date
           GROUP BY date
       """,
            start_date=datetime.date(2025, 1, 1),
        )
    # end-example
