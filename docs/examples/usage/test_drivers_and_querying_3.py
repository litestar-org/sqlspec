# Test module converted from docs example - code-block 3
"""Minimal smoke test for drivers_and_querying example 3."""

from pytest_databases.docker.postgres import PostgresService


def test_example_3_importable(postgres_service: PostgresService) -> None:
    from sqlspec import SQLSpec
    from sqlspec.adapters.psycopg import PsycopgSyncConfig

    spec = SQLSpec()
    # Async version
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 5,
            "max_size": 10,
        }
    )

    with spec.provide_session(config) as session:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );
        """
        session.execute(create_table_query)
        # Insert with RETURNING
        session.execute("INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id", "Alice", "alice@example.com")
        session.execute("SELECT * FROM users")
