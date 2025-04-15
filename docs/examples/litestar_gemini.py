"""Litestar DuckLLM

This example demonstrates how to use the Litestar framework with the DuckLLM extension.

The example uses the `SQLSpec` extension to create a connection to the DuckDB database.
The `DuckDB` adapter is used to create a connection to the database.
"""

# /// script
# dependencies = [
#   "sqlspec[duckdb,performance] @ git+https://github.com/litestar-org/sqlspec.git@query-service",
#   "litestar[standard]",
# ]
# ///

import os

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDB

EMBEDDING_MODEL = "gemini-embedding-exp-03-07"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{EMBEDDING_MODEL}:embedContent?key=${GOOGLE_API_KEY}"
)

sql = SQLSpec()
etl_config = sql.add_config(
    DuckDB(
        extensions=[{"name": "vss"}, {"name": "http_client"}],
        on_connection_create=lambda connection: connection.execute(f"""
            CREATE IF NOT EXISTS MACRO generate_embedding(q) AS (
                WITH  __request AS (
                    SELECT http_post(
                        '{API_URL}',
                        headers => MAP {{
                            'accept': 'application/json',
                        }},
                        params => MAP {{
                            'model': 'models/{EMBEDDING_MODEL}',
                            'parts': [{{ 'text': q }}],
                            'taskType': 'SEMANTIC_SIMILARITY'
                        }}
                    ) AS response
                )
                SELECT *
                FROM __request,
            );
        """),
    )
)


if __name__ == "__main__":
    with sql.get_connection(etl_config) as connection:
        result = connection.execute("SELECT generate_embedding('example text')")
        print(result.fetchall())
