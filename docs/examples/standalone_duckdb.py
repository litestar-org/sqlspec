"""Generating embeddings with Gemini

This example demonstrates how to generate embeddings with Gemini using only DuckDB and the HTTP client extension.
"""

# /// script
# dependencies = [
#   "sqlspec[duckdb,performance]",
# ]
# ///

import os

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

EMBEDDING_MODEL = "gemini-embedding-exp-03-07"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{EMBEDDING_MODEL}:embedContent?key=${GOOGLE_API_KEY}"
)

sqlspec = SQLSpec()
etl_config = sqlspec.add_config(
    DuckDBConfig(
        driver_features={
            "extensions": [{"name": "vss"}, {"name": "http_client"}],
            "on_connection_create": lambda connection: connection.execute(f"""
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
        }
    )
)

with sqlspec.provide_session(etl_config) as session:
    result = session.execute("SELECT generate_embedding('example text')")
    print(result)
