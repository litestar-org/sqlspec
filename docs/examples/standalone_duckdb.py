# /// script
# dependencies = [
#   "sqlspec[duckdb,performance]",
#   "rich",
# ]
# requires-python = ">=3.10"
# ///

"""Generating embeddings with Gemini

This example demonstrates how to generate embeddings with Gemini using only DuckDB and the HTTP client extension.
"""

import os

from rich import print

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig

EMBEDDING_MODEL = "gemini-embedding-exp-03-07"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    print("[red]Error: GOOGLE_API_KEY environment variable not set[/red]")
    print("[yellow]Please set GOOGLE_API_KEY to use this example[/yellow]")
    exit(1)

API_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{EMBEDDING_MODEL}:embedContent?key=${GOOGLE_API_KEY}"
)

spec = SQLSpec()
db = spec.add_config(
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

print("[cyan]Generating embedding with Gemini...[/cyan]")

with spec.provide_session(db) as session:
    result = session.execute("SELECT generate_embedding('example text')")
    print("[green]✅ Embedding generated successfully[/green]")
    print(f"[yellow]Result:[/yellow] {result}")
