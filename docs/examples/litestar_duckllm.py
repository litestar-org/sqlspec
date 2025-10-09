"""Litestar DuckLLM

This example demonstrates how to use the Litestar framework with the DuckLLM extension.

The example uses the `SQLSpec` extension to create a connection to the DuckDB database.
The `DuckDB` adapter is used to create a connection to the database.

Usage:
    litestar --app docs.examples.litestar_duckllm:app run --reload
"""

# /// script
# dependencies = [
#   "sqlspec[duckdb,litestar]",
#   "rich",
#   "litestar[standard]",
# ]
# requires-python = ">=3.10"
# ///

from litestar import Litestar, post
from msgspec import Struct
from rich import print

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.extensions.litestar import SQLSpecPlugin


class ChatMessage(Struct):
    message: str


@post("/chat", sync_to_thread=True)
def duckllm_chat(db_session: DuckDBDriver, data: ChatMessage) -> ChatMessage:
    result = db_session.execute("SELECT open_prompt(?)", data.message)
    messages = result.get_data(schema_type=ChatMessage)
    return messages[0] if messages else ChatMessage(message="No response from DuckLLM")


spec = SQLSpec()
db = spec.add_config(
    DuckDBConfig(
        driver_features={
            "extensions": [{"name": "open_prompt"}],
            "secrets": [
                {
                    "secret_type": "open_prompt",
                    "name": "open_prompt",
                    "value": {
                        "api_url": "http://127.0.0.1:11434/v1/chat/completions",
                        "model_name": "gemma3:1b",
                        "api_timeout": "120",
                    },
                }
            ],
        }
    )
)
plugin = SQLSpecPlugin(sqlspec=spec)
app = Litestar(route_handlers=[duckllm_chat], plugins=[plugin], debug=True)

if __name__ == "__main__":
    print("[cyan]Run with:[/cyan] litestar --app docs.examples.litestar_duckllm:app run --reload")
    print("[yellow]Or directly:[/yellow] uv run python docs/examples/litestar_duckllm.py")
