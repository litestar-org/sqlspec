"""Litestar DuckLLM

This example demonstrates how to use the Litestar framework with the DuckLLM extension.

The example uses the `SQLSpec` extension to create a connection to the DuckDB database.
The `DuckDB` adapter is used to create a connection to the database.
"""

# /// script
# dependencies = [
#   "sqlspec[duckdb,performance]",
#   "litestar[standard]",
# ]
# ///

from litestar import Litestar, post
from msgspec import Struct

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.extensions.litestar import SQLSpec


class ChatMessage(Struct):
    message: str


@post("/chat", sync_to_thread=True)
def duckllm_chat(db_session: DuckDBDriver, data: ChatMessage) -> ChatMessage:
    results = db_session.execute("SELECT open_prompt(?)", data.message, schema_type=ChatMessage).get_first()
    return results or ChatMessage(message="No response from DuckLLM")


sqlspec = SQLSpec(
    config=DuckDBConfig(
        extensions=[{"name": "open_prompt"}],
        secrets=[
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
    )
)
app = Litestar(route_handlers=[duckllm_chat], plugins=[sqlspec], debug=True)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
