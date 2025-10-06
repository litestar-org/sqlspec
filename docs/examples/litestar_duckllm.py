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

from sqlspec import SQLSpec
from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.extensions.litestar import SQLSpecPlugin


class ChatMessage(Struct):
    message: str


@post("/chat", sync_to_thread=True)
def duckllm_chat(db_session: DuckDBDriver, data: ChatMessage) -> ChatMessage:
    results = db_session.execute("SELECT open_prompt(?)", data.message).get_first()
    return db_session.to_schema(results or {"message": "No response from DuckLLM"}, schema_type=ChatMessage)


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
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
