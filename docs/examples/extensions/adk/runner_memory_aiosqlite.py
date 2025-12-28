"""Run an ADK agent with SQLSpec-backed session and memory services (AioSQLite)."""

import asyncio

from google.adk.agents.llm_agent import LlmAgent
from google.adk.apps.app import App
from google.adk.runners import Runner
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.adapters.aiosqlite.adk.memory_store import AiosqliteADKMemoryStore
from sqlspec.extensions.adk import SQLSpecSessionService
from sqlspec.extensions.adk.memory import SQLSpecMemoryService

__all__ = ("main",)


async def main() -> None:
    """Run a single ADK turn, then persist memory and search it."""
    config = AiosqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"adk": {"memory_use_fts": False}}
    )
    session_store = AiosqliteADKStore(config)
    memory_store = AiosqliteADKMemoryStore(config)
    await session_store.create_tables()
    await memory_store.create_tables()

    session_service = SQLSpecSessionService(session_store)
    memory_service = SQLSpecMemoryService(memory_store)

    agent = LlmAgent(name="sqlspec_agent", model="gemini-2.5-flash", instruction="Answer briefly.")
    app = App(name="sqlspec_demo", root_agent=agent)
    runner = Runner(app=app, session_service=session_service, memory_service=memory_service)

    session_id = "session-1"
    user_id = "demo-user"
    await session_service.create_session(app_name=app.name, user_id=user_id, session_id=session_id)

    new_message = types.UserContent(parts=[types.Part(text="Remember I like espresso.")])
    async for _event in runner.run_async(user_id=user_id, session_id=session_id, new_message=new_message):
        pass

    session = await session_service.get_session(app_name=app.name, user_id=user_id, session_id=session_id)
    if session:
        await memory_service.add_session_to_memory(session)

    response = await memory_service.search_memory(app_name=app.name, user_id=user_id, query="espresso")
    print({"memories": len(response.memories)})


if __name__ == "__main__":
    asyncio.run(main())
