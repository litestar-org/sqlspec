"""Example: Google ADK session storage with AIOSQLite.

This example demonstrates async session and event management using
the Google ADK extension with AIOSQLite (async SQLite wrapper).

AIOSQLite is perfect for:
- Async web applications (FastAPI, Litestar, Starlette)
- Async testing and development
- Embedded async applications
- Prototyping async AI agent applications

Requirements:
    - pip install sqlspec[aiosqlite] google-genai

Usage:
    python docs/examples/adk_basic_aiosqlite.py
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main", "run_adk_example")


async def initialize_database(config: "AiosqliteConfig") -> "AiosqliteADKStore":
    """Initialize database with optimal async SQLite settings.

    Args:
        config: AiosqliteConfig instance.

    Returns:
        Initialized AiosqliteADKStore.
    """
    async with config.provide_connection() as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA cache_size=-64000")
        await conn.commit()

    store = AiosqliteADKStore(config)
    await store.create_tables()
    return store


async def run_adk_example() -> None:
    """Demonstrate Google ADK session storage with AIOSQLite."""
    db_path = Path("./sqlspec_adk_aiosqlite.db")
    config = AiosqliteConfig(pool_config={"database": str(db_path)})

    store = await initialize_database(config)
    print(f"✅ Created ADK tables in async SQLite database: {db_path}")
    print("   (WAL mode enabled for better concurrency)")

    service = SQLSpecSessionService(store)

    print("\n=== Creating Session (Async) ===")
    session = await service.create_session(
        app_name="async_chatbot",
        user_id="async_user_1",
        state={"mode": "conversational", "language": "en"},
    )
    print(f"Created session: {session['id']}")
    print(f"App: {session['app_name']}, User: {session['user_id']}")
    print(f"Initial state: {session['state']}")

    print("\n=== Adding Conversation Events (Async) ===")
    user_event = Event(
        id="evt_async_user_1",
        invocation_id="inv_async_1",
        author="user",
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(parts=[types.Part(text="Tell me about async SQLite")]),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, user_event)
    print(f"Added user event: {user_event.id}")

    assistant_event = Event(
        id="evt_async_assistant_1",
        invocation_id="inv_async_1",
        author="assistant",
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(
            parts=[
                types.Part(
                    text="AIOSQLite wraps SQLite with async/await support via thread pool executor. "
                    "It's perfect for async web frameworks like FastAPI and Litestar, allowing you to "
                    "avoid blocking the event loop while still using SQLite's embedded database features!"
                )
            ]
        ),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, assistant_event)
    print(f"Added assistant event: {assistant_event.id}")

    print("\n=== Retrieving Session with History (Async) ===")
    retrieved_session = await service.get_session(
        app_name="async_chatbot", user_id="async_user_1", session_id=session["id"]
    )

    if retrieved_session:
        print(f"Retrieved session: {retrieved_session['id']}")
        print(f"Event count: {len(retrieved_session['events'])}")
        print("\nConversation history:")
        for idx, event in enumerate(retrieved_session["events"], 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  {idx}. [{author}]: {text[:80]}{'...' if len(text) > 80 else ''}")
    else:
        print("❌ Session not found")

    print("\n=== Multi-Session Management (Async) ===")
    session2 = await service.create_session(
        app_name="async_chatbot",
        user_id="async_user_1",
        state={"mode": "analytical", "language": "en"},
    )
    print(f"Created second session: {session2['id']}")

    sessions = await service.list_sessions(app_name="async_chatbot", user_id="async_user_1")
    print(f"Total sessions for user 'async_user_1': {len(sessions)}")

    print("\n=== Async Benefits ===")
    print("With AIOSQLite, all database operations use async/await:")
    print("  - await store.create_session(...)")
    print("  - await store.get_session(...)")
    print("  - await store.append_event(...)")
    print("  - await store.list_sessions(...)")
    print("\nThis prevents blocking the event loop in async web applications!")

    print("\n=== Performance Tips ===")
    print("For optimal async SQLite performance:")
    print("  1. Enable WAL mode: PRAGMA journal_mode=WAL")
    print("  2. Use connection pooling (configured in AiosqliteConfig)")
    print("  3. Batch operations when possible to reduce thread pool overhead")
    print("  4. Keep transactions short to avoid blocking other writers")

    print("\n=== Cleanup (Async) ===")
    await service.delete_session(session["id"])
    await service.delete_session(session2["id"])
    print(f"Deleted {2} sessions")

    await config.close_pool()
    print("Closed async connection pool")

    if db_path.exists():
        db_path.unlink()
        print(f"Cleaned up database: {db_path}")

    print("\n✅ Async example completed successfully!")


async def main() -> None:
    """Run the async ADK example."""
    try:
        await run_adk_example()
    except Exception as e:
        print(f"\n❌ Error: {e!s}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
