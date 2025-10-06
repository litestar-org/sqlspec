"""Example: Google ADK session storage with SQLite.

This example demonstrates basic session and event management using
the Google ADK extension with SQLite (embedded database).

SQLite is perfect for:
- Development and testing (zero-configuration)
- Embedded desktop applications
- Single-user AI agents
- Prototyping and demos

Requirements:
    - pip install sqlspec google-genai

Usage:
    python docs/examples/adk_basic_sqlite.py
"""

from datetime import datetime, timezone
from pathlib import Path

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main", "run_adk_example")


async def run_adk_example() -> None:
    """Demonstrate Google ADK session storage with SQLite."""
    db_path = Path("./sqlspec_adk_sqlite.db")
    config = SqliteConfig(pool_config={"database": str(db_path)})

    store = SqliteADKStore(config)
    await store.create_tables()
    print(f"✅ Created ADK tables in SQLite database: {db_path}")

    # Enable WAL mode for better concurrency
    with config.provide_connection() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.commit()
    print("✅ Enabled WAL mode and foreign keys")

    service = SQLSpecSessionService(store)

    print("\n=== Creating Session ===")
    session = await service.create_session(
        app_name="chatbot", user_id="user_123", state={"conversation_started": True, "context": "greeting"}
    )
    print(f"Created session: {session.id}")
    print(f"App: {session.app_name}, User: {session.user_id}")
    print(f"Initial state: {session.state}")

    print("\n=== Adding Conversation Events ===")
    user_event = Event(
        id="evt_user_1",
        invocation_id="inv_1",
        author="user",
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(parts=[types.Part(text="Hello! Can you help me with Python?")]),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, user_event)
    print(f"Added user event: {user_event.id}")

    assistant_event = Event(
        id="evt_assistant_1",
        invocation_id="inv_1",
        author="assistant",
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(
            parts=[
                types.Part(
                    text="Of course! SQLite is perfect for embedded applications. "
                    "It's lightweight, requires zero configuration, and works great for "
                    "development and single-user scenarios!"
                )
            ]
        ),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, assistant_event)
    print(f"Added assistant event: {assistant_event.id}")

    print("\n=== Retrieving Session with History ===")
    retrieved_session = await service.get_session(app_name="chatbot", user_id="user_123", session_id=session.id)

    if retrieved_session:
        print(f"Retrieved session: {retrieved_session.id}")
        print(f"Event count: {len(retrieved_session.events)}")
        print("\nConversation history:")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            max_text_length = 80
            print(f"  {idx}. [{author}]: {text[:max_text_length]}{'...' if len(text or '') > max_text_length else ''}")
    else:
        print("❌ Session not found")

    print("\n=== Multi-Session Management ===")
    session2 = await service.create_session(
        app_name="chatbot", user_id="user_123", state={"conversation_started": True, "context": "technical_help"}
    )
    print(f"Created second session: {session2.id}")

    sessions = await service.list_sessions(app_name="chatbot", user_id="user_123")
    print(f"Total sessions for user 'user_123': {len(sessions)}")

    print("\n=== SQLite Benefits ===")
    print("SQLite is ideal for:")
    print("  ✅ Zero-configuration development")
    print("  ✅ Embedded desktop applications")
    print("  ✅ Single-user AI agents")
    print("  ✅ Prototyping and testing")
    print("  ✅ Offline-first applications")
    print()
    print("Consider PostgreSQL for:")
    print("  ⚠️  High-concurrency production deployments")
    print("  ⚠️  Multi-user web applications")
    print("  ⚠️  Server-based architectures")

    print("\n=== Cleanup ===")
    await service.delete_session(session.id)
    await service.delete_session(session2.id)
    print(f"Deleted {2} sessions")

    if db_path.exists():
        db_path.unlink()
        print(f"Cleaned up database: {db_path}")

    print("\n✅ Example completed successfully!")


async def main() -> None:
    """Run the ADK example."""
    try:
        await run_adk_example()
    except Exception as e:
        print(f"\n❌ Error: {e!s}")
        raise


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
