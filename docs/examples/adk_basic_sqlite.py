"""Example: Google ADK session storage with SQLite.

This example demonstrates basic session and event management using
the Google ADK extension with SQLite (synchronous driver with async wrapper).

Requirements:
    - pip install sqlspec[adk] google-genai

Usage:
    python docs/examples/adk_basic_sqlite.py
"""

import asyncio
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
    db_path = Path("./sqlspec_adk_example.db")
    config = SqliteConfig(database=str(db_path))

    store = SqliteADKStore(config)
    await store.create_tables()
    print(f"✅ Created ADK tables in SQLite database: {db_path}")

    service = SQLSpecSessionService(store)

    print("\n=== Creating Session ===")
    session = await service.create_session(
        app_name="chatbot", user_id="alice", state={"theme": "dark", "language": "en"}
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
        content=types.Content(parts=[types.Part(text="How do I use SQLSpec with ADK?")]),
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
                    text="SQLSpec provides ADK stores for multiple databases. "
                    "Just create a store instance, create tables, and pass it to SQLSpecSessionService!"
                )
            ]
        ),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, assistant_event)
    print(f"Added assistant event: {assistant_event.id}")

    print("\n=== Retrieving Session with History ===")
    retrieved_session = await service.get_session(app_name="chatbot", user_id="alice", session_id=session.id)

    if retrieved_session:
        print(f"Retrieved session: {retrieved_session.id}")
        print(f"Event count: {len(retrieved_session.events)}")
        print("\nConversation history:")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  {idx}. [{author}]: {text[:80]}{'...' if len(text) > 80 else ''}")
    else:
        print("❌ Session not found")

    print("\n=== Multi-Session Management ===")
    session2 = await service.create_session(
        app_name="chatbot", user_id="alice", state={"theme": "light", "language": "es"}
    )
    print(f"Created second session: {session2.id}")

    all_sessions = await service.list_sessions(app_name="chatbot", user_id="alice")
    print(f"\nAlice has {len(all_sessions.sessions)} active session(s):")
    for s in all_sessions.sessions:
        state_preview = str(s.state)[:50]
        print(f"  - {s.id[:8]}... (state: {state_preview})")

    print("\n=== State Updates ===")
    session.state["message_count"] = 2
    session.state["last_topic"] = "ADK Integration"
    await store.update_session_state(session.id, session.state)
    print(f"Updated session state: {session.state}")

    print("\n=== Cleanup ===")
    await service.delete_session(app_name="chatbot", user_id="alice", session_id=session.id)
    await service.delete_session(app_name="chatbot", user_id="alice", session_id=session2.id)
    print("Deleted all sessions")

    remaining = await service.list_sessions(app_name="chatbot", user_id="alice")
    print(f"Remaining sessions: {len(remaining.sessions)}")

    print(f"\nNote: Database file retained at: {db_path}")
    print("Delete manually if desired, or use it for inspection with: sqlite3 sqlspec_adk_example.db")


def main() -> None:
    """Run the ADK SQLite example."""
    print("=== Google ADK with SQLite Example ===")
    try:
        asyncio.run(run_adk_example())
        print("\n✅ Example completed successfully!")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
