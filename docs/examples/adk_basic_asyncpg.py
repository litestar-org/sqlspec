"""Example: Google ADK session storage with AsyncPG.

This example demonstrates basic session and event management using
the Google ADK extension with PostgreSQL via AsyncPG.

Requirements:
    - PostgreSQL running locally (default port 5432)
    - pip install sqlspec[asyncpg,adk] google-genai

Usage:
    python docs/examples/adk_basic_asyncpg.py
"""

import asyncio
from datetime import datetime, timezone

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main", "run_adk_example")


async def run_adk_example() -> None:
    """Demonstrate Google ADK session storage with AsyncPG."""
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://postgres:postgres@localhost:5432/sqlspec_dev"})

    store = AsyncpgADKStore(config)
    await store.create_tables()
    print("✅ Created ADK tables in PostgreSQL")

    service = SQLSpecSessionService(store)

    print("\n=== Creating Session ===")
    session = await service.create_session(app_name="chatbot", user_id="user_123", state={"conversation_count": 0})
    print(f"Created session: {session.id}")
    print(f"App: {session.app_name}, User: {session.user_id}")
    print(f"Initial state: {session.state}")

    print("\n=== Adding User Message Event ===")
    user_event = Event(
        id="event_1",
        invocation_id="inv_1",
        author="user",
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(parts=[types.Part(text="What is the weather like today?")]),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, user_event)
    print(f"Added user event: {user_event.id}")
    print(f"User message: {user_event.content.parts[0].text if user_event.content else 'None'}")

    print("\n=== Adding Assistant Response Event ===")
    assistant_event = Event(
        id="event_2",
        invocation_id="inv_1",
        author="assistant",
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(parts=[types.Part(text="The weather is sunny with a high of 75°F.")]),
        partial=False,
        turn_complete=True,
    )
    await service.append_event(session, assistant_event)
    print(f"Added assistant event: {assistant_event.id}")
    print(f"Assistant response: {assistant_event.content.parts[0].text if assistant_event.content else 'None'}")

    print("\n=== Retrieving Session with Events ===")
    retrieved_session = await service.get_session(app_name="chatbot", user_id="user_123", session_id=session.id)

    if retrieved_session:
        print(f"Retrieved session: {retrieved_session.id}")
        print(f"Number of events: {len(retrieved_session.events)}")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  Event {idx} ({author}): {text}")
    else:
        print("❌ Session not found")

    print("\n=== Listing Sessions ===")
    sessions = await service.list_sessions(app_name="chatbot", user_id="user_123")
    print(f"Found {len(sessions.sessions)} session(s) for user_123")
    for s in sessions.sessions:
        print(f"  - {s.id} (updated: {datetime.fromtimestamp(s.last_update_time, tz=timezone.utc)})")

    print("\n=== Updating Session State ===")
    session.state["conversation_count"] = 1
    await store.update_session_state(session.id, session.state)
    print(f"Updated state: {session.state}")

    updated_session = await service.get_session(app_name="chatbot", user_id="user_123", session_id=session.id)
    if updated_session:
        print(f"Verified updated state: {updated_session.state}")

    print("\n=== Cleaning Up ===")
    await service.delete_session(app_name="chatbot", user_id="user_123", session_id=session.id)
    print(f"Deleted session: {session.id}")

    remaining_sessions = await service.list_sessions(app_name="chatbot", user_id="user_123")
    print(f"Remaining sessions: {len(remaining_sessions.sessions)}")


def main() -> None:
    """Run the ADK AsyncPG example."""
    print("=== Google ADK with AsyncPG Example ===")
    try:
        asyncio.run(run_adk_example())
        print("\n✅ Example completed successfully!")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        print("Make sure PostgreSQL is running with: make infra-up")
        print("Or manually: docker run -d --name postgres-dev -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres")


if __name__ == "__main__":
    main()
