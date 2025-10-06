"""Example: Google ADK session storage with MySQL.

This example demonstrates basic session and event management using
the Google ADK extension with MySQL/MariaDB via AsyncMy driver.

Requirements:
    - MySQL or MariaDB running locally (default port 3306)
    - pip install sqlspec[asyncmy,adk] google-genai

Usage:
    python docs/examples/adk_basic_mysql.py
"""

import asyncio
from datetime import datetime, timezone

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.adapters.asyncmy.adk import AsyncmyADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main", "run_adk_example")


async def run_adk_example() -> None:
    """Demonstrate Google ADK session storage with MySQL."""
    config = AsyncmyConfig(
        pool_config={"host": "localhost", "port": 3306, "user": "root", "password": "root", "database": "sqlspec_dev"}
    )

    store = AsyncmyADKStore(config)
    await store.create_tables()
    print("✅ Created ADK tables in MySQL database")

    service = SQLSpecSessionService(store)

    print("\n=== Creating Session ===")
    session = await service.create_session(
        app_name="assistant", user_id="bob", state={"preferences": {"notifications": True, "theme": "auto"}}
    )
    print(f"Created session: {session.id}")
    print(f"App: {session.app_name}, User: {session.user_id}")
    print(f"Initial state: {session.state}")

    print("\n=== Simulating Multi-Turn Conversation ===")
    conversation = [
        ("user", "What databases does SQLSpec support?"),
        (
            "assistant",
            "SQLSpec supports PostgreSQL, MySQL, SQLite, DuckDB, Oracle, BigQuery, and more! "
            "Each has an optimized adapter.",
        ),
        ("user", "Which one is best for production?"),
        ("assistant", "PostgreSQL or MySQL are excellent for production. AsyncPG offers great performance."),
    ]

    for turn_idx, (author, message) in enumerate(conversation, 1):
        event = Event(
            id=f"evt_{author}_{turn_idx}",
            invocation_id=f"inv_{turn_idx}",
            author=author,
            branch="main",
            actions=[],
            timestamp=datetime.now(timezone.utc).timestamp(),
            content=types.Content(parts=[types.Part(text=message)]),
            partial=False,
            turn_complete=True,
        )
        await service.append_event(session, event)
        print(f"  Turn {turn_idx} [{author}]: {message[:60]}{'...' if len(message) > 60 else ''}")

    print("\n=== Retrieving Full Conversation ===")
    retrieved_session = await service.get_session(app_name="assistant", user_id="bob", session_id=session.id)

    if retrieved_session:
        print(f"Session: {retrieved_session.id}")
        print(f"Total events: {len(retrieved_session.events)}")
        print("\nFull conversation history:")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  {idx}. [{author}]: {text}")
    else:
        print("❌ Session not found")

    print("\n=== Partial Event Retrieval (Recent Events) ===")
    from google.adk.sessions.base_session_service import GetSessionConfig

    config_recent = GetSessionConfig(num_recent_events=2)
    recent_session = await service.get_session(
        app_name="assistant", user_id="bob", session_id=session.id, config=config_recent
    )

    if recent_session:
        print(f"Retrieved {len(recent_session.events)} most recent events:")
        for event in recent_session.events:
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  [{author}]: {text[:50]}{'...' if len(text) > 50 else ''}")

    print("\n=== State Management ===")
    session.state["message_count"] = len(conversation)
    session.state["last_interaction"] = datetime.now(timezone.utc).isoformat()
    await store.update_session_state(session.id, session.state)
    print(f"Updated state: {session.state}")

    verified = await service.get_session(app_name="assistant", user_id="bob", session_id=session.id)
    if verified:
        print(f"Verified state from database: {verified.state}")

    print("\n=== Session Listing ===")
    session2 = await service.create_session(app_name="assistant", user_id="bob", state={"archived": True})
    print(f"Created second session: {session2.id}")

    all_sessions = await service.list_sessions(app_name="assistant", user_id="bob")
    print(f"\nUser 'bob' has {len(all_sessions.sessions)} session(s):")
    for s in all_sessions.sessions:
        print(f"  - {s.id} (updated: {datetime.fromtimestamp(s.last_update_time, tz=timezone.utc)})")

    print("\n=== Cleanup ===")
    await service.delete_session(app_name="assistant", user_id="bob", session_id=session.id)
    await service.delete_session(app_name="assistant", user_id="bob", session_id=session2.id)
    print("Deleted all sessions")

    final_count = await service.list_sessions(app_name="assistant", user_id="bob")
    print(f"Remaining sessions: {len(final_count.sessions)}")


def main() -> None:
    """Run the ADK MySQL example."""
    print("=== Google ADK with MySQL Example ===")
    try:
        asyncio.run(run_adk_example())
        print("\n✅ Example completed successfully!")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        print("\nMake sure MySQL is running with:")
        print(
            "  docker run -d --name mysql-dev -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sqlspec_dev -p 3306:3306 mysql:8"
        )
        print("\nOr use make infra-up if configured in Makefile")


if __name__ == "__main__":
    main()
