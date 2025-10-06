"""Example: Google ADK session storage with DuckDB.

This example demonstrates basic session and event management using
the Google ADK extension with DuckDB (embedded OLAP database).

DuckDB is perfect for:
- Development and testing (zero-configuration)
- Analytical workloads on session data
- Embedded applications
- Session analytics and reporting

Requirements:
    - pip install sqlspec[adk] google-genai duckdb

Usage:
    python docs/examples/adk_basic_duckdb.py
"""

from datetime import datetime, timezone
from pathlib import Path

from google.adk.events.event import Event
from google.genai import types

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.adk import DuckdbADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main", "run_adk_example")


def run_adk_example() -> None:
    """Demonstrate Google ADK session storage with DuckDB."""
    db_path = Path("./sqlspec_adk_duckdb.db")
    config = DuckDBConfig(database=str(db_path))

    store = DuckdbADKStore(config)
    store.create_tables()
    print(f"✅ Created ADK tables in DuckDB database: {db_path}")

    service = SQLSpecSessionService(store)

    print("\n=== Creating Session ===")
    session = service.create_session(
        app_name="analytics_bot", user_id="data_analyst", state={"dashboard": "active", "filters": {"date_range": "7d"}}
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
        content=types.Content(parts=[types.Part(text="Show me session analytics for the last week")]),
        partial=False,
        turn_complete=True,
    )
    service.append_event(session, user_event)
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
                    text="DuckDB's columnar storage makes it perfect for analytical queries! "
                    "You can run fast aggregations on session data without impacting performance."
                )
            ]
        ),
        partial=False,
        turn_complete=True,
    )
    service.append_event(session, assistant_event)
    print(f"Added assistant event: {assistant_event.id}")

    print("\n=== Retrieving Session with History ===")
    retrieved_session = service.get_session(app_name="analytics_bot", user_id="data_analyst", session_id=session.id)

    if retrieved_session:
        print(f"Retrieved session: {retrieved_session.id}")
        print(f"Event count: {len(retrieved_session.events)}")
        print("\nConversation history:")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  {idx}. [{author}]: {text[:80]}{'...' if len(text) > 80 else ''}")  # noqa: PLR2004
    else:
        print("❌ Session not found")

    print("\n=== Multi-Session Management ===")
    session2 = service.create_session(
        app_name="analytics_bot",
        user_id="data_analyst",
        state={"dashboard": "reports", "filters": {"date_range": "30d"}},
    )
    print(f"Created second session: {session2.id}")

    sessions = service.list_sessions(app_name="analytics_bot", user_id="data_analyst")
    print(f"Total sessions for user 'data_analyst': {len(sessions)}")

    print("\n=== DuckDB Analytics Example ===")
    print("DuckDB is optimized for OLAP queries. Example analytical queries:")
    print()
    print("  -- Session activity by user")
    print("  SELECT user_id, COUNT(*) as session_count")
    print("  FROM adk_sessions")
    print("  WHERE app_name = 'analytics_bot'")
    print("  GROUP BY user_id")
    print("  ORDER BY session_count DESC;")
    print()
    print("  -- Event distribution by author")
    print("  SELECT author, COUNT(*) as event_count")
    print("  FROM adk_events")
    print("  WHERE app_name = 'analytics_bot'")
    print("  GROUP BY author;")

    print("\n=== Cleanup ===")
    service.delete_session(session.id)
    service.delete_session(session2.id)
    print(f"Deleted {2} sessions")

    if db_path.exists():
        db_path.unlink()
        print(f"Cleaned up database: {db_path}")

    print("\n✅ Example completed successfully!")


def main() -> None:
    """Run the ADK example."""
    try:
        run_adk_example()
    except Exception as e:
        print(f"\n❌ Error: {e!s}")
        raise


if __name__ == "__main__":
    main()
