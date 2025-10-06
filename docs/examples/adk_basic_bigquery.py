"""Basic BigQuery ADK store example.

This example demonstrates using BigQuery as a serverless, scalable backend
for Google ADK session and event storage.
"""

import asyncio

from google.adk.events.event import Event
from google.genai.types import Content, Part

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.adapters.bigquery.adk import BigQueryADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main",)


async def main() -> None:
    """Main function demonstrating BigQuery ADK integration."""
    config = BigQueryConfig(connection_config={"project": "my-gcp-project", "dataset_id": "my_dataset"})

    store = BigQueryADKStore(config)

    await store.create_tables()

    service = SQLSpecSessionService(store)

    session = await service.create_session(
        app_name="my_agent_app", user_id="user_123", state={"conversation_context": "initial"}
    )

    print(f"Created session: {session.id}")

    event = Event(
        session_id=session.id,
        app_name=session.app_name,
        user_id=session.user_id,
        author="user",
        content=Content(parts=[Part(text="Hello, AI assistant!")]),
    )

    await service.append_event(session.id, event)

    print(f"Appended event: {event.id}")

    events = await service.get_events(session.id)
    print(f"Retrieved {len(events)} events")

    sessions = await service.list_sessions(app_name="my_agent_app", user_id="user_123")
    print(f"Found {len(sessions)} sessions for user")

    await service.delete_session(session.id)
    print("Session deleted successfully")


if __name__ == "__main__":
    asyncio.run(main())
