"""Example: Google ADK session storage with AsyncPG.

This example demonstrates basic session and event management using
the Google ADK extension with PostgreSQL via AsyncPG.

Requirements:
    - PostgreSQL running locally (default port 5432)

Usage:
    uv run docs/examples/adk_basic_asyncpg.py
"""

# /// script
# dependencies = [
#   "sqlspec[asyncpg,adk]",
#   "rich",
#   "google-genai",
# ]
# requires-python = ">=3.10"
# ///

import asyncio
from datetime import datetime, timezone

from google.adk.events.event import Event
from google.genai import types
from rich import print

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("main", "run_adk_example")


async def run_adk_example() -> None:
    """Demonstrate Google ADK session storage with AsyncPG."""
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://postgres:postgres@localhost:5432/sqlspec_dev"})

    store = AsyncpgADKStore(config)
    await store.create_tables()
    print("[green]✅ Created ADK tables in PostgreSQL[/green]")

    service = SQLSpecSessionService(store)

    print("\n[bold cyan]=== Creating Session ===[/bold cyan]")
    session = await service.create_session(app_name="chatbot", user_id="user_123", state={"conversation_count": 0})
    print(f"[cyan]Created session:[/cyan] {session.id}")
    print(f"[cyan]App:[/cyan] {session.app_name}, [cyan]User:[/cyan] {session.user_id}")
    print(f"[cyan]Initial state:[/cyan] {session.state}")

    print("\n[bold cyan]=== Adding User Message Event ===[/bold cyan]")
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
    print(f"[cyan]Added user event:[/cyan] {user_event.id}")
    print(f"[cyan]User message:[/cyan] {user_event.content.parts[0].text if user_event.content else 'None'}")

    print("\n[bold cyan]=== Adding Assistant Response Event ===[/bold cyan]")
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
    print(f"[cyan]Added assistant event:[/cyan] {assistant_event.id}")
    print(
        f"[cyan]Assistant response:[/cyan] {assistant_event.content.parts[0].text if assistant_event.content else 'None'}"
    )

    print("\n[bold cyan]=== Retrieving Session with Events ===[/bold cyan]")
    retrieved_session = await service.get_session(app_name="chatbot", user_id="user_123", session_id=session.id)

    if retrieved_session:
        print(f"[cyan]Retrieved session:[/cyan] {retrieved_session.id}")
        print(f"[cyan]Number of events:[/cyan] {len(retrieved_session.events)}")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  [cyan]Event {idx}[/cyan] ([yellow]{author}[/yellow]): {text}")
    else:
        print("[red]❌ Session not found[/red]")

    print("\n[bold cyan]=== Listing Sessions ===[/bold cyan]")
    sessions = await service.list_sessions(app_name="chatbot", user_id="user_123")
    print(f"[cyan]Found {len(sessions.sessions)} session(s) for user_123[/cyan]")
    for s in sessions.sessions:
        print(f"  - {s.id} [dim](updated: {datetime.fromtimestamp(s.last_update_time, tz=timezone.utc)})[/dim]")

    print("\n[bold cyan]=== Updating Session State ===[/bold cyan]")
    session.state["conversation_count"] = 1
    await store.update_session_state(session.id, session.state)
    print(f"[cyan]Updated state:[/cyan] {session.state}")

    updated_session = await service.get_session(app_name="chatbot", user_id="user_123", session_id=session.id)
    if updated_session:
        print(f"[cyan]Verified updated state:[/cyan] {updated_session.state}")

    print("\n[bold cyan]=== Cleaning Up ===[/bold cyan]")
    await service.delete_session(app_name="chatbot", user_id="user_123", session_id=session.id)
    print(f"[cyan]Deleted session:[/cyan] {session.id}")

    remaining_sessions = await service.list_sessions(app_name="chatbot", user_id="user_123")
    print(f"[cyan]Remaining sessions:[/cyan] {len(remaining_sessions.sessions)}")


def main() -> None:
    """Run the ADK AsyncPG example."""
    print("[bold magenta]=== Google ADK with AsyncPG Example ===[/bold magenta]")
    try:
        asyncio.run(run_adk_example())
        print("\n[green]✅ Example completed successfully![/green]")
    except Exception as e:
        print(f"\n[red]❌ Example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with:[/yellow] [cyan]make infra-up[/cyan]")
        print(
            "[yellow]Or manually:[/yellow] [cyan]docker run -d --name postgres-dev -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres[/cyan]"
        )


if __name__ == "__main__":
    main()
