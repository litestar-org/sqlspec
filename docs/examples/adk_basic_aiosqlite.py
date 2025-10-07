"""Example: Google ADK session storage with AIOSQLite.

This example demonstrates async session and event management using
the Google ADK extension with AIOSQLite (async SQLite wrapper).

AIOSQLite is perfect for:
- Async web applications (FastAPI, Litestar, Starlette)
- Async testing and development
- Embedded async applications
- Prototyping async AI agent applications

Usage:
    uv run docs/examples/adk_basic_aiosqlite.py
"""

# /// script
# dependencies = [
#   "sqlspec[aiosqlite,adk]",
#   "rich",
#   "google-genai",
# ]
# requires-python = ">=3.10"
# ///

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from google.adk.events.event import Event
from google.genai import types
from rich import print

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
    print(f"[green]✅ Created ADK tables in async SQLite database:[/green] {db_path}")
    print("   [dim](WAL mode enabled for better concurrency)[/dim]")

    service = SQLSpecSessionService(store)

    print("\n[bold cyan]=== Creating Session (Async) ===[/bold cyan]")
    session = await service.create_session(
        app_name="async_chatbot", user_id="async_user_1", state={"mode": "conversational", "language": "en"}
    )
    print(f"[cyan]Created session:[/cyan] {session.id}")
    print(f"[cyan]App:[/cyan] {session.app_name}, [cyan]User:[/cyan] {session.user_id}")
    print(f"[cyan]Initial state:[/cyan] {session.state}")

    print("\n[bold cyan]=== Adding Conversation Events (Async) ===[/bold cyan]")
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
    print(f"[cyan]Added user event:[/cyan] {user_event.id}")

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
    print(f"[cyan]Added assistant event:[/cyan] {assistant_event.id}")

    print("\n[bold cyan]=== Retrieving Session with History (Async) ===[/bold cyan]")
    retrieved_session = await service.get_session(
        app_name="async_chatbot", user_id="async_user_1", session_id=session.id
    )

    if retrieved_session:
        print(f"[cyan]Retrieved session:[/cyan] {retrieved_session.id}")
        print(f"[cyan]Event count:[/cyan] {len(retrieved_session.events)}")
        print("\n[cyan]Conversation history:[/cyan]")
        for idx, event in enumerate(retrieved_session.events, 1):
            author = event.author or "unknown"
            text = event.content.parts[0].text if event.content and event.content.parts else "No content"
            print(f"  {idx}. [[yellow]{author}[/yellow]]: {text[:80]}{'...' if len(text) > 80 else ''}")  # noqa: PLR2004
    else:
        print("[red]❌ Session not found[/red]")

    print("\n[bold cyan]=== Multi-Session Management (Async) ===[/bold cyan]")
    session2 = await service.create_session(
        app_name="async_chatbot", user_id="async_user_1", state={"mode": "analytical", "language": "en"}
    )
    print(f"[cyan]Created second session:[/cyan] {session2.id}")

    sessions = await service.list_sessions(app_name="async_chatbot", user_id="async_user_1")
    print(f"[cyan]Total sessions for user 'async_user_1':[/cyan] {len(sessions.sessions)}")

    print("\n[bold cyan]=== Async Benefits ===[/bold cyan]")
    print("[green]With AIOSQLite, all database operations use async/await:[/green]")
    print("  - [cyan]await store.create_session(...)[/cyan]")
    print("  - [cyan]await store.get_session(...)[/cyan]")
    print("  - [cyan]await store.append_event(...)[/cyan]")
    print("  - [cyan]await store.list_sessions(...)[/cyan]")
    print("\n[green]This prevents blocking the event loop in async web applications![/green]")

    print("\n[bold cyan]=== Performance Tips ===[/bold cyan]")
    print("[yellow]For optimal async SQLite performance:[/yellow]")
    print("  1. Enable WAL mode: [cyan]PRAGMA journal_mode=WAL[/cyan]")
    print("  2. Use connection pooling (configured in AiosqliteConfig)")
    print("  3. Batch operations when possible to reduce thread pool overhead")
    print("  4. Keep transactions short to avoid blocking other writers")

    print("\n[bold cyan]=== Cleanup (Async) ===[/bold cyan]")
    await service.delete_session(app_name="async_chatbot", user_id="async_user_1", session_id=session.id)
    await service.delete_session(app_name="async_chatbot", user_id="async_user_1", session_id=session2.id)
    print("[cyan]Deleted 2 sessions[/cyan]")

    await config.close_pool()
    print("[cyan]Closed async connection pool[/cyan]")

    if db_path.exists():
        db_path.unlink()
        print(f"[cyan]Cleaned up database:[/cyan] {db_path}")

    print("\n[green]✅ Async example completed successfully![/green]")


async def main() -> None:
    """Run the async ADK example."""
    try:
        await run_adk_example()
    except Exception as e:
        print(f"\n[red]❌ Error: {e!s}[/red]")
        raise


if __name__ == "__main__":
    asyncio.run(main())
