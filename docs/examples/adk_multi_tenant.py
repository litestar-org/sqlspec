"""Example: Multi-tenant ADK session management.

This example demonstrates managing sessions for multiple applications
and users in a single database, showing proper isolation via app_name
and user_id.

Requirements:
    - PostgreSQL running locally (default port 5432)

Usage:
    uv run docs/examples/adk_multi_tenant.py
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

__all__ = ("main", "run_multi_tenant_example")


async def create_sample_session(
    service: SQLSpecSessionService, app_name: str, user_id: str, messages: "list[tuple[str, str]]"
) -> str:
    """Create a session with sample conversation.

    Args:
        service: ADK session service.
        app_name: Application name.
        user_id: User identifier.
        messages: List of (author, text) tuples.

    Returns:
        Created session ID.
    """
    session = await service.create_session(app_name=app_name, user_id=user_id, state={"created_by": "demo"})

    for idx, (author, text) in enumerate(messages, 1):
        event = Event(
            id=f"evt_{session.id[:8]}_{idx}",
            invocation_id=f"inv_{idx}",
            author=author,
            branch="main",
            actions=[],
            timestamp=datetime.now(timezone.utc).timestamp(),
            content=types.Content(parts=[types.Part(text=text)]),
            partial=False,
            turn_complete=True,
        )
        await service.append_event(session, event)

    return session.id


async def run_multi_tenant_example() -> None:
    """Demonstrate multi-tenant session management."""
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://postgres:postgres@localhost:5432/sqlspec_dev"})

    store = AsyncpgADKStore(config)
    await store.create_tables()
    print("[green]✅ ADK tables ready for multi-tenant demo[/green]")

    service = SQLSpecSessionService(store)

    print("\n[bold cyan]=== Scenario: Multiple Apps and Users ===[/bold cyan]")
    print("[cyan]Creating sessions for different apps and users...[/cyan]")

    chatbot_alice_1 = await create_sample_session(
        service,
        app_name="chatbot",
        user_id="alice",
        messages=[("user", "Hello!"), ("assistant", "Hi Alice! How can I help?")],
    )
    print(f"  [cyan]Created:[/cyan] chatbot/alice/{chatbot_alice_1[:8]}...")

    chatbot_alice_2 = await create_sample_session(
        service,
        app_name="chatbot",
        user_id="alice",
        messages=[("user", "What's the weather?"), ("assistant", "It's sunny today!")],
    )
    print(f"  [cyan]Created:[/cyan] chatbot/alice/{chatbot_alice_2[:8]}...")

    chatbot_bob = await create_sample_session(
        service, app_name="chatbot", user_id="bob", messages=[("user", "Help me!"), ("assistant", "Sure, Bob!")]
    )
    print(f"  [cyan]Created:[/cyan] chatbot/bob/{chatbot_bob[:8]}...")

    assistant_alice = await create_sample_session(
        service,
        app_name="assistant",
        user_id="alice",
        messages=[("user", "Summarize this document"), ("assistant", "Here's a summary...")],
    )
    print(f"  [cyan]Created:[/cyan] assistant/alice/{assistant_alice[:8]}...")

    assistant_carol = await create_sample_session(
        service,
        app_name="assistant",
        user_id="carol",
        messages=[("user", "Schedule a meeting"), ("assistant", "Meeting scheduled!")],
    )
    print(f"  [cyan]Created:[/cyan] assistant/carol/{assistant_carol[:8]}...")

    print("\n[bold cyan]=== Tenant Isolation Demo ===[/bold cyan]")

    print("\n[cyan]1. Alice's chatbot sessions:[/cyan]")
    alice_chatbot = await service.list_sessions(app_name="chatbot", user_id="alice")
    print(f"   [cyan]Found {len(alice_chatbot.sessions)} session(s)[/cyan]")
    for s in alice_chatbot.sessions:
        print(
            f"     - {s.id[:12]}... [dim](updated: {datetime.fromtimestamp(s.last_update_time, tz=timezone.utc)})[/dim]"
        )

    print("\n[cyan]2. Bob's chatbot sessions:[/cyan]")
    bob_chatbot = await service.list_sessions(app_name="chatbot", user_id="bob")
    print(f"   [cyan]Found {len(bob_chatbot.sessions)} session(s)[/cyan]")
    for s in bob_chatbot.sessions:
        print(f"     - {s.id[:12]}...")

    print("\n[cyan]3. Alice's assistant sessions:[/cyan]")
    alice_assistant = await service.list_sessions(app_name="assistant", user_id="alice")
    print(f"   [cyan]Found {len(alice_assistant.sessions)} session(s)[/cyan]")
    for s in alice_assistant.sessions:
        print(f"     - {s.id[:12]}...")

    print("\n[cyan]4. Carol's assistant sessions:[/cyan]")
    carol_assistant = await service.list_sessions(app_name="assistant", user_id="carol")
    print(f"   [cyan]Found {len(carol_assistant.sessions)} session(s)[/cyan]")
    for s in carol_assistant.sessions:
        print(f"     - {s.id[:12]}...")

    print("\n[bold cyan]=== Cross-Tenant Access Protection ===[/bold cyan]")
    print("\n[yellow]Attempting to access Bob's session as Alice...[/yellow]")
    bob_session_as_alice = await service.get_session(app_name="chatbot", user_id="alice", session_id=chatbot_bob)

    if bob_session_as_alice is None:
        print("[green]✅ Access denied - tenant isolation working![/green]")
    else:
        print("[red]❌ SECURITY ISSUE - should not have access![/red]")

    print("\n[yellow]Attempting to access Bob's session correctly (as Bob)...[/yellow]")
    bob_session_as_bob = await service.get_session(app_name="chatbot", user_id="bob", session_id=chatbot_bob)

    if bob_session_as_bob:
        print(f"[green]✅ Access granted - retrieved {len(bob_session_as_bob.events)} event(s)[/green]")
    else:
        print("[red]❌ Should have access but got None[/red]")

    print("\n[bold cyan]=== Aggregated Statistics ===[/bold cyan]")
    all_apps = ["chatbot", "assistant"]
    all_users = ["alice", "bob", "carol"]

    stats = {}
    for app in all_apps:
        stats[app] = {}
        for user in all_users:
            sessions = await service.list_sessions(app_name=app, user_id=user)
            stats[app][user] = len(sessions.sessions)

    print("\n[cyan]Session count by tenant:[/cyan]")
    print(f"[bold]{'App':<12} {'Alice':<8} {'Bob':<8} {'Carol':<8}[/bold]")
    print("-" * 40)
    for app in all_apps:
        print(f"{app:<12} {stats[app]['alice']:<8} {stats[app]['bob']:<8} {stats[app]['carol']:<8}")

    total = sum(sum(users.values()) for users in stats.values())
    print(f"\n[cyan]Total sessions across all tenants:[/cyan] {total}")

    print("\n[bold cyan]=== Selective Cleanup ===[/bold cyan]")
    print("\n[yellow]Deleting all of Alice's chatbot sessions...[/yellow]")
    for session in alice_chatbot.sessions:
        await service.delete_session(app_name="chatbot", user_id="alice", session_id=session.id)
    print(f"[cyan]Deleted {len(alice_chatbot.sessions)} session(s)[/cyan]")

    remaining = await service.list_sessions(app_name="chatbot", user_id="alice")
    print(f"[cyan]Alice's remaining chatbot sessions:[/cyan] {len(remaining.sessions)}")

    bob_remaining = await service.list_sessions(app_name="chatbot", user_id="bob")
    print(f"[cyan]Bob's chatbot sessions (unchanged):[/cyan] {len(bob_remaining.sessions)}")

    print("\n[bold cyan]=== Full Cleanup ===[/bold cyan]")

    cleanup_map = [
        ("chatbot", "bob", chatbot_bob),
        ("assistant", "alice", assistant_alice),
        ("assistant", "carol", assistant_carol),
    ]

    for app, user, session_id in cleanup_map:
        await service.delete_session(app_name=app, user_id=user, session_id=session_id)

    print("[cyan]Deleted all remaining sessions[/cyan]")

    final_stats = {}
    for app in all_apps:
        for user in all_users:
            sessions = await service.list_sessions(app_name=app, user_id=user)
            if len(sessions.sessions) > 0:
                final_stats[f"{app}/{user}"] = len(sessions.sessions)

    if final_stats:
        print(f"[yellow]⚠️  Remaining sessions:[/yellow] {final_stats}")
    else:
        print("[green]✅ All sessions cleaned up successfully[/green]")


def main() -> None:
    """Run the multi-tenant example."""
    print("[bold magenta]=== Multi-Tenant ADK Session Management Example ===[/bold magenta]")
    try:
        asyncio.run(run_multi_tenant_example())
        print("\n[green]✅ Multi-tenant demo completed successfully![/green]")
    except Exception as e:
        print(f"\n[red]❌ Example failed: {e}[/red]")
        print("[yellow]Make sure PostgreSQL is running with:[/yellow] [cyan]make infra-up[/cyan]")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
