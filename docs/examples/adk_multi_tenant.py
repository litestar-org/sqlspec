"""Example: Multi-tenant ADK session management.

This example demonstrates managing sessions for multiple applications
and users in a single database, showing proper isolation via app_name
and user_id.

Requirements:
    - PostgreSQL running locally (default port 5432)
    - pip install sqlspec[asyncpg,adk] google-genai

Usage:
    python docs/examples/adk_multi_tenant.py
"""

import asyncio
from datetime import datetime, timezone

from google.adk.events.event import Event
from google.genai import types

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
    print("✅ ADK tables ready for multi-tenant demo")

    service = SQLSpecSessionService(store)

    print("\n=== Scenario: Multiple Apps and Users ===")
    print("Creating sessions for different apps and users...")

    chatbot_alice_1 = await create_sample_session(
        service,
        app_name="chatbot",
        user_id="alice",
        messages=[("user", "Hello!"), ("assistant", "Hi Alice! How can I help?")],
    )
    print(f"  Created: chatbot/alice/{chatbot_alice_1[:8]}...")

    chatbot_alice_2 = await create_sample_session(
        service,
        app_name="chatbot",
        user_id="alice",
        messages=[("user", "What's the weather?"), ("assistant", "It's sunny today!")],
    )
    print(f"  Created: chatbot/alice/{chatbot_alice_2[:8]}...")

    chatbot_bob = await create_sample_session(
        service, app_name="chatbot", user_id="bob", messages=[("user", "Help me!"), ("assistant", "Sure, Bob!")]
    )
    print(f"  Created: chatbot/bob/{chatbot_bob[:8]}...")

    assistant_alice = await create_sample_session(
        service,
        app_name="assistant",
        user_id="alice",
        messages=[("user", "Summarize this document"), ("assistant", "Here's a summary...")],
    )
    print(f"  Created: assistant/alice/{assistant_alice[:8]}...")

    assistant_carol = await create_sample_session(
        service,
        app_name="assistant",
        user_id="carol",
        messages=[("user", "Schedule a meeting"), ("assistant", "Meeting scheduled!")],
    )
    print(f"  Created: assistant/carol/{assistant_carol[:8]}...")

    print("\n=== Tenant Isolation Demo ===")

    print("\n1. Alice's chatbot sessions:")
    alice_chatbot = await service.list_sessions(app_name="chatbot", user_id="alice")
    print(f"   Found {len(alice_chatbot.sessions)} session(s)")
    for s in alice_chatbot.sessions:
        print(f"     - {s.id[:12]}... (updated: {datetime.fromtimestamp(s.last_update_time, tz=timezone.utc)})")

    print("\n2. Bob's chatbot sessions:")
    bob_chatbot = await service.list_sessions(app_name="chatbot", user_id="bob")
    print(f"   Found {len(bob_chatbot.sessions)} session(s)")
    for s in bob_chatbot.sessions:
        print(f"     - {s.id[:12]}...")

    print("\n3. Alice's assistant sessions:")
    alice_assistant = await service.list_sessions(app_name="assistant", user_id="alice")
    print(f"   Found {len(alice_assistant.sessions)} session(s)")
    for s in alice_assistant.sessions:
        print(f"     - {s.id[:12]}...")

    print("\n4. Carol's assistant sessions:")
    carol_assistant = await service.list_sessions(app_name="assistant", user_id="carol")
    print(f"   Found {len(carol_assistant.sessions)} session(s)")
    for s in carol_assistant.sessions:
        print(f"     - {s.id[:12]}...")

    print("\n=== Cross-Tenant Access Protection ===")
    print("\nAttempting to access Bob's session as Alice...")
    bob_session_as_alice = await service.get_session(app_name="chatbot", user_id="alice", session_id=chatbot_bob)

    if bob_session_as_alice is None:
        print("✅ Access denied - tenant isolation working!")
    else:
        print("❌ SECURITY ISSUE - should not have access!")

    print("\nAttempting to access Bob's session correctly (as Bob)...")
    bob_session_as_bob = await service.get_session(app_name="chatbot", user_id="bob", session_id=chatbot_bob)

    if bob_session_as_bob:
        print(f"✅ Access granted - retrieved {len(bob_session_as_bob.events)} event(s)")
    else:
        print("❌ Should have access but got None")

    print("\n=== Aggregated Statistics ===")
    all_apps = ["chatbot", "assistant"]
    all_users = ["alice", "bob", "carol"]

    stats = {}
    for app in all_apps:
        stats[app] = {}
        for user in all_users:
            sessions = await service.list_sessions(app_name=app, user_id=user)
            stats[app][user] = len(sessions.sessions)

    print("\nSession count by tenant:")
    print(f"{'App':<12} {'Alice':<8} {'Bob':<8} {'Carol':<8}")
    print("-" * 40)
    for app in all_apps:
        print(f"{app:<12} {stats[app]['alice']:<8} {stats[app]['bob']:<8} {stats[app]['carol']:<8}")

    total = sum(sum(users.values()) for users in stats.values())
    print(f"\nTotal sessions across all tenants: {total}")

    print("\n=== Selective Cleanup ===")
    print("\nDeleting all of Alice's chatbot sessions...")
    for session in alice_chatbot.sessions:
        await service.delete_session(app_name="chatbot", user_id="alice", session_id=session.id)
    print(f"Deleted {len(alice_chatbot.sessions)} session(s)")

    remaining = await service.list_sessions(app_name="chatbot", user_id="alice")
    print(f"Alice's remaining chatbot sessions: {len(remaining.sessions)}")

    bob_remaining = await service.list_sessions(app_name="chatbot", user_id="bob")
    print(f"Bob's chatbot sessions (unchanged): {len(bob_remaining.sessions)}")

    print("\n=== Full Cleanup ===")

    cleanup_map = [
        ("chatbot", "bob", chatbot_bob),
        ("assistant", "alice", assistant_alice),
        ("assistant", "carol", assistant_carol),
    ]

    for app, user, session_id in cleanup_map:
        await service.delete_session(app_name=app, user_id=user, session_id=session_id)

    print("Deleted all remaining sessions")

    final_stats = {}
    for app in all_apps:
        for user in all_users:
            sessions = await service.list_sessions(app_name=app, user_id=user)
            if len(sessions.sessions) > 0:
                final_stats[f"{app}/{user}"] = len(sessions.sessions)

    if final_stats:
        print(f"⚠️  Remaining sessions: {final_stats}")
    else:
        print("✅ All sessions cleaned up successfully")


def main() -> None:
    """Run the multi-tenant example."""
    print("=== Multi-Tenant ADK Session Management Example ===")
    try:
        asyncio.run(run_multi_tenant_example())
        print("\n✅ Multi-tenant demo completed successfully!")
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        print("Make sure PostgreSQL is running with: make infra-up")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
