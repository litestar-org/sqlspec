from __future__ import annotations

from datetime import datetime, timezone

import anyio
import pytest

__all__ = ("test_adk_memory_store",)


def test_adk_memory_store() -> None:
    pytest.importorskip("aiosqlite")

    async def _run() -> list[dict[str, object]]:
        # start-example
        from sqlspec.adapters.aiosqlite import AiosqliteConfig
        from sqlspec.adapters.aiosqlite.adk import AiosqliteADKStore

        config = AiosqliteConfig(connection_config={"database": ":memory:"})
        store = AiosqliteADKStore(config)
        await store.ensure_tables()

        session = await store.create_session(
            session_id="session_1", app_name="docs", user_id="user_1", state={"mode": "demo"}
        )

        event_record = {
            "id": "evt_1",
            "app_name": "docs",
            "user_id": "user_1",
            "session_id": session["id"],
            "invocation_id": "inv_1",
            "author": "user",
            "branch": "main",
            "actions": b"",
            "long_running_tool_ids_json": None,
            "timestamp": datetime.now(timezone.utc),
            "content": {"text": "Hello"},
            "grounding_metadata": None,
            "custom_metadata": None,
            "partial": False,
            "turn_complete": True,
            "interrupted": None,
            "error_code": None,
            "error_message": None,
        }
        await store.append_event(event_record)
        events = await store.get_events(session_id=session["id"])
        # end-example
        return events

    events = anyio.run(_run)

    assert len(events) == 1
