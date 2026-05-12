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
            "session_id": session["id"],
            "invocation_id": "inv_1",
            "author": "user",
            "timestamp": datetime.now(timezone.utc),
            "event_json": {
                "id": "evt_1",
                "invocation_id": "inv_1",
                "author": "user",
                "content": {"parts": [{"text": "Hello"}]},
            },
        }
        await store.append_event(event_record)
        events = await store.get_events(session_id=session["id"])
        # end-example
        return events

    events = anyio.run(_run)

    assert len(events) == 1
