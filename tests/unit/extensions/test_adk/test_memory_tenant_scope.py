"""Tenant scoping of ADK memory entry ids and session deletes."""

import importlib.util
import inspect
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

if importlib.util.find_spec("google.genai") is None or importlib.util.find_spec("google.adk") is None:
    pytest.skip("google-adk not installed", allow_module_level=True)

from google.adk.memory.memory_entry import MemoryEntry
from google.genai import types

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.aiosqlite.adk import AiosqliteADKMemoryStore
from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.adapters.duckdb.adk import DuckDBADKMemoryStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.adapters.sqlite.adk import SqliteADKMemoryStore
from sqlspec.extensions.adk.memory.service import SQLSpecMemoryService

_UNKNOWN_SESSION_ID = "__unknown_session_id__"
_EXTENSION_CONFIG: dict[str, Any] = {"adk": {"enable_memory": True}}


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@pytest.fixture(params=["sqlite", "aiosqlite", "duckdb"])
async def memory_store(request: pytest.FixtureRequest, tmp_path: Path) -> "AsyncIterator[Any]":
    kind = request.param
    if kind == "sqlite":
        config: Any = SqliteConfig(
            connection_config={"database": str(tmp_path / "mem.db")}, extension_config=_EXTENSION_CONFIG
        )
        store: Any = SqliteADKMemoryStore(config)
    elif kind == "aiosqlite":
        config = AiosqliteConfig(
            connection_config={"database": str(tmp_path / "mem.db")}, extension_config=_EXTENSION_CONFIG
        )
        store = AiosqliteADKMemoryStore(config)
    else:
        config = DuckDBConfig(
            connection_config={"database": str(tmp_path / "mem.duckdb")}, extension_config=_EXTENSION_CONFIG
        )
        store = DuckDBADKMemoryStore(config)
    await _maybe_await(store.create_tables())
    try:
        yield store
    finally:
        await _maybe_await(config.close_pool())


def _entry(entry_id: str, text: str) -> MemoryEntry:
    return MemoryEntry(id=entry_id, author="user", content=types.Content(role="user", parts=[types.Part(text=text)]))


async def _add(service: SQLSpecMemoryService, app_name: str, user_id: str, entry_id: str, text: str) -> None:
    await service.add_memory(app_name=app_name, user_id=user_id, memories=[_entry(entry_id, text)])


async def _search(service: SQLSpecMemoryService, app_name: str, user_id: str, query: str) -> list[MemoryEntry]:
    response = await service.search_memory(app_name=app_name, user_id=user_id, query=query)
    return list(response.memories)


async def test_same_entry_id_stored_for_each_tenant(memory_store: Any) -> None:
    service = SQLSpecMemoryService(memory_store)
    await _add(service, "app", "mallory", "profile", "squatted profile")
    await _add(service, "app", "alice", "profile", "alice real profile data")

    found = await _search(service, "app", "alice", "profile")

    assert [entry.content.parts[0].text for entry in found if entry.content and entry.content.parts] == [
        "alice real profile data"
    ]


async def test_same_entry_id_twice_in_one_tenant_is_idempotent(memory_store: Any) -> None:
    service = SQLSpecMemoryService(memory_store)
    await _add(service, "app", "alice", "profile", "alice profile data")
    await _add(service, "app", "alice", "profile", "alice profile data")

    assert len(await _search(service, "app", "alice", "profile")) == 1


async def test_entry_id_round_trips(memory_store: Any) -> None:
    service = SQLSpecMemoryService(memory_store)
    await _add(service, "app", "alice", "profile", "alice profile data")

    found = await _search(service, "app", "alice", "profile")

    assert [entry.id for entry in found] == ["profile"]


async def test_delete_by_session_only_touches_owner(memory_store: Any) -> None:
    service = SQLSpecMemoryService(memory_store)
    await _add(service, "app", "alice", "a1", "alice fact")
    await _add(service, "app2", "bob", "b1", "bob fact")

    deleted = await _maybe_await(memory_store.delete_entries_by_session(_UNKNOWN_SESSION_ID, "app", "alice"))

    assert deleted == 1
    assert len(await _search(service, "app2", "bob", "fact")) == 1
    assert await _search(service, "app", "alice", "fact") == []
