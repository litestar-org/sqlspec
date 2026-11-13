"""ADK store foundation preserves primary identity and scoped read selection."""

import asyncio
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk.artifact.store import _ADKArtifactStoreCommon
from sqlspec.extensions.adk.memory.store import _ADKMemoryStoreCommon
from sqlspec.extensions.adk.store import _ADKStoreCommon, adk_primary_read_scope
from sqlspec.utils.routing import RoutingContext


class _ReadProbe(Protocol):
    def _provide_read_session(
        self, *, primary: bool = False, **kwargs: Any
    ) -> AbstractContextManager[Any] | AbstractAsyncContextManager[Any]: ...


def _read_label(self: _ReadProbe, *, primary: bool = False) -> str:
    with cast("AbstractContextManager[SqliteDriver]", self._provide_read_session(primary=primary)) as session:
        return str(session.select_value("SELECT origin FROM sources"))


class _SessionProbe(_ADKStoreCommon[SqliteConfig]):
    __slots__ = ()
    read_label = _read_label


class _MemoryProbe(_ADKMemoryStoreCommon[SqliteConfig]):
    __slots__ = ()
    read_label = _read_label


class _ArtifactProbe(_ADKArtifactStoreCommon[SqliteConfig]):
    __slots__ = ()
    read_label = _read_label


@pytest.mark.parametrize("store_type", [_SessionProbe, _MemoryProbe, _ArtifactProbe])
def test_adk_read_selection_preserves_primary_and_scope_reset(
    tmp_path: Path, store_type: type[_SessionProbe] | type[_MemoryProbe] | type[_ArtifactProbe]
) -> None:
    writer = SqliteConfig(connection_config={"database": str(tmp_path / "writer.db")}, bind_key="agents")
    reader = SqliteConfig(connection_config={"database": str(tmp_path / "reader.db")}, bind_key="readers")
    for config, label in ((writer, "writer"), (reader, "reader")):
        with config.provide_session() as session:
            session.execute("CREATE TABLE sources (origin TEXT)")
            session.execute("INSERT INTO sources VALUES (:origin)", {"origin": label})
    store = store_type(writer, read_config=reader)
    assert store.config is writer
    assert store.read_config is reader
    assert store.read_config_enabled is True
    assert store.read_label() == "reader"
    assert store.read_label(primary=True) == "writer"
    with pytest.raises(RuntimeError, match="restore"), adk_primary_read_scope():
        with adk_primary_read_scope():
            assert store.read_label() == "writer"
        raise RuntimeError("restore")
    assert store.read_label() == "reader"
    with RoutingContext.primary("agents"):
        assert store.read_label() == "writer"
    with RoutingContext.scope():
        RoutingContext.mark_write("agents")
        assert store.read_label() == "writer"
    writer.close_pool()
    reader.close_pool()


def test_adk_offload_defaults_kill_switch_and_validation() -> None:
    writer = SqliteConfig()
    assert _SessionProbe(writer).read_config_enabled is False
    disabled = SqliteConfig(extension_config={"adk": {"read_config_enabled": False}})
    assert _SessionProbe(disabled, read_config=SqliteConfig()).read_config_enabled is False
    with pytest.raises(ImproperConfigurationError, match="boolean"):
        _SessionProbe(SqliteConfig(extension_config={"adk": {"read_config_enabled": "yes"}}))
    with pytest.raises(ImproperConfigurationError, match="same adapter"):
        _SessionProbe(writer, read_config=cast("SqliteConfig", AiosqliteConfig()))


async def test_adk_primary_scope_isolated_between_concurrent_tasks(tmp_path: Path) -> None:
    writer = AiosqliteConfig(connection_config={"database": str(tmp_path / "writer.db")})
    reader = AiosqliteConfig(connection_config={"database": str(tmp_path / "reader.db")})
    for config, label in ((writer, "writer"), (reader, "reader")):
        async with config.provide_session() as session:
            await session.execute("CREATE TABLE sources (origin TEXT)")
            await session.execute("INSERT INTO sources VALUES (:origin)", {"origin": label})
            await session.commit()
    store = _ADKStoreCommon(writer, read_config=reader)

    async def read_label() -> str:
        async with cast("AbstractAsyncContextManager[AiosqliteDriver]", store._provide_read_session()) as session:
            return str(await session.select_value("SELECT origin FROM sources"))

    async def primary_task() -> str:
        with adk_primary_read_scope():
            await asyncio.sleep(0)
            return await read_label()

    primary_label, replica_label = await asyncio.gather(primary_task(), read_label())
    assert (primary_label, replica_label) == ("writer", "reader")
    await writer.close_pool()
    await reader.close_pool()
