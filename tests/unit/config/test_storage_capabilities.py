from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any

import pytest

from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
from sqlspec.core.config_runtime import (
    build_default_statement_config,
    close_async_pool,
    close_sync_pool,
    create_async_pool,
    create_sync_pool,
    seed_runtime_driver_features,
)
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from tests.conftest import requires_interpreted

pytestmark = requires_interpreted


if TYPE_CHECKING:
    _NoPoolSyncConfigBase = NoPoolSyncConfig[Any, "_DummyDriver"]
    _NoPoolAsyncConfigBase = NoPoolAsyncConfig[Any, "_AsyncDummyDriver"]
    _SyncPoolConfigBase = SyncDatabaseConfig[Any, object, "_DummyDriver"]
    _AsyncPoolConfigBase = AsyncDatabaseConfig[Any, object, "_AsyncDummyDriver"]
else:
    _NoPoolSyncConfigBase = NoPoolSyncConfig
    _NoPoolAsyncConfigBase = NoPoolAsyncConfig
    _SyncPoolConfigBase = SyncDatabaseConfig
    _AsyncPoolConfigBase = AsyncDatabaseConfig


class _DummyDriver(SyncDriverAdapterBase):
    __slots__ = ()

    @property
    def data_dictionary(self) -> SyncDataDictionaryBase:  # type: ignore[override]
        raise NotImplementedError

    def with_cursor(self, connection: Any) -> AbstractContextManager[Any]:  # type: ignore[override]
        @contextmanager
        def _cursor_ctx():
            yield object()

        return _cursor_ctx()

    def handle_database_exceptions(self) -> AbstractContextManager[None]:  # type: ignore[override]
        @contextmanager
        def _handler_ctx():
            yield None

        return _handler_ctx()

    def begin(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    def rollback(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    def commit(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    def dispatch_special_handling(self, cursor: Any, statement: Any):  # type: ignore[override]
        return None

    def dispatch_execute_script(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    def dispatch_execute_many(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    def dispatch_execute(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError


class _AsyncDummyDriver(AsyncDriverAdapterBase):
    __slots__ = ()

    @property
    def data_dictionary(self) -> AsyncDataDictionaryBase:  # type: ignore[override]
        raise NotImplementedError

    @asynccontextmanager
    async def with_cursor(self, connection: Any):  # type: ignore[override]
        yield object()

    @asynccontextmanager
    async def handle_database_exceptions(self):  # type: ignore[override]
        yield None

    async def begin(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    async def rollback(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    async def commit(self) -> None:  # type: ignore[override]
        raise NotImplementedError

    async def dispatch_special_handling(self, cursor: Any, statement: Any):  # type: ignore[override]
        return None

    async def dispatch_execute_script(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    async def dispatch_execute_many(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError

    async def dispatch_execute(self, cursor: Any, statement: Any):  # type: ignore[override]
        raise NotImplementedError


class _CapabilityConfig(_NoPoolSyncConfigBase):
    driver_type = _DummyDriver
    connection_type = object
    supports_native_arrow_export = True
    supports_native_arrow_import = True
    supports_native_parquet_export = False
    supports_native_parquet_import = False
    requires_staging_for_load = True
    staging_protocols = ("s3://",)
    storage_partition_strategies = ("fixed", "rows_per_chunk")
    default_storage_profile = "local-temp"

    def create_connection(self) -> object:
        return object()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()


class _AsyncCapabilityConfig(_NoPoolAsyncConfigBase):
    driver_type = _AsyncDummyDriver
    connection_type = object
    supports_native_arrow_export = True
    supports_native_arrow_import = True
    requires_staging_for_load = True
    staging_protocols = ("s3://",)
    storage_partition_strategies = ("fixed", "rows_per_chunk")

    async def create_connection(self) -> object:
        return object()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()


class _SyncPoolConfig(_SyncPoolConfigBase):
    driver_type = _DummyDriver
    connection_type = object

    def create_connection(self) -> object:
        return object()

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()

    def _create_pool(self) -> object:
        return object()

    def _close_pool(self) -> None:
        return None


class _AsyncPoolConfig(_AsyncPoolConfigBase):
    driver_type = _AsyncDummyDriver
    connection_type = object

    async def create_connection(self) -> object:
        return object()

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any):  # type: ignore[override]
        yield object()

    async def _create_pool(self) -> object:
        return object()

    async def _close_pool(self) -> None:
        return None


def test_storage_capabilities_snapshot(monkeypatch):
    monkeypatch.setattr(_CapabilityConfig, "_dependency_available", staticmethod(lambda checker: True))
    config = _CapabilityConfig()

    capabilities = config.storage_capabilities()
    assert capabilities["arrow_export_enabled"] is True
    assert capabilities["arrow_import_enabled"] is True
    assert capabilities["parquet_export_enabled"] is False
    assert capabilities["requires_staging_for_load"] is True
    assert capabilities["partition_strategies"] == ["fixed", "rows_per_chunk"]
    assert capabilities["default_storage_profile"] == "local-temp"

    capabilities["arrow_export_enabled"] = False
    assert config.storage_capabilities()["arrow_export_enabled"] is True

    monkeypatch.setattr(_CapabilityConfig, "supports_native_arrow_export", False)
    config.reset_storage_capabilities_cache()
    assert config.storage_capabilities()["arrow_export_enabled"] is False


def test_driver_features_seed_capabilities(monkeypatch):
    monkeypatch.setattr(_CapabilityConfig, "_dependency_available", staticmethod(lambda checker: False))
    config = _CapabilityConfig()
    assert "storage_capabilities" in config.driver_features
    snapshot = config.driver_features["storage_capabilities"]
    assert isinstance(snapshot, dict)


def test_async_driver_features_seed_capabilities(monkeypatch):
    monkeypatch.setattr(_AsyncCapabilityConfig, "_dependency_available", staticmethod(lambda checker: False))
    config = _AsyncCapabilityConfig()
    assert "storage_capabilities" in config.driver_features
    snapshot = config.driver_features["storage_capabilities"]
    assert isinstance(snapshot, dict)


def test_build_default_statement_config_uses_requested_dialect() -> None:
    statement_config = build_default_statement_config("postgres")
    assert statement_config.dialect == "postgres"


def test_seed_runtime_driver_features_preserves_existing_values() -> None:
    seeded = seed_runtime_driver_features({"custom": "value"}, {"arrow_export_enabled": True})
    assert seeded["custom"] == "value"
    assert seeded["storage_capabilities"] == {"arrow_export_enabled": True}


def test_create_sync_pool_emits_observability_once() -> None:
    emitted: list[object] = []
    created: list[object] = []
    config = _SyncPoolConfig()
    lock = config._pool_lock  # pyright: ignore[reportPrivateUsage]

    def _factory() -> object:
        pool = object()
        created.append(pool)
        return pool

    pool = create_sync_pool(None, lock, lambda: None, _factory, emitted.append)

    assert pool is created[0]
    assert emitted == [pool]


def test_close_sync_pool_emits_observability_once() -> None:
    closed: list[str] = []
    emitted: list[object] = []
    pool = object()

    close_sync_pool(pool, lambda: closed.append("closed"), emitted.append)

    assert closed == ["closed"]
    assert emitted == [pool]


@pytest.mark.anyio
async def test_create_async_pool_emits_observability_once() -> None:
    emitted: list[object] = []
    created: list[object] = []
    config = _AsyncPoolConfig()
    lock = config._pool_lock  # pyright: ignore[reportPrivateUsage]

    async def _factory() -> object:
        pool = object()
        created.append(pool)
        return pool

    pool = await create_async_pool(None, lock, lambda: None, _factory, emitted.append)

    assert pool is created[0]
    assert emitted == [pool]


@pytest.mark.anyio
async def test_close_async_pool_emits_observability_once() -> None:
    closed: list[str] = []
    emitted: list[object] = []
    pool = object()

    async def _closer() -> None:
        closed.append("closed")

    await close_async_pool(pool, _closer, emitted.append)

    assert closed == ["closed"]
    assert emitted == [pool]
