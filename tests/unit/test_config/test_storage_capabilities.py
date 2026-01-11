from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.config import NoPoolSyncConfig
from sqlspec.driver import SyncDataDictionaryBase, SyncDriverAdapterBase
from tests.conftest import requires_interpreted

pytestmark = requires_interpreted


if TYPE_CHECKING:
    _NoPoolSyncConfigBase = NoPoolSyncConfig[Any, "_DummyDriver"]
else:
    _NoPoolSyncConfigBase = NoPoolSyncConfig


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
