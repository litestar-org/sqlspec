"""Live Spanner + Floci GCS Google ADK artifact store integration tests."""

from collections.abc import Awaitable
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, TypeVar
from uuid import uuid4

import pytest

from sqlspec.storage.registry import StorageRegistry
from sqlspec.utils.module_loader import import_optional, import_optional_attr
from tests.integration.fixtures.cloud_storage import floci_gcs_obstore_kwargs

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database
    from google.genai import types
    from pytest_databases.docker.floci import FlociGCPService
    from pytest_databases.docker.spanner import SpannerService

    from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
    from sqlspec.adapters.spanner.adk.artifact_store import SpannerAsyncADKArtifactStore, SpannerSyncADKArtifactStore
    from sqlspec.extensions.adk.artifact.service import SQLSpecArtifactService
    from sqlspec.protocols import ObjectStoreProtocol
else:
    types = import_optional("google.genai.types")
    SpannerAsyncConfig = import_optional_attr("sqlspec.adapters.spanner", "SpannerAsyncConfig")
    SpannerSyncConfig = import_optional_attr("sqlspec.adapters.spanner", "SpannerSyncConfig")
    SpannerAsyncADKArtifactStore = import_optional_attr(
        "sqlspec.adapters.spanner.adk.artifact_store", "SpannerAsyncADKArtifactStore"
    )
    SpannerSyncADKArtifactStore = import_optional_attr(
        "sqlspec.adapters.spanner.adk.artifact_store", "SpannerSyncADKArtifactStore"
    )
    SQLSpecArtifactService = import_optional_attr("sqlspec.extensions.adk.artifact.service", "SQLSpecArtifactService")

build_spanner_connection_config: Any = import_optional_attr(
    "tests.integration.fixtures.spanner", "build_spanner_connection_config"
)

pytest.importorskip("google.cloud.spanner")
pytest.importorskip("google.adk")
pytest.importorskip("google.genai")
pytest.importorskip("obstore")

pytestmark = [pytest.mark.spanner, pytest.mark.integration, pytest.mark.xdist_group("spanner")]

T = TypeVar("T")


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_spanner_adk_artifact_service_gcs_round_trip(
    mode: str,
    spanner_service: "SpannerService",
    spanner_database: "Database",
    floci_gcp_service: "FlociGCPService",
    floci_gcs_bucket_name: str,
    floci_gcs_obstore: "ObjectStoreProtocol",
) -> None:
    """Verify SQLSpecArtifactService with SpannerSyncADKArtifactStore and SpannerAsyncADKArtifactStore over Floci GCS."""
    del spanner_database
    suffix = uuid4().hex[:8]
    connection_config = build_spanner_connection_config(spanner_service)
    extension_config: dict[str, Any] = {"adk": {"artifact_table": f"adk_art_{suffix}"}}

    config: SpannerSyncConfig | SpannerAsyncConfig
    store: SpannerSyncADKArtifactStore | SpannerAsyncADKArtifactStore
    if mode == "sync":
        config = SpannerSyncConfig(connection_config=connection_config, extension_config=extension_config)
        store = SpannerSyncADKArtifactStore(config)
    else:
        config = SpannerAsyncConfig(connection_config=connection_config, extension_config=extension_config)
        store = SpannerAsyncADKArtifactStore(config)

    registry = StorageRegistry()
    registry.register_alias(
        "test-gcs-artifacts",
        f"gs://{floci_gcs_bucket_name}",
        backend="obstore",
        **floci_gcs_obstore_kwargs(floci_gcp_service),
    )
    service = SQLSpecArtifactService(store, "test-gcs-artifacts", registry=registry)

    try:
        await _resolve(store.create_table())

        v0_part = types.Part.from_bytes(data=b"report-v0-payload", mime_type="text/plain")
        v1_part = types.Part.from_bytes(data=b"report-v1-payload", mime_type="text/plain")
        user_part = types.Part.from_bytes(data=b'{"theme":"dark"}', mime_type="application/json")

        v0 = await service.save_artifact(
            app_name="app1",
            user_id="u1",
            session_id="s1",
            filename="report.txt",
            artifact=v0_part,
            custom_metadata={"stage": "draft"},
        )
        v1 = await service.save_artifact(
            app_name="app1",
            user_id="u1",
            session_id="s1",
            filename="report.txt",
            artifact=v1_part,
            custom_metadata={"stage": "final"},
        )
        user_v0 = await service.save_artifact(
            app_name="app1", user_id="u1", session_id="s1", filename="user:prefs.json", artifact=user_part
        )
        assert (v0, v1, user_v0) == (0, 1, 0)

        loaded_latest = await service.load_artifact(
            app_name="app1", user_id="u1", session_id="s1", filename="report.txt"
        )
        assert loaded_latest is not None
        assert loaded_latest.inline_data is not None
        assert loaded_latest.inline_data.data == b"report-v1-payload"

        loaded_v0 = await service.load_artifact(
            app_name="app1", user_id="u1", session_id="s1", filename="report.txt", version=0
        )
        assert loaded_v0 is not None
        assert loaded_v0.inline_data is not None
        assert loaded_v0.inline_data.data == b"report-v0-payload"

        loaded_user = await service.load_artifact(app_name="app1", user_id="u1", filename="user:prefs.json")
        assert loaded_user is not None
        assert loaded_user.inline_data is not None
        assert loaded_user.inline_data.data == b'{"theme":"dark"}'

        session_keys = await service.list_artifact_keys(app_name="app1", user_id="u1", session_id="s1")
        assert set(session_keys) == {"report.txt", "user:prefs.json"}
        user_only_keys = await service.list_artifact_keys(app_name="app1", user_id="u1")
        assert user_only_keys == ["user:prefs.json"]

        versions = await service.list_versions(app_name="app1", user_id="u1", session_id="s1", filename="report.txt")
        assert versions == [0, 1]

        v0_meta = await service.get_artifact_version(
            app_name="app1", user_id="u1", session_id="s1", filename="report.txt", version=0
        )
        v1_meta = await service.get_artifact_version(
            app_name="app1", user_id="u1", session_id="s1", filename="report.txt", version=1
        )
        assert v0_meta is not None
        assert v1_meta is not None
        assert v0_meta.canonical_uri.startswith("test-gcs-artifacts/apps/")
        assert v1_meta.canonical_uri.startswith("test-gcs-artifacts/apps/")
        assert v1_meta.custom_metadata == {"stage": "final"}

        v0_content_path = v0_meta.canonical_uri.removeprefix("test-gcs-artifacts/")
        v1_content_path = v1_meta.canonical_uri.removeprefix("test-gcs-artifacts/")
        assert floci_gcs_obstore.exists_sync(v0_content_path) is True
        assert floci_gcs_obstore.exists_sync(v1_content_path) is True

        await service.delete_artifact(app_name="app1", user_id="u1", session_id="s1", filename="report.txt")
        assert (
            await service.load_artifact(app_name="app1", user_id="u1", session_id="s1", filename="report.txt") is None
        )
        assert floci_gcs_obstore.exists_sync(v0_content_path) is False
        assert floci_gcs_obstore.exists_sync(v1_content_path) is False

        await service.delete_artifact(app_name="app1", user_id="u1", filename="user:prefs.json")
        assert await service.load_artifact(app_name="app1", user_id="u1", filename="user:prefs.json") is None
    finally:
        try:
            await _resolve(store.drop_table())
        finally:
            await _resolve(config.close_pool())


async def _resolve(result: "T | Awaitable[T]") -> T:
    """Await a store/config result when async or return directly when sync."""
    if isawaitable(result):
        return await result
    return result
