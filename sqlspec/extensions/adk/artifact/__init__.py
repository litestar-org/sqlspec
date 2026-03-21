"""Google ADK artifact service extension for SQLSpec.

Provides artifact versioning and storage for Google Agent Development Kit
using SQLSpec database adapters for metadata and ``sqlspec/storage/`` backends
for content.

Public API exports:
    - SQLSpecArtifactService: Main service implementing BaseArtifactService
    - BaseAsyncADKArtifactStore: Base class for async artifact metadata stores
    - BaseSyncADKArtifactStore: Base class for sync artifact metadata stores
    - ArtifactRecord: TypedDict for artifact metadata database records

Example:
    from sqlspec.adapters.asyncpg import AsyncpgConfig
    from sqlspec.extensions.adk.artifact import SQLSpecArtifactService

    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://..."},
        extension_config={
            "adk": {
                "artifact_table": "adk_artifact_versions",
            }
        }
    )

    # Create an adapter-specific artifact store (e.g., AsyncpgADKArtifactStore)
    # and ensure tables exist:
    artifact_store = AsyncpgADKArtifactStore(config)
    await artifact_store.ensure_table()

    # Create the service with a storage backend URI:
    service = SQLSpecArtifactService(
        store=artifact_store,
        artifact_storage_uri="s3://my-bucket/adk-artifacts/",
    )

    # Save an artifact (returns version number starting from 0):
    version = await service.save_artifact(
        app_name="my_app",
        user_id="user123",
        filename="report.pdf",
        artifact=part,
    )

    # Load artifact content:
    loaded = await service.load_artifact(
        app_name="my_app",
        user_id="user123",
        filename="report.pdf",
    )
"""

from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.extensions.adk.artifact.service import SQLSpecArtifactService
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore

__all__ = ("ArtifactRecord", "BaseAsyncADKArtifactStore", "BaseSyncADKArtifactStore", "SQLSpecArtifactService")
