"""Google ADK artifact service extension for SQLSpec.

Provides artifact versioning and storage for Google Agent Development Kit
using SQLSpec database adapters for metadata and ``sqlspec/storage/`` backends
for content.

Public API exports:
    - SQLSpecArtifactService: Main service implementing BaseArtifactService
    - BaseAsyncADKArtifactStore: Base class for async artifact metadata stores
    - BaseSyncADKArtifactStore: Base class for sync artifact metadata stores
    - ArtifactRecord: TypedDict for artifact metadata database records
"""

from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.extensions.adk.artifact.service import SQLSpecArtifactService
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore

__all__ = ("ArtifactRecord", "BaseAsyncADKArtifactStore", "BaseSyncADKArtifactStore", "SQLSpecArtifactService")
