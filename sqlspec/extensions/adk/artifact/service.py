"""SQLSpec-backed artifact service for Google ADK.

Implements ``BaseArtifactService`` by composing SQL-backed metadata storage
(via :class:`BaseAsyncADKArtifactStore`) with ``sqlspec/storage/`` content
backends (via :class:`StorageRegistry`).

Metadata (version, filename, MIME type, custom metadata, canonical URI) lives
in a SQL table.  Content bytes live in object storage addressed by canonical
URI.  Versioning is append-only with monotonically increasing version numbers
starting from 0.
"""

import json
import logging
import re
from typing import TYPE_CHECKING, Any

from google.adk.artifacts.base_artifact_service import BaseArtifactService

from sqlspec.extensions.adk.artifact._types import ArtifactRecord
from sqlspec.storage.registry import StorageRegistry, storage_registry
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from google.adk.artifacts.base_artifact_service import ArtifactVersion
    from google.genai import types

    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore

logger = get_logger("sqlspec.extensions.adk.artifact.service")

__all__ = ("SQLSpecArtifactService",)

# Matches path traversal and absolute path components
_UNSAFE_PATH_CHARS = re.compile(r"(?:^|/)\.\.(?:/|$)|[\x00]")


def _sanitize_path_component(value: str) -> str:
    """Sanitize a path component to prevent directory traversal.

    Removes leading/trailing slashes, rejects ``..`` traversals, and
    replaces NUL bytes.

    Args:
        value: Raw path component.

    Returns:
        Sanitized path component.

    Raises:
        ValueError: If the value contains path traversal sequences.
    """
    value = value.strip("/")
    if _UNSAFE_PATH_CHARS.search(value):
        msg = f"Unsafe path component: {value!r}"
        raise ValueError(msg)
    return value


def _build_content_path(
    app_name: str, user_id: str, filename: str, version: int, session_id: "str | None" = None
) -> str:
    """Build the storage path for artifact content.

    Pattern:
        ``apps/{app_name}/users/{user_id}/[sessions/{session_id}/]artifacts/{filename}/v{version}``

    All path components are sanitized to prevent directory traversal.

    Args:
        app_name: Application name.
        user_id: User identifier.
        filename: Artifact filename.
        version: Version number.
        session_id: Optional session identifier.

    Returns:
        Sanitized storage path.
    """
    parts = ["apps", _sanitize_path_component(app_name), "users", _sanitize_path_component(user_id)]
    if session_id is not None:
        parts.extend(["sessions", _sanitize_path_component(session_id)])
    parts.extend(["artifacts", _sanitize_path_component(filename), f"v{version}"])
    return "/".join(parts)


def _extract_mime_type(artifact: "types.Part | dict[str, Any]") -> "str | None":
    """Extract MIME type from an artifact Part.

    Checks ``inline_data.mime_type`` and ``file_data.mime_type`` on the Part.

    Args:
        artifact: ADK Part or dict representation.

    Returns:
        MIME type string, or None if not determinable.
    """
    if isinstance(artifact, dict):
        # Handle camelCase and snake_case keys
        inline = artifact.get("inline_data") or artifact.get("inlineData")
        if isinstance(inline, dict):
            return inline.get("mime_type") or inline.get("mimeType")
        file_data = artifact.get("file_data") or artifact.get("fileData")
        if isinstance(file_data, dict):
            return file_data.get("mime_type") or file_data.get("mimeType")
        return None

    # types.Part object
    if hasattr(artifact, "inline_data") and artifact.inline_data is not None:
        return getattr(artifact.inline_data, "mime_type", None)
    if hasattr(artifact, "file_data") and artifact.file_data is not None:
        return getattr(artifact.file_data, "mime_type", None)
    return None


def _serialize_artifact(artifact: "types.Part | dict[str, Any]") -> bytes:
    """Serialize an artifact Part to bytes for content storage.

    The artifact is serialized as JSON via ``model_dump(exclude_none=True)``.
    This preserves the full Part structure including text, inline_data,
    file_data, and any future Part fields.

    Args:
        artifact: ADK Part or dict representation.

    Returns:
        JSON-encoded bytes.
    """
    if isinstance(artifact, dict):
        return json.dumps(artifact, default=str).encode("utf-8")

    # Use Pydantic model serialization
    if hasattr(artifact, "model_dump"):
        data = artifact.model_dump(exclude_none=True)
        return json.dumps(data, default=str).encode("utf-8")

    # Fallback for unexpected types
    return json.dumps({"text": str(artifact)}).encode("utf-8")


def _deserialize_artifact(data: bytes) -> "types.Part":
    """Deserialize bytes back into an ADK Part.

    Args:
        data: JSON-encoded bytes from content storage.

    Returns:
        Reconstructed Part object.
    """
    from google.genai import types

    parsed = json.loads(data.decode("utf-8"))
    return types.Part.model_validate(parsed)


def _record_to_artifact_version(record: "ArtifactRecord") -> "ArtifactVersion":
    """Convert a database artifact record to an ADK ArtifactVersion.

    Args:
        record: Database artifact record.

    Returns:
        ArtifactVersion model instance.
    """
    from google.adk.artifacts.base_artifact_service import ArtifactVersion

    return ArtifactVersion(
        version=record["version"],
        canonical_uri=record["canonical_uri"],
        custom_metadata=record["custom_metadata"] or {},
        create_time=record["created_at"].timestamp(),
        mime_type=record["mime_type"],
    )


class SQLSpecArtifactService(BaseArtifactService):
    """SQLSpec-backed implementation of BaseArtifactService.

    Composes SQL metadata storage with ``sqlspec/storage/`` content backends
    to provide versioned artifact management for Google ADK.

    Metadata (version number, filename, MIME type, custom metadata, canonical
    URI) is stored in a SQL table managed by the artifact store.  Content
    bytes are stored in object storage (S3, GCS, Azure, local filesystem)
    via the storage registry.

    Args:
        store: Artifact metadata store implementation.
        artifact_storage_uri: Base URI for content storage (e.g.,
            ``"s3://my-bucket/adk-artifacts/"``, ``"file:///var/data/artifacts/"``).
            Can also be a registered alias in the storage registry.
        registry: Storage registry to use.  Defaults to the global singleton.

    Example:
        from sqlspec.adapters.asyncpg.adk.artifact_store import AsyncpgADKArtifactStore
        from sqlspec.extensions.adk.artifact import SQLSpecArtifactService

        artifact_store = AsyncpgADKArtifactStore(config)
        await artifact_store.ensure_table()

        service = SQLSpecArtifactService(
            store=artifact_store,
            artifact_storage_uri="s3://my-bucket/adk-artifacts/",
        )

        version = await service.save_artifact(
            app_name="my_app",
            user_id="user123",
            filename="output.png",
            artifact=part,
        )
    """

    def __init__(
        self, store: "BaseAsyncADKArtifactStore", artifact_storage_uri: str, registry: "StorageRegistry | None" = None
    ) -> None:
        self._store = store
        self._artifact_storage_uri = artifact_storage_uri.rstrip("/")
        self._registry = registry or storage_registry

    @property
    def store(self) -> "BaseAsyncADKArtifactStore":
        """Return the artifact metadata store."""
        return self._store

    @property
    def artifact_storage_uri(self) -> str:
        """Return the base URI for content storage."""
        return self._artifact_storage_uri

    async def save_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        artifact: "types.Part | dict[str, Any]",
        session_id: "str | None" = None,
        custom_metadata: "dict[str, Any] | None" = None,
    ) -> int:
        """Save an artifact, returning the new version number.

        Writes content to object storage first, then inserts the metadata
        row.  If content write succeeds but metadata insert fails, the
        orphaned content blob is logged but not automatically cleaned up
        (eventual consistency is acceptable; orphan sweep can be added later).

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            artifact: ADK Part or dict to save.
            session_id: Session identifier (None for user-scoped).
            custom_metadata: Optional per-version metadata dict.

        Returns:
            The version number (0-based, incrementing).
        """
        from google.adk.artifacts.base_artifact_service import ensure_part

        # Normalize artifact to Part
        artifact_part: types.Part = ensure_part(artifact)

        # Determine the next version
        version = await self._store.get_next_version(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        )

        # Build the content path and canonical URI
        content_path = _build_content_path(
            app_name=app_name, user_id=user_id, filename=filename, version=version, session_id=session_id
        )
        canonical_uri = f"{self._artifact_storage_uri}/{content_path}"

        # Serialize content
        content_bytes = _serialize_artifact(artifact_part)

        # Extract MIME type
        mime_type = _extract_mime_type(artifact_part)

        # Write content first (fail-fast before metadata)
        backend = self._registry.get(self._artifact_storage_uri)
        if hasattr(backend, "write_bytes_async"):
            await backend.write_bytes_async(content_path, content_bytes)
        else:
            backend.write_bytes_sync(content_path, content_bytes)

        # Insert metadata row
        from datetime import datetime, timezone

        record = ArtifactRecord(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            filename=filename,
            version=version,
            mime_type=mime_type,
            canonical_uri=canonical_uri,
            custom_metadata=custom_metadata,
            created_at=datetime.now(tz=timezone.utc),
        )
        await self._store.insert_artifact(record)

        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.save",
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            version=version,
            session_id=session_id,
            mime_type=mime_type,
        )
        return version

    async def load_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: "str | None" = None,
        version: "int | None" = None,
    ) -> "types.Part | None":
        """Load an artifact by reading metadata then fetching content.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).
            version: Specific version, or None for latest.

        Returns:
            Deserialized Part, or None if not found.
        """
        record = await self._store.get_artifact(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id, version=version
        )
        if record is None:
            log_with_context(
                logger,
                logging.DEBUG,
                "adk.artifact.load",
                app_name=app_name,
                filename=filename,
                version=version,
                found=False,
            )
            return None

        # Derive content path from canonical URI
        content_path = record["canonical_uri"].removeprefix(self._artifact_storage_uri + "/")

        backend = self._registry.get(self._artifact_storage_uri)
        if hasattr(backend, "read_bytes_async"):
            content_bytes = await backend.read_bytes_async(content_path)
        else:
            content_bytes = backend.read_bytes_sync(content_path)

        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.load",
            app_name=app_name,
            filename=filename,
            version=record["version"],
            found=True,
        )
        return _deserialize_artifact(content_bytes)

    async def list_artifact_keys(self, *, app_name: str, user_id: str, session_id: "str | None" = None) -> "list[str]":
        """List distinct artifact filenames.

        When ``session_id`` is provided, returns both session-scoped and
        user-scoped filenames.  When None, returns only user-scoped filenames.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.

        Returns:
            List of artifact filenames.
        """
        keys = await self._store.list_artifact_keys(app_name=app_name, user_id=user_id, session_id=session_id)
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.list_keys",
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            count=len(keys),
        )
        return keys

    async def delete_artifact(
        self, *, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> None:
        """Delete an artifact and all its versions.

        Deletes metadata rows first (fail-fast), then removes content
        objects from storage (best-effort).

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).
        """
        deleted_records = await self._store.delete_artifact(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        )

        # Best-effort content cleanup
        backend = self._registry.get(self._artifact_storage_uri)
        for record in deleted_records:
            content_path = record["canonical_uri"].removeprefix(self._artifact_storage_uri + "/")
            try:
                if hasattr(backend, "delete_async"):
                    await backend.delete_async(content_path)
                else:
                    backend.delete_sync(content_path)
            except Exception:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "adk.artifact.delete.content_cleanup_failed",
                    canonical_uri=record["canonical_uri"],
                    version=record["version"],
                )

        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.delete",
            app_name=app_name,
            filename=filename,
            session_id=session_id,
            versions_deleted=len(deleted_records),
        )

    async def list_versions(
        self, *, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[int]":
        """List all version numbers for an artifact.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            Sorted list of version numbers.
        """
        records = await self._store.list_artifact_versions(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        )
        return [r["version"] for r in records]

    async def list_artifact_versions(
        self, *, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[ArtifactVersion]":
        """List all versions with full metadata for an artifact.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            List of ArtifactVersion objects ordered by version ascending.
        """
        records = await self._store.list_artifact_versions(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        )
        return [_record_to_artifact_version(r) for r in records]

    async def get_artifact_version(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: "str | None" = None,
        version: "int | None" = None,
    ) -> "ArtifactVersion | None":
        """Get metadata for a specific artifact version.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).
            version: Version number, or None for latest.

        Returns:
            ArtifactVersion if found, None otherwise.
        """
        record = await self._store.get_artifact(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id, version=version
        )
        if record is None:
            return None
        return _record_to_artifact_version(record)
