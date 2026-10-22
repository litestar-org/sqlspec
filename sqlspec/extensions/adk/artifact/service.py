"""SQLSpec-backed artifact service for Google ADK.

Implements ``BaseArtifactService`` by composing SQL-backed metadata storage
(via :class:`BaseAsyncADKArtifactStore`) with ``sqlspec/storage/`` content
backends (via :class:`StorageRegistry`).

Metadata (version, filename, MIME type, custom metadata, canonical URI) lives
in a SQL table. Content bytes live in object storage addressed by canonical
URI. Versioning is append-only with monotonically increasing version numbers
starting from 0.
"""

import inspect
import logging
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from google.adk.artifacts import artifact_util
from google.adk.artifacts.base_artifact_service import ArtifactVersion, BaseArtifactService, ensure_part
from google.adk.errors.input_validation_error import InputValidationError
from google.genai import types

from sqlspec.extensions.adk.artifact._types import StoredArtifact
from sqlspec.storage.registry import StorageRegistry, storage_registry
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore

__all__ = ("SQLSpecArtifactService",)

logger = get_logger("sqlspec.extensions.adk.artifact.service")

_REWIND_TOMBSTONE = types.Part(inline_data=types.Blob(mime_type="application/octet-stream", data=b""))


def _file_has_user_namespace(filename: str) -> bool:
    """Return True when *filename* uses ADK's ``user:`` user-scoped prefix."""
    return filename.startswith("user:")


def _resolve_scoped_session_id(
    filename: str, session_id: "str | None", *, require_session_id: bool = False
) -> "str | None":
    """Resolve effective ``session_id`` honoring ``user:`` prefix and flat-storage validation."""
    if session_id is not None:
        artifact_util._validate_session_id_for_flat_storage(session_id)
    if _file_has_user_namespace(filename):
        return None
    if require_session_id and session_id is None:
        msg = "Session ID must be provided for session-scoped artifacts."
        raise InputValidationError(msg)
    return session_id


class SQLSpecArtifactService(BaseArtifactService):
    """SQLSpec-backed implementation of BaseArtifactService.

    Composes SQL metadata storage with ``sqlspec/storage/`` content backends
    to provide versioned artifact management for Google ADK.

    Metadata (version number, filename, MIME type, custom metadata, canonical
    URI) is stored in a SQL table managed by the artifact store. Content
    bytes are stored in object storage (S3, GCS, Azure, local filesystem)
    via the storage registry.

    Args:
        store: Artifact metadata store implementation (async or sync).
        artifact_storage_uri: Base URI for content storage.
            Can also be a registered alias in the storage registry.
        registry: Storage registry to use. Defaults to the global singleton.
    """

    def __init__(
        self,
        store: "BaseAsyncADKArtifactStore | BaseSyncADKArtifactStore",
        artifact_storage_uri: str,
        registry: "StorageRegistry | None" = None,
    ) -> None:
        self._store = store
        self._artifact_storage_uri = artifact_storage_uri.rstrip("/")
        self._registry = registry or storage_registry

    @property
    def store(self) -> "BaseAsyncADKArtifactStore | BaseSyncADKArtifactStore":
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
        row. If content write succeeds but metadata insert fails, the
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
        effective_session_id = _resolve_scoped_session_id(filename, session_id, require_session_id=True)
        artifact_part: types.Part = ensure_part(artifact)
        if artifact_util.is_artifact_ref(artifact_part):
            file_uri = cast("str", artifact_part.file_data.file_uri) if artifact_part.file_data is not None else ""
            parsed_uri = artifact_util.parse_artifact_uri(file_uri)
            if not parsed_uri:
                msg = f"Invalid artifact reference URI: {file_uri}"
                raise InputValidationError(msg)
            artifact_util.validate_artifact_reference_scope(
                app_name=app_name, user_id=user_id, session_id=effective_session_id, parsed_uri=parsed_uri
            )
            mime_type: str | None = None
        else:
            mime_type = _extract_mime_type(artifact_part)

        version = await self._call_store(
            "get_next_version", app_name=app_name, user_id=user_id, filename=filename, session_id=effective_session_id
        )
        content_path = _build_content_path(
            app_name=app_name, user_id=user_id, filename=filename, version=version, session_id=effective_session_id
        )
        canonical_uri = f"{self._artifact_storage_uri}/{content_path}"
        content_bytes = _serialize_artifact(artifact_part)
        backend = self._registry.get(self._artifact_storage_uri)
        await _call_storage_backend(backend, "write_bytes_async", "write_bytes_sync", content_path, content_bytes)
        record = StoredArtifact(
            app_name=app_name,
            user_id=user_id,
            session_id=effective_session_id,
            filename=filename,
            version=version,
            mime_type=mime_type,
            canonical_uri=canonical_uri,
            custom_metadata=custom_metadata,
            created_at=datetime.now(tz=UTC),
        )
        await self._call_store("insert_artifact", record)

        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.save",
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            version=version,
            session_id=effective_session_id,
            mime_type=mime_type,
        )
        return int(version)

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
        return await self._load_artifact(
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            session_id=session_id,
            version=version,
            remaining_depth=artifact_util._MAX_ARTIFACT_REFERENCE_DEPTH,
        )

    async def list_artifact_keys(self, *, app_name: str, user_id: str, session_id: "str | None" = None) -> "list[str]":
        """List distinct artifact filenames.

        When ``session_id`` is provided, returns both session-scoped and
        user-scoped filenames. When None, returns only user-scoped filenames.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier.

        Returns:
            List of artifact filenames.
        """
        if session_id is not None:
            artifact_util._validate_session_id_for_flat_storage(session_id)
        keys: list[str] = list(
            await self._call_store("list_artifact_keys", app_name=app_name, user_id=user_id, session_id=session_id)
        )
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
        effective_session_id = _resolve_scoped_session_id(filename, session_id)
        deleted_records = await self._call_store(
            "delete_artifact", app_name=app_name, user_id=user_id, filename=filename, session_id=effective_session_id
        )
        await self._delete_content(deleted_records)

        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.delete",
            app_name=app_name,
            filename=filename,
            session_id=effective_session_id,
            versions_deleted=len(deleted_records),
        )

    async def delete_artifacts_older_than(self, before: "datetime", app_name: "str | None" = None) -> int:
        """Delete artifact versions created before a cutoff.

        Metadata rows are deleted first (fail-fast). Content objects for the
        deleted versions are then removed on a best-effort basis: a failed
        content delete is logged with its canonical URI and version, and the
        remaining versions are still processed. Because the metadata row is
        already gone, a later run of this method cannot rediscover that
        content, so the warning records are the input to an independent
        object-store orphan sweep.

        Args:
            before: Timezone-aware UTC cutoff; versions created strictly before it are deleted.
            app_name: Application name to limit the deletion, or None for every application.

        Returns:
            Number of artifact version rows deleted from metadata storage.
        """
        deleted_records = await self._call_store("delete_artifacts_older_than", before, app_name=app_name)
        await self._delete_content(deleted_records)

        log_with_context(
            logger,
            logging.INFO,
            "adk.artifact.retention.prune",
            app_name=app_name,
            before=before.isoformat(),
            versions_deleted=len(deleted_records),
        )
        return len(deleted_records)

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
        effective_session_id = _resolve_scoped_session_id(filename, session_id)
        records = await self._call_store(
            "list_artifact_versions",
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            session_id=effective_session_id,
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
        effective_session_id = _resolve_scoped_session_id(filename, session_id)
        records = await self._call_store(
            "list_artifact_versions",
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            session_id=effective_session_id,
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
        effective_session_id = _resolve_scoped_session_id(filename, session_id)
        record = await self._call_store(
            "get_artifact",
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            session_id=effective_session_id,
            version=version,
        )
        if record is None:
            return None
        return _record_to_artifact_version(record)

    async def _load_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: "str | None",
        version: "int | None",
        remaining_depth: int,
    ) -> "types.Part | None":
        """Load an artifact, following at most *remaining_depth* ``artifact://`` references."""
        effective_session_id = _resolve_scoped_session_id(filename, session_id)
        record = await self._call_store(
            "get_artifact",
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            session_id=effective_session_id,
            version=version,
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

        content_path = record["canonical_uri"].removeprefix(self._artifact_storage_uri + "/")
        backend = self._registry.get(self._artifact_storage_uri)
        content_bytes = await _call_storage_backend(backend, "read_bytes_async", "read_bytes_sync", content_path)

        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.load",
            app_name=app_name,
            filename=filename,
            version=record["version"],
            found=True,
        )
        artifact_part = _deserialize_artifact(content_bytes)
        if artifact_util.is_artifact_ref(artifact_part):
            file_uri = cast("str", artifact_part.file_data.file_uri) if artifact_part.file_data is not None else ""
            parsed_uri = artifact_util.resolve_artifact_reference(
                file_uri=file_uri,
                app_name=app_name,
                user_id=user_id,
                session_id=session_id,
                remaining_depth=remaining_depth,
            )
            return await self._load_artifact(
                app_name=parsed_uri.app_name,
                user_id=parsed_uri.user_id,
                filename=parsed_uri.filename,
                session_id=parsed_uri.session_id,
                version=parsed_uri.version,
                remaining_depth=remaining_depth - 1,
            )

        if artifact_part in (types.Part(), _REWIND_TOMBSTONE):
            return None
        return artifact_part

    async def _call_store(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Invoke an async or sync artifact store method, bridging sync stores via async_()."""
        method = getattr(self._store, method_name)
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        return await async_(method)(*args, **kwargs)

    async def _delete_content(self, records: "list[StoredArtifact]") -> None:
        """Remove stored content for deleted metadata records on a best-effort basis.

        Each failure is logged with the canonical URI and version of the record
        it belongs to, and the remaining records are still processed. No storage
        backend is resolved when there is nothing to clean up.

        Args:
            records: Artifact metadata records whose content should be removed.
        """
        if not records:
            return

        backend = self._registry.get(self._artifact_storage_uri)
        for record in records:
            content_path = record["canonical_uri"].removeprefix(self._artifact_storage_uri + "/")
            try:
                await _call_storage_backend(backend, "delete_async", "delete_sync", content_path)
            except Exception:
                log_with_context(
                    logger,
                    logging.WARNING,
                    "adk.artifact.delete.content_cleanup_failed",
                    canonical_uri=record["canonical_uri"],
                    version=record["version"],
                )


_UNSAFE_PATH_CHARS = re.compile(r"(?:^|/)\.\.(?:/|$)|[/\\\x00]")


def _sanitize_path_component(value: str) -> str:
    """Sanitize a path component to prevent directory traversal and namespace injection.

    Removes leading/trailing slashes and rejects empty values, ``..`` traversals,
    internal slashes/backslashes, and NUL bytes.

    Args:
        value: Raw path component.

    Returns:
        Sanitized path component.

    Raises:
        ValueError: If the value is empty or contains unsafe path sequences.
    """
    value = value.strip("/")
    if not value or _UNSAFE_PATH_CHARS.search(value):
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
        inline = artifact.get("inline_data") or artifact.get("inlineData")
        if isinstance(inline, dict):
            return inline.get("mime_type") or inline.get("mimeType")
        file_data = artifact.get("file_data") or artifact.get("fileData")
        if isinstance(file_data, dict):
            return file_data.get("mime_type") or file_data.get("mimeType")
        return None

    if hasattr(artifact, "inline_data") and artifact.inline_data is not None:
        return getattr(artifact.inline_data, "mime_type", None)
    if hasattr(artifact, "file_data") and artifact.file_data is not None:
        return getattr(artifact.file_data, "mime_type", None)
    return None


def _serialize_artifact(artifact: "types.Part | dict[str, Any]") -> bytes:
    """Serialize an artifact Part to bytes for content storage.

    The artifact is serialized as JSON via ``model_dump(exclude_none=True, mode='json')``.
    This preserves the full Part structure including binary ``inline_data.data``,
    text, file_data, and any future Part fields.

    Args:
        artifact: ADK Part or dict representation.

    Returns:
        JSON-encoded bytes.
    """
    if isinstance(artifact, dict):
        return to_json(artifact, as_bytes=True)

    if hasattr(artifact, "model_dump"):
        data = artifact.model_dump(exclude_none=True, mode="json")
        return to_json(data, as_bytes=True)

    return to_json({"text": str(artifact)}, as_bytes=True)


def _deserialize_artifact(data: bytes) -> "types.Part":
    """Deserialize bytes back into an ADK Part.

    Args:
        data: JSON-encoded bytes from content storage.

    Returns:
        Reconstructed Part object.
    """
    parsed = from_json(data)
    return types.Part.model_validate(parsed)


def _record_to_artifact_version(record: "StoredArtifact") -> "ArtifactVersion":
    """Convert a database artifact record to an ADK ArtifactVersion.

    Args:
        record: Database artifact record.

    Returns:
        ArtifactVersion model instance.
    """
    return ArtifactVersion(
        version=record["version"],
        canonical_uri=record["canonical_uri"],
        custom_metadata=record["custom_metadata"] or {},
        create_time=record["created_at"].timestamp(),
        mime_type=record["mime_type"],
    )


async def _call_storage_backend(
    backend: Any, async_method_name: str, sync_method_name: str, *args: Any, **kwargs: Any
) -> Any:
    """Call the available async or sync storage-backend capability."""
    async_method = getattr(backend, async_method_name, None)
    if async_method is not None:
        return await async_method(*args, **kwargs)
    return await async_(getattr(backend, sync_method_name))(*args, **kwargs)
