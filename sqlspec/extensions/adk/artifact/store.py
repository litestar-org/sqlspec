"""Base store classes for ADK artifact metadata backend (sync and async).

These abstract base classes define the database operations needed to manage
artifact version metadata. Content storage is handled separately by
``sqlspec/storage/`` backends; these stores only manage the relational
metadata rows.

Concrete subclasses implement the abstract methods with dialect-specific SQL.
"""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from sqlspec.extensions.adk._config_utils import _adk_artifact_store_config
from sqlspec.extensions.adk._table_utils import ensure_table_name
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.adk.artifact._types import ArtifactRecord

__all__ = ("BaseAsyncADKArtifactStore", "BaseSyncADKArtifactStore")

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.extensions.adk.artifact.store")


class BaseAsyncADKArtifactStore(ABC, Generic[ConfigT]):
    """Base class for async SQLSpec-backed ADK artifact metadata stores.

    Manages artifact version metadata in a SQL table. Content bytes are
    stored externally via ``sqlspec/storage/`` backends and referenced
    by canonical URI in each metadata row.

    Subclasses must implement dialect-specific SQL queries.

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.
    """

    __slots__ = ("_artifact_table", "_config")

    def __init__(self, config: ConfigT) -> None:
        """Initialize the async ADK artifact store.

        Args:
            config: SQLSpec database configuration.
        """
        self._config = config
        store_config = _adk_artifact_store_config(self._config)
        self._artifact_table: str = store_config["artifact_table"]
        ensure_table_name(self._artifact_table)

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def artifact_table(self) -> str:
        """Return the artifact versions table name."""
        return self._artifact_table

    @abstractmethod
    async def insert_artifact(self, record: "ArtifactRecord") -> None:
        """Insert an artifact version metadata row.

        Args:
            record: Artifact metadata record to insert.
        """

    @abstractmethod
    async def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "ArtifactRecord | None":
        """Get a specific artifact version's metadata.

        When ``version`` is None, returns the latest version.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).
            version: Specific version number, or None for latest.

        Returns:
            Artifact record if found, None otherwise.
        """

    @abstractmethod
    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: "str | None" = None) -> "list[str]":
        """List distinct artifact filenames.

        When ``session_id`` is provided, returns filenames from both
        session-scoped and user-scoped artifacts. When None, returns
        only user-scoped artifact filenames.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier (None for user-scoped only).

        Returns:
            List of distinct artifact filenames.
        """

    @abstractmethod
    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[ArtifactRecord]":
        """List all version records for an artifact, ordered by version ascending.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            List of artifact records ordered by version ascending.
        """

    @abstractmethod
    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[ArtifactRecord]":
        """Delete all version records for an artifact and return them.

        The caller uses the returned records to clean up content from
        object storage. Metadata is deleted first (fail-fast); content
        cleanup is best-effort.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            List of deleted artifact records (needed for content cleanup).
        """

    @abstractmethod
    async def get_next_version(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> int:
        """Get the next version number for an artifact.

        Returns 0 if no versions exist (first version), otherwise
        ``max(version) + 1``.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            Next version number (0-based).
        """

    @abstractmethod
    async def create_table(self) -> None:
        """Create the artifact versions table if it does not exist."""

    async def ensure_table(self) -> None:
        """Create the artifact table and emit a standardized log entry."""
        await self.create_table()
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.table.ready",
            db_system=resolve_db_system(type(self).__name__),
            artifact_table=self._artifact_table,
        )


class BaseSyncADKArtifactStore(ABC, Generic[ConfigT]):
    """Base class for sync SQLSpec-backed ADK artifact metadata stores.

    Synchronous counterpart of :class:`BaseAsyncADKArtifactStore`.

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.
    """

    __slots__ = ("_artifact_table", "_config")

    def __init__(self, config: ConfigT) -> None:
        """Initialize the sync ADK artifact store.

        Args:
            config: SQLSpec database configuration.
        """
        self._config = config
        store_config = _adk_artifact_store_config(self._config)
        self._artifact_table: str = store_config["artifact_table"]
        ensure_table_name(self._artifact_table)

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def artifact_table(self) -> str:
        """Return the artifact versions table name."""
        return self._artifact_table

    @abstractmethod
    def insert_artifact(self, record: "ArtifactRecord") -> None:
        """Insert an artifact version metadata row.

        Args:
            record: Artifact metadata record to insert.
        """

    @abstractmethod
    def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "ArtifactRecord | None":
        """Get a specific artifact version's metadata.

        When ``version`` is None, returns the latest version.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).
            version: Specific version number, or None for latest.

        Returns:
            Artifact record if found, None otherwise.
        """

    @abstractmethod
    def list_artifact_keys(self, app_name: str, user_id: str, session_id: "str | None" = None) -> "list[str]":
        """List distinct artifact filenames.

        Args:
            app_name: Application name.
            user_id: User identifier.
            session_id: Session identifier (None for user-scoped only).

        Returns:
            List of distinct artifact filenames.
        """

    @abstractmethod
    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[ArtifactRecord]":
        """List all version records for an artifact, ordered by version ascending.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            List of artifact records ordered by version ascending.
        """

    @abstractmethod
    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[ArtifactRecord]":
        """Delete all version records for an artifact and return them.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            List of deleted artifact records (needed for content cleanup).
        """

    @abstractmethod
    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None) -> int:
        """Get the next version number for an artifact.

        Args:
            app_name: Application name.
            user_id: User identifier.
            filename: Artifact filename.
            session_id: Session identifier (None for user-scoped).

        Returns:
            Next version number (0-based).
        """

    @abstractmethod
    def create_table(self) -> None:
        """Create the artifact versions table if it does not exist."""

    def ensure_table(self) -> None:
        """Create the artifact table and emit a standardized log entry."""
        self.create_table()
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.artifact.table.ready",
            db_system=resolve_db_system(type(self).__name__),
            artifact_table=self._artifact_table,
        )
