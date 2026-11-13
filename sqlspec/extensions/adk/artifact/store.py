"""Base store classes for ADK artifact metadata backend (sync and async).

These abstract base classes define the database operations needed to manage
artifact version metadata. Content storage is handled separately by
``sqlspec/storage/`` backends; these stores only manage the relational
metadata rows.

Concrete subclasses implement the abstract methods with dialect-specific SQL.
"""

import logging
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from sqlspec.extensions.adk._config_utils import _adk_artifact_store_config
from sqlspec.extensions.adk._table_utils import ensure_table_name
from sqlspec.extensions.adk.store import _adk_read_provider, _adk_read_settings
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.config import DatabaseConfigProtocol
    from sqlspec.extensions.adk.artifact._types import StoredArtifact
    from sqlspec.migrations.schema import SchemaTarget

__all__ = ("BaseAsyncADKArtifactStore", "BaseSyncADKArtifactStore")

ConfigT = TypeVar("ConfigT", bound="DatabaseConfigProtocol[Any, Any, Any]")

logger = get_logger("sqlspec.extensions.adk.artifact.store")


class _ADKArtifactStoreCommon(Generic[ConfigT]):
    """Shared non-async ADK store state and helpers."""

    __slots__ = ("_artifact_table", "_config", "_read_bind_group", "_read_config", "_read_config_enabled")

    def __init__(self, config: ConfigT, *, read_config: ConfigT | None = None) -> None:
        """Initialize the async ADK artifact store.

        Args:
            config: SQLSpec database configuration.
            read_config: Optional compatible configuration for pure reads.
        """
        self._config = config
        self._read_config = read_config
        self._read_config_enabled, self._read_bind_group = _adk_read_settings(config, read_config)
        store_config = _adk_artifact_store_config(self._config)
        self._artifact_table: str = store_config["artifact_table"]
        ensure_table_name(self._artifact_table)

    @property
    def read_config(self) -> ConfigT | None:
        """Return the explicitly configured reader, when supplied."""
        return self._read_config

    @property
    def read_config_enabled(self) -> bool:
        """Return whether this store opts into pure-read offload."""
        return self._read_config_enabled

    @property
    def read_bind_group(self) -> str | None:
        """Return the configured named replica group for ADK reads."""
        return self._read_bind_group

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def artifact_table(self) -> str:
        """Return the artifact versions table name."""
        return self._artifact_table

    def _provide_read_connection(
        self, *, primary: bool = False, **kwargs: Any
    ) -> AbstractContextManager[Any] | AbstractAsyncContextManager[Any]:
        return _adk_read_provider(
            self._config, self._read_config, self._read_config_enabled, self._read_bind_group, primary, False, kwargs
        )

    def _provide_read_session(
        self, *, primary: bool = False, **kwargs: Any
    ) -> AbstractContextManager[Any] | AbstractAsyncContextManager[Any]:
        return _adk_read_provider(
            self._config, self._read_config, self._read_config_enabled, self._read_bind_group, primary, True, kwargs
        )

    def _schema_management_flags(self) -> tuple[bool, bool]:
        extension_config = getattr(self._config, "extension_config", {})
        adk_config = extension_config.get("adk", {}) if isinstance(extension_config, dict) else {}
        manage_schema = adk_config.get("manage_schema", True) if isinstance(adk_config, dict) else True
        create_schema = adk_config.get("create_schema", True) if isinstance(adk_config, dict) else True
        return bool(manage_schema), bool(create_schema)


class BaseAsyncADKArtifactStore(_ADKArtifactStoreCommon[ConfigT], ABC):
    """Base class for async SQLSpec-backed ADK artifact metadata stores.

    Manages artifact version metadata in a SQL table. Content bytes are
    stored externally via ``sqlspec/storage/`` backends and referenced
    by canonical URI in each metadata row.

    Subclasses must implement dialect-specific SQL queries.

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.
    """

    __slots__ = ()

    async def prepare_schema_async(self, driver: Any) -> None:
        """Prepare adapter-specific schema decisions with an asynchronous driver."""

    async def prepare_reconcile_async(self, driver: Any) -> None:
        """Run reconcile-only prerequisite DDL; the default does nothing."""

    async def get_schema_targets_async(self, driver: Any) -> "list[SchemaTarget]":
        """Return artifact table targets, which is empty by default."""
        del driver
        return []

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Reconcile adapter-specific artifact companion objects; the default does nothing."""

    async def finalize_reconcile_async(self, driver: Any) -> None:
        """Refresh config state after every DDL phase succeeds; the default does nothing."""

    @abstractmethod
    async def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert an artifact version metadata row.

        Args:
            record: Artifact metadata record to insert.
        """

    @abstractmethod
    async def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "StoredArtifact | None":
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
    ) -> "list[StoredArtifact]":
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
    ) -> "list[StoredArtifact]":
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

    async def delete_artifacts_older_than(
        self, before: "datetime", app_name: "str | None" = None
    ) -> "list[StoredArtifact]":
        """Delete artifact version rows created before a cutoff and return them.

        Implementations delete and return the affected version rows in a single
        atomic operation where the database supports it, ordered deterministically
        so cleanup logs stay reproducible. The caller uses the returned records to
        remove content from object storage.

        Args:
            before: Timezone-aware UTC cutoff; rows created strictly before it are deleted.
            app_name: Application name to limit the deletion, or None for every application.

        Returns:
            List of deleted artifact records (needed for content cleanup).

        Raises:
            NotImplementedError: If the store does not implement artifact retention.
        """
        msg = f"{type(self).__name__} does not support artifact retention pruning"
        raise NotImplementedError(msg)

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

    def _provide_read_connection(self, *, primary: bool = False, **kwargs: Any) -> AbstractAsyncContextManager[Any]:
        return cast("AbstractAsyncContextManager[Any]", super()._provide_read_connection(primary=primary, **kwargs))

    def _provide_read_session(self, *, primary: bool = False, **kwargs: Any) -> AbstractAsyncContextManager[Any]:
        return cast("AbstractAsyncContextManager[Any]", super()._provide_read_session(primary=primary, **kwargs))


class BaseSyncADKArtifactStore(_ADKArtifactStoreCommon[ConfigT], ABC):
    """Base class for sync SQLSpec-backed ADK artifact metadata stores.

    Synchronous counterpart of :class:`BaseAsyncADKArtifactStore`.

    Args:
        config: SQLSpec database configuration with extension_config["adk"] settings.
    """

    __slots__ = ()

    def prepare_schema_sync(self, driver: Any) -> None:
        """Prepare adapter-specific schema decisions with a synchronous driver."""

    def prepare_reconcile_sync(self, driver: Any) -> None:
        """Run reconcile-only prerequisite DDL; the default does nothing."""

    def get_schema_targets_sync(self, driver: Any) -> "list[SchemaTarget]":
        """Return artifact table targets, which is empty by default."""
        del driver
        return []

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Reconcile adapter-specific artifact companion objects; the default does nothing."""

    def finalize_reconcile_sync(self, driver: Any) -> None:
        """Refresh config state after every DDL phase succeeds; the default does nothing."""

    @abstractmethod
    def insert_artifact(self, record: "StoredArtifact") -> None:
        """Insert an artifact version metadata row.

        Args:
            record: Artifact metadata record to insert.
        """

    @abstractmethod
    def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "StoredArtifact | None":
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
    ) -> "list[StoredArtifact]":
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
    ) -> "list[StoredArtifact]":
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

    def delete_artifacts_older_than(self, before: "datetime", app_name: "str | None" = None) -> "list[StoredArtifact]":
        """Delete artifact version rows created before a cutoff and return them.

        Implementations delete and return the affected version rows in a single
        atomic operation where the database supports it, ordered deterministically
        so cleanup logs stay reproducible. The caller uses the returned records to
        remove content from object storage.

        Args:
            before: Timezone-aware UTC cutoff; rows created strictly before it are deleted.
            app_name: Application name to limit the deletion, or None for every application.

        Returns:
            List of deleted artifact records (needed for content cleanup).

        Raises:
            NotImplementedError: If the store does not implement artifact retention.
        """
        msg = f"{type(self).__name__} does not support artifact retention pruning"
        raise NotImplementedError(msg)

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

    def _provide_read_connection(self, *, primary: bool = False, **kwargs: Any) -> AbstractContextManager[Any]:
        return cast("AbstractContextManager[Any]", super()._provide_read_connection(primary=primary, **kwargs))

    def _provide_read_session(self, *, primary: bool = False, **kwargs: Any) -> AbstractContextManager[Any]:
        return cast("AbstractContextManager[Any]", super()._provide_read_session(primary=primary, **kwargs))
