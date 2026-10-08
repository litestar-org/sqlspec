"""Spanner ADK artifact store (sync and async)."""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar, Protocol, cast

from sqlspec.adapters.spanner._typing import SpannerNotFound as NotFound
from sqlspec.adapters.spanner.adk.store import (
    SPANNER_PARAM_TYPES,
    _adk_config,
    _is_spanner_table_missing,
    _json_param_type,
    _run_read_async,
    _spanner_row_deletion_policy,
)
from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.core import (
    execute_ddl_async,
    execute_ddl_sync,
    list_existing_table_names_async,
    list_existing_table_names_sync,
)
from sqlspec.extensions.adk.artifact._types import StoredArtifact
from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from datetime import datetime

    from sqlspec.adapters.spanner._typing import (
        SpannerAsyncDatabase,
        SpannerAsyncTransaction,
        SpannerDatabase,
        SpannerTransaction,
    )

__all__ = ("USER_SCOPED_SESSION_ID", "SpannerAsyncADKArtifactStore", "SpannerSyncADKArtifactStore")

USER_SCOPED_SESSION_ID = ""
_DDL_TIMEOUT_SECONDS = 300


class _SpannerADKArtifactStoreMixin:
    """Shared SQL and conversion helpers for Spanner ADK artifact stores."""

    __slots__ = ()

    if TYPE_CHECKING:
        _artifact_table: str
        _artifact_row_deletion_policy: str
        _artifact_table_options: "str | None"

    def _build_artifact_table_ddl(self) -> "list[str]":
        options = f" OPTIONS ({self._artifact_table_options})" if self._artifact_table_options else ""
        ddl = f"""CREATE TABLE IF NOT EXISTS {self._artifact_table} (
    app_name STRING(128) NOT NULL,
    user_id STRING(128) NOT NULL,
    session_id STRING(128) NOT NULL,
    filename STRING(512) NOT NULL,
    version INT64 NOT NULL,
    mime_type STRING(256),
    canonical_uri STRING(2048) NOT NULL,
    custom_metadata JSON,
    created_at TIMESTAMP NOT NULL
) PRIMARY KEY (app_name, user_id, session_id, filename, version){self._artifact_row_deletion_policy}{options}"""
        return [ddl]

    def _build_drop_artifact_table_sql(self) -> "list[str]":
        return [f"DROP TABLE {self._artifact_table}"]

    def _build_insert_artifact_statement(self, record: StoredArtifact) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        normalized_session_id = record["session_id"] if record["session_id"] is not None else USER_SCOPED_SESSION_ID
        metadata = record.get("custom_metadata")
        sql = f"""INSERT INTO {self._artifact_table} (
    app_name,
    user_id,
    session_id,
    filename,
    version,
    mime_type,
    canonical_uri,
    custom_metadata,
    created_at
) VALUES (
    @app_name,
    @user_id,
    @session_id,
    @filename,
    @version,
    @mime_type,
    @canonical_uri,
    @custom_metadata,
    @created_at
)"""
        params: dict[str, Any] = {
            "app_name": record["app_name"],
            "user_id": record["user_id"],
            "session_id": normalized_session_id,
            "filename": record["filename"],
            "version": int(record["version"]),
            "mime_type": record["mime_type"],
            "canonical_uri": record["canonical_uri"],
            "custom_metadata": to_json(metadata) if metadata is not None else None,
            "created_at": record["created_at"],
        }
        types: dict[str, Any] = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "filename": SPANNER_PARAM_TYPES.STRING,
            "version": SPANNER_PARAM_TYPES.INT64,
            "mime_type": SPANNER_PARAM_TYPES.STRING,
            "canonical_uri": SPANNER_PARAM_TYPES.STRING,
            "custom_metadata": _json_param_type(),
            "created_at": SPANNER_PARAM_TYPES.TIMESTAMP,
        }
        return (sql, params, types)

    def _build_get_artifact_query(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        normalized_session_id = session_id if session_id is not None else USER_SCOPED_SESSION_ID
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": normalized_session_id,
            "filename": filename,
        }
        types: dict[str, Any] = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "filename": SPANNER_PARAM_TYPES.STRING,
        }
        if version is not None:
            params["version"] = int(version)
            types["version"] = SPANNER_PARAM_TYPES.INT64
            sql = f"""SELECT
    app_name,
    user_id,
    session_id,
    filename,
    version,
    mime_type,
    canonical_uri,
    custom_metadata,
    created_at
FROM {self._artifact_table}
WHERE app_name = @app_name
  AND user_id = @user_id
  AND session_id = @session_id
  AND filename = @filename
  AND version = @version
LIMIT 1"""
            return (sql, params, types)

        sql = f"""SELECT
    app_name,
    user_id,
    session_id,
    filename,
    version,
    mime_type,
    canonical_uri,
    custom_metadata,
    created_at
FROM {self._artifact_table}
WHERE app_name = @app_name
  AND user_id = @user_id
  AND session_id = @session_id
  AND filename = @filename
ORDER BY version DESC LIMIT 1"""
        return (sql, params, types)

    def _build_list_artifact_versions_query(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        normalized_session_id = session_id if session_id is not None else USER_SCOPED_SESSION_ID
        sql = f"""SELECT
    app_name,
    user_id,
    session_id,
    filename,
    version,
    mime_type,
    canonical_uri,
    custom_metadata,
    created_at
FROM {self._artifact_table}
WHERE app_name = @app_name
  AND user_id = @user_id
  AND session_id = @session_id
  AND filename = @filename
ORDER BY version ASC"""
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": normalized_session_id,
            "filename": filename,
        }
        types: dict[str, Any] = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "filename": SPANNER_PARAM_TYPES.STRING,
        }
        return (sql, params, types)

    def _build_list_artifact_keys_query(
        self, app_name: str, user_id: str, session_id: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        params: dict[str, Any] = {"app_name": app_name, "user_id": user_id}
        types: dict[str, Any] = {"app_name": SPANNER_PARAM_TYPES.STRING, "user_id": SPANNER_PARAM_TYPES.STRING}
        if session_id is not None:
            params["session_id"] = session_id
            types["session_id"] = SPANNER_PARAM_TYPES.STRING
            sql = f"""SELECT DISTINCT filename
FROM {self._artifact_table}
WHERE app_name = @app_name
  AND user_id = @user_id
  AND session_id IN ('', @session_id)
ORDER BY filename ASC"""
            return (sql, params, types)

        sql = f"""SELECT DISTINCT filename
FROM {self._artifact_table}
WHERE app_name = @app_name
  AND user_id = @user_id
  AND session_id = ''
ORDER BY filename ASC"""
        return (sql, params, types)

    def _build_delete_artifact_statements(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "tuple[str, str, dict[str, Any], dict[str, Any]]":
        select_sql, params, types = self._build_list_artifact_versions_query(app_name, user_id, filename, session_id)
        delete_sql = (
            f"DELETE FROM {self._artifact_table} "
            "WHERE app_name = @app_name AND user_id = @user_id AND session_id = @session_id AND filename = @filename"
        )
        return (select_sql, delete_sql, params, types)

    def _build_get_next_version_query(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "tuple[str, dict[str, Any], dict[str, Any]]":
        normalized_session_id = session_id if session_id is not None else USER_SCOPED_SESSION_ID
        sql = f"""SELECT COALESCE(MAX(version) + 1, 0)
FROM {self._artifact_table}
WHERE app_name = @app_name
  AND user_id = @user_id
  AND session_id = @session_id
  AND filename = @filename"""
        params: dict[str, Any] = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": normalized_session_id,
            "filename": filename,
        }
        types: dict[str, Any] = {
            "app_name": SPANNER_PARAM_TYPES.STRING,
            "user_id": SPANNER_PARAM_TYPES.STRING,
            "session_id": SPANNER_PARAM_TYPES.STRING,
            "filename": SPANNER_PARAM_TYPES.STRING,
        }
        return (sql, params, types)

    def _build_delete_artifacts_older_than_statements(
        self, before: "datetime", app_name: "str | None" = None
    ) -> "tuple[str, str, dict[str, Any], dict[str, Any]]":
        clauses = ["created_at < @before"]
        params: dict[str, Any] = {"before": before}
        types: dict[str, Any] = {"before": SPANNER_PARAM_TYPES.TIMESTAMP}
        if app_name is not None:
            clauses.append("app_name = @app_name")
            params["app_name"] = app_name
            types["app_name"] = SPANNER_PARAM_TYPES.STRING
        where_sql = " AND ".join(clauses)
        select_sql = f"""SELECT
    app_name,
    user_id,
    session_id,
    filename,
    version,
    mime_type,
    canonical_uri,
    custom_metadata,
    created_at
FROM {self._artifact_table}
WHERE {where_sql}
ORDER BY app_name ASC, user_id ASC, session_id ASC, filename ASC, version ASC"""
        delete_sql = f"DELETE FROM {self._artifact_table} WHERE {where_sql}"
        return (select_sql, delete_sql, params, types)

    def _decode_metadata(self, value: Any) -> "dict[str, Any] | None":
        if value is None:
            return None
        if isinstance(value, dict):
            return cast("dict[str, Any]", value)
        raw = getattr(value, "serialize", None)
        if callable(raw):
            value = raw()
        if isinstance(value, str):
            return cast("dict[str, Any]", from_json(value)) if value else None
        return cast("dict[str, Any]", from_json(str(value)))

    def _rows_to_artifacts(self, rows: "list[Any]") -> "list[StoredArtifact]":
        return [
            StoredArtifact(
                app_name=str(row[0]),
                user_id=str(row[1]),
                session_id=None if row[2] == USER_SCOPED_SESSION_ID else str(row[2]),
                filename=str(row[3]),
                version=int(row[4]),
                mime_type=str(row[5]) if row[5] is not None else None,
                canonical_uri=str(row[6]),
                custom_metadata=self._decode_metadata(row[7]),
                created_at=row[8],
            )
            for row in rows
        ]


class SpannerSyncADKArtifactStore(BaseSyncADKArtifactStore[SpannerSyncConfig], _SpannerADKArtifactStoreMixin):
    """Spanner ADK artifact store backed by synchronous Spanner client."""

    __slots__ = ("_artifact_row_deletion_policy", "_artifact_table_options")

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerSyncConfig) -> None:
        super().__init__(config)
        self._artifact_table_options, self._artifact_row_deletion_policy = _extract_spanner_artifact_options(config)

    def create_table(self) -> None:
        """Create the artifact versions table if it does not exist."""
        if self._artifact_table in self._existing_tables():
            return
        execute_ddl_sync(self._database(), self._artifact_table_ddl(), timeout=_DDL_TIMEOUT_SECONDS)

    def drop_table(self) -> None:
        """Drop the artifact versions table if it exists."""
        if self._artifact_table not in self._existing_tables():
            return
        execute_ddl_sync(self._database(), self._build_drop_artifact_table_sql(), timeout=_DDL_TIMEOUT_SECONDS)

    def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert an artifact version metadata row."""
        sql, params, types = self._build_insert_artifact_statement(record)
        cast("Any", self._database()).run_in_transaction(_SpannerSyncArtifactInsertJob(sql, params, types))

    def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "StoredArtifact | None":
        """Get a specific artifact version's metadata."""
        sql, params, types = self._build_get_artifact_query(app_name, user_id, filename, session_id, version)
        try:
            rows = self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return None
            raise
        if not rows:
            return None
        return self._rows_to_artifacts(rows)[0]

    def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[StoredArtifact]":
        """List all version records for an artifact, ordered by version ascending."""
        sql, params, types = self._build_list_artifact_versions_query(app_name, user_id, filename, session_id)
        try:
            rows = self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return []
            raise
        return self._rows_to_artifacts(rows)

    def list_artifact_keys(self, app_name: str, user_id: str, session_id: "str | None" = None) -> "list[str]":
        """List distinct artifact filenames."""
        sql, params, types = self._build_list_artifact_keys_query(app_name, user_id, session_id)
        try:
            rows = self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return []
            raise
        return [str(row[0]) for row in rows]

    def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[StoredArtifact]":
        """Delete all version records for an artifact and return them."""
        select_sql, delete_sql, params, types = self._build_delete_artifact_statements(
            app_name, user_id, filename, session_id
        )
        job = _SpannerSyncArtifactDeleteJob(select_sql, delete_sql, params, types)
        rows = cast("list[Any]", cast("Any", self._database()).run_in_transaction(job))
        return self._rows_to_artifacts(rows)

    def get_next_version(self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None) -> int:
        """Get the next version number for an artifact."""
        sql, params, types = self._build_get_next_version_query(app_name, user_id, filename, session_id)
        try:
            rows = self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return 0
            raise
        if not rows or rows[0][0] is None:
            return 0
        return int(rows[0][0])

    def delete_artifacts_older_than(self, before: "datetime", app_name: "str | None" = None) -> "list[StoredArtifact]":
        """Delete artifact version rows created before a cutoff and return them."""
        select_sql, delete_sql, params, types = self._build_delete_artifacts_older_than_statements(before, app_name)
        job = _SpannerSyncArtifactDeleteJob(select_sql, delete_sql, params, types)
        rows = cast("list[Any]", cast("Any", self._database()).run_in_transaction(job))
        return self._rows_to_artifacts(rows)

    def _database(self) -> "SpannerDatabase":
        return self._config.get_database()

    def _existing_tables(self) -> "set[str]":
        return list_existing_table_names_sync(self._database())

    def _artifact_table_ddl(self) -> "list[str]":
        return self._build_artifact_table_ddl()

    def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        with self._config.provide_connection() as snapshot:
            reader = cast("_SpannerArtifactReadProtocol", snapshot)
            return list(reader.execute_sql(sql, params=params, param_types=types))


class SpannerAsyncADKArtifactStore(BaseAsyncADKArtifactStore[SpannerAsyncConfig], _SpannerADKArtifactStoreMixin):
    """Spanner ADK artifact store backed by asynchronous Spanner client."""

    __slots__ = ("_artifact_row_deletion_policy", "_artifact_table_options")

    connector_name: ClassVar[str] = "spanner"

    def __init__(self, config: SpannerAsyncConfig) -> None:
        super().__init__(config)
        self._artifact_table_options, self._artifact_row_deletion_policy = _extract_spanner_artifact_options(config)

    async def create_table(self) -> None:
        """Create the artifact versions table if it does not exist."""
        if self._artifact_table in await self._existing_tables():
            return
        await execute_ddl_async(await self._database(), self._artifact_table_ddl(), timeout=_DDL_TIMEOUT_SECONDS)

    async def drop_table(self) -> None:
        """Drop the artifact versions table if it exists."""
        if self._artifact_table not in await self._existing_tables():
            return
        await execute_ddl_async(
            await self._database(), self._build_drop_artifact_table_sql(), timeout=_DDL_TIMEOUT_SECONDS
        )

    async def insert_artifact(self, record: StoredArtifact) -> None:
        """Insert an artifact version metadata row."""
        sql, params, types = self._build_insert_artifact_statement(record)
        database = await self._database()
        await database.run_in_transaction(_SpannerAsyncArtifactInsertJob(sql, params, types))

    async def get_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None, version: "int | None" = None
    ) -> "StoredArtifact | None":
        """Get a specific artifact version's metadata."""
        sql, params, types = self._build_get_artifact_query(app_name, user_id, filename, session_id, version)
        try:
            rows = await self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return None
            raise
        if not rows:
            return None
        return self._rows_to_artifacts(rows)[0]

    async def list_artifact_versions(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[StoredArtifact]":
        """List all version records for an artifact, ordered by version ascending."""
        sql, params, types = self._build_list_artifact_versions_query(app_name, user_id, filename, session_id)
        try:
            rows = await self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return []
            raise
        return self._rows_to_artifacts(rows)

    async def list_artifact_keys(self, app_name: str, user_id: str, session_id: "str | None" = None) -> "list[str]":
        """List distinct artifact filenames."""
        sql, params, types = self._build_list_artifact_keys_query(app_name, user_id, session_id)
        try:
            rows = await self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return []
            raise
        return [str(row[0]) for row in rows]

    async def delete_artifact(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> "list[StoredArtifact]":
        """Delete all version records for an artifact and return them."""
        select_sql, delete_sql, params, types = self._build_delete_artifact_statements(
            app_name, user_id, filename, session_id
        )
        database = await self._database()
        job = _SpannerAsyncArtifactDeleteJob(select_sql, delete_sql, params, types)
        rows = cast("list[Any]", await database.run_in_transaction(job))
        return self._rows_to_artifacts(rows)

    async def get_next_version(
        self, app_name: str, user_id: str, filename: str, session_id: "str | None" = None
    ) -> int:
        """Get the next version number for an artifact."""
        sql, params, types = self._build_get_next_version_query(app_name, user_id, filename, session_id)
        try:
            rows = await self._run_read(sql, params, types)
        except NotFound as exc:
            if _is_spanner_table_missing(exc, self._artifact_table):
                return 0
            raise
        if not rows or rows[0][0] is None:
            return 0
        return int(rows[0][0])

    async def delete_artifacts_older_than(
        self, before: "datetime", app_name: "str | None" = None
    ) -> "list[StoredArtifact]":
        """Delete artifact version rows created before a cutoff and return them."""
        select_sql, delete_sql, params, types = self._build_delete_artifacts_older_than_statements(before, app_name)
        database = await self._database()
        job = _SpannerAsyncArtifactDeleteJob(select_sql, delete_sql, params, types)
        rows = cast("list[Any]", await database.run_in_transaction(job))
        return self._rows_to_artifacts(rows)

    async def _database(self) -> "SpannerAsyncDatabase":
        return await self._config.get_database()

    async def _existing_tables(self) -> "set[str]":
        return await list_existing_table_names_async(await self._database())

    def _artifact_table_ddl(self) -> "list[str]":
        return self._build_artifact_table_ddl()

    async def _run_read(
        self, sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        return await _run_read_async(await self._database(), sql, params, types)


def _extract_spanner_artifact_options(config: Any) -> "tuple[str | None, str]":
    """Return table options and row deletion policy for the Spanner ADK artifact table."""
    adk_config = _adk_config(config)
    raw_options = adk_config.get("artifact_table_options")
    table_options = str(raw_options) if isinstance(raw_options, str) else None
    return (
        table_options,
        _spanner_row_deletion_policy(adk_config, "artifact_ttl_seconds", "created_at"),
    )


class _SpannerArtifactReadProtocol(Protocol):
    def execute_sql(
        self, sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> Iterable[Any]: ...


class _SpannerSyncArtifactInsertJob:
    """Callable transaction work item for Spanner sync ADK artifact inserts."""

    __slots__ = ("_params", "_sql", "_types")

    def __init__(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._sql = sql
        self._params = params
        self._types = types

    def __call__(self, transaction: "SpannerTransaction") -> int:
        return int(cast("Any", transaction).execute_update(self._sql, params=self._params, param_types=self._types))


class _SpannerAsyncArtifactInsertJob:
    """Callable async transaction work item for Spanner async ADK artifact inserts."""

    __slots__ = ("_params", "_sql", "_types")

    def __init__(self, sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._sql = sql
        self._params = params
        self._types = types

    async def __call__(self, transaction: "SpannerAsyncTransaction") -> int:
        return int(await transaction.execute_update(self._sql, params=self._params, param_types=self._types))


class _SpannerSyncArtifactDeleteJob:
    """Callable transaction work item for Spanner sync ADK artifact delete-and-return operations."""

    __slots__ = ("_delete_sql", "_params", "_select_sql", "_types")

    def __init__(self, select_sql: str, delete_sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._select_sql = select_sql
        self._delete_sql = delete_sql
        self._params = params
        self._types = types

    def __call__(self, transaction: "SpannerTransaction") -> "list[Any]":
        rows = list(
            cast("_SpannerArtifactReadProtocol", transaction).execute_sql(
                self._select_sql, params=self._params, param_types=self._types
            )
        )
        if not rows:
            return []
        cast("Any", transaction).execute_update(self._delete_sql, params=self._params, param_types=self._types)
        return rows


class _SpannerAsyncArtifactDeleteJob:
    """Callable async transaction work item for Spanner async ADK artifact delete-and-return operations."""

    __slots__ = ("_delete_sql", "_params", "_select_sql", "_types")

    def __init__(self, select_sql: str, delete_sql: str, params: "dict[str, Any]", types: "dict[str, Any]") -> None:
        self._select_sql = select_sql
        self._delete_sql = delete_sql
        self._params = params
        self._types = types

    async def __call__(self, transaction: "SpannerAsyncTransaction") -> "list[Any]":
        result_set = await cast("Any", transaction).execute_sql(
            self._select_sql, params=self._params, param_types=self._types
        )
        rows = [row async for row in result_set]
        if not rows:
            return []
        await transaction.execute_update(self._delete_sql, params=self._params, param_types=self._types)
        return rows
