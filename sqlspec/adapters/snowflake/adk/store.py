"""Synchronous Snowflake persistence for Google ADK sessions and memory."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Final, Literal

from sqlspec.adapters.snowflake.config import SnowflakeConfig
from sqlspec.exceptions import NotFoundError, SerializationConflictError, SQLSpecError
from sqlspec.extensions.adk._types import SessionOrderBy, StoredEvent, StoredSession
from sqlspec.extensions.adk.converters import merge_scoped_state
from sqlspec.extensions.adk.memory._types import StoredMemory
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore
from sqlspec.extensions.adk.store import BaseSyncADKStore, normalize_session_list_options
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

__all__ = ("SnowflakeADKMemoryStore", "SnowflakeADKStore")

logger = get_logger("sqlspec.adapters.snowflake.adk")
_MAX_STATE_MERGE_ATTEMPTS: Final[int] = 8


class SnowflakeADKStore(BaseSyncADKStore[SnowflakeConfig]):
    """Store ADK sessions and events in Snowflake VARIANT columns."""

    __slots__ = ()
    _max_table_name_parts: ClassVar[int] = 3

    def create_tables(self) -> None:
        """Create and reconcile the five session persistence tables."""
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            for ddl in (
                self._sessions_table_ddl(),
                self._events_table_ddl(),
                self._app_states_table_ddl(),
                self._user_states_table_ddl(),
                self._metadata_table_ddl(),
            ):
                driver.execute(ddl)
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def create_session(
        self,
        session_id: str,
        app_name: str,
        user_id: str,
        state: dict[str, Any],
        owner_id: Any | None = None,
        *,
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Insert and return a session with its merged durable state."""
        now = datetime.now(UTC)
        session_state = dict(state)
        columns = ["id", "app_name", "user_id"]
        values = ["?", "?", "?"]
        parameters: list[Any] = [session_id, app_name, user_id]
        if self._owner_id_column_name:
            columns.append(self._owner_id_column_name)
            values.append("?")
            parameters.append(owner_id)
        columns.extend(("state", "create_time", "update_time"))
        values.extend(("PARSE_JSON(?)", "?", "?"))
        parameters.extend((to_json(session_state), now, now))
        with self._config.provide_session() as driver, driver.transaction():
            driver.execute(
                f"INSERT INTO {self._session_table} ({', '.join(columns)}) SELECT {', '.join(values)}", parameters
            )
            current_app_state = (
                self._apply_state_delta(driver, self._app_state_table, app_name, app_state_delta)
                if app_state_delta
                else self._select_state(self._app_state_table, app_name, driver=driver)
            )
            current_user_state = (
                self._apply_state_delta(driver, self._user_state_table, app_name, user_state_delta, user_id=user_id)
                if user_state_delta
                else self._select_state(self._user_state_table, app_name, user_id, driver=driver)
            )
        return StoredSession(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merge_scoped_state(session_state, current_app_state, current_user_state),
            create_time=now,
            update_time=now,
        )

    def get_session(
        self, app_name: str, user_id: str, session_id: str, *, renew_for: int | timedelta | None = None
    ) -> StoredSession | None:
        """Read a session and optionally refresh its update timestamp."""
        try:
            with self._provide_replica_session(primary=renew_for is not None) as driver:
                row = driver.select_one_or_none(
                    f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} "
                    "WHERE app_name = ? AND user_id = ? AND id = ?",
                    (app_name, user_id, session_id),
                )
                if row is None:
                    return None
                session = _session(row)
                if self._is_renewal_requested(renew_for):
                    now = datetime.now(UTC)
                    driver.execute(
                        f"UPDATE {self._session_table} SET update_time = ? WHERE app_name = ? AND user_id = ? AND id = ?",
                        (now, app_name, user_id, session_id),
                    )
                    driver.commit()
                    session["update_time"] = now
                return session
        except SQLSpecError as exc:
            if _missing_table(exc):
                return None
            raise

    def update_session_state(self, app_name: str, user_id: str, session_id: str, state: dict[str, Any]) -> None:
        """Replace a session's durable state."""
        with self._config.provide_session() as driver:
            driver.execute(
                f"UPDATE {self._session_table} SET state = PARSE_JSON(?), update_time = ? "
                "WHERE app_name = ? AND user_id = ? AND id = ?",
                (to_json(state), datetime.now(UTC), app_name, user_id, session_id),
            )
            driver.commit()

    def list_sessions(
        self,
        app_name: str,
        user_id: str | None = None,
        *,
        order_by: SessionOrderBy = "update_time",
        descending: bool = True,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[StoredSession]:
        """List sessions in deterministic timestamp and ID order."""
        order_column, direction, limit, offset = normalize_session_list_options(
            order_by=order_by, descending=descending, limit=limit, offset=offset
        )
        if limit == 0:
            return []
        sql = f"SELECT id, app_name, user_id, state, create_time, update_time FROM {self._session_table} WHERE app_name = ?"
        parameters: list[Any] = [app_name]
        if user_id is not None:
            sql += " AND user_id = ?"
            parameters.append(user_id)
        sql += f" ORDER BY {order_column} {direction}, id {direction}"
        if limit is not None:
            sql += " LIMIT ?"
            parameters.append(limit)
            if offset:
                sql += " OFFSET ?"
                parameters.append(offset)
        try:
            with self._provide_replica_session(primary=False) as driver:
                return [_session(row) for row in driver.select(sql, parameters)]
        except SQLSpecError as exc:
            if _missing_table(exc):
                return []
            raise

    def delete_session(self, app_name: str, user_id: str, session_id: str) -> None:
        """Delete child events and their scoped session in one transaction."""
        try:
            with self._config.provide_session() as driver, driver.transaction():
                driver.execute(
                    f"DELETE FROM {self._events_table} WHERE session_id = ? AND app_name = ? AND user_id = ?",
                    (session_id, app_name, user_id),
                )
                driver.execute(
                    f"DELETE FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?",
                    (app_name, user_id, session_id),
                )
        except SQLSpecError as exc:
            if not _missing_table(exc):
                raise

    def append_event(self, event_record: StoredEvent) -> None:
        """Append an ADK event to its session after verifying session ownership."""
        app_name = event_record["app_name"]
        user_id = event_record["user_id"]
        session_id = event_record["session_id"]
        with self._config.provide_session() as driver, driver.transaction():
            row = driver.select_one_or_none(
                f"SELECT 1 FROM {self._session_table} WHERE id = ? AND app_name = ? AND user_id = ? LIMIT 1",
                (session_id, app_name, user_id),
            )
            if row is None:
                msg = f"Session {session_id} not found for app={app_name!r}, user={user_id!r}"
                raise ValueError(msg)
            self._insert_event(driver, event_record)

    def append_event_and_update_state(
        self,
        event_record: StoredEvent,
        app_name: str,
        user_id: str,
        session_id: str,
        state: dict[str, Any],
        *,
        app_state_delta: dict[str, Any] | None = None,
        user_state_delta: dict[str, Any] | None = None,
    ) -> StoredSession:
        """Atomically append an event and update session, app, and user state."""
        _validate_event_belongs_to_session(event_record, app_name, user_id, session_id)
        now = datetime.now(UTC)
        session_state = dict(state)
        with self._config.provide_session() as driver, driver.transaction():
            row = driver.select_one_or_none(
                f"SELECT create_time FROM {self._session_table} WHERE app_name = ? AND user_id = ? AND id = ?",
                (app_name, user_id, session_id),
            )
            if row is None:
                msg = f"Session {session_id} not found during append_event_and_update_state."
                raise ValueError(msg)
            driver.execute(
                f"UPDATE {self._session_table} SET state = PARSE_JSON(?), update_time = ? "
                "WHERE app_name = ? AND user_id = ? AND id = ?",
                (to_json(session_state), now, app_name, user_id, session_id),
            )
            self._insert_event(driver, event_record)
            merged_app_state = (
                self._apply_state_delta(driver, self._app_state_table, app_name, app_state_delta)
                if app_state_delta
                else None
            )
            merged_user_state = (
                self._apply_state_delta(driver, self._user_state_table, app_name, user_state_delta, user_id=user_id)
                if user_state_delta
                else None
            )
            return StoredSession(
                id=session_id,
                app_name=app_name,
                user_id=user_id,
                state=merge_scoped_state(session_state, merged_app_state, merged_user_state),
                create_time=_normalize_utc(_lower_row(row)["create_time"]),
                update_time=now,
            )

    def get_events(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        after_timestamp: datetime | None = None,
        limit: int | None = None,
    ) -> list[StoredEvent]:
        """Read session events in ascending timestamp order."""
        if limit is not None and (isinstance(limit, bool) or limit < 0):
            msg = "limit must be non-negative"
            raise ValueError(msg)
        if limit == 0:
            return []
        sql = (
            "SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data "
            f"FROM {self._events_table} e "
            f"JOIN {self._session_table} s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
            "WHERE s.app_name = ? AND s.user_id = ? AND s.id = ?"
        )
        parameters: list[Any] = [app_name, user_id, session_id]
        if after_timestamp is not None:
            sql += " AND e.timestamp > ?"
            parameters.append(after_timestamp)
        if limit is not None:
            sql += " ORDER BY e.timestamp DESC, e.id DESC LIMIT ?"
            parameters.append(limit)
        else:
            sql += " ORDER BY e.timestamp ASC, e.id ASC"
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(sql, parameters)
            if limit is not None:
                rows = list(reversed(rows))
            events: list[StoredEvent] = []
            for raw in rows:
                row = _lower_row(raw)
                events.append(
                    StoredEvent(
                        id=str(row["id"]),
                        app_name=str(row["app_name"]),
                        user_id=str(row["user_id"]),
                        session_id=str(row["session_id"]),
                        invocation_id=str(row["invocation_id"] or ""),
                        timestamp=_normalize_utc(row["timestamp"]),
                        event_data=_decode_variant(row["event_data"]),
                    )
                )
        except SQLSpecError as exc:
            if _missing_table(exc):
                return []
            raise
        else:
            return events

    def delete_expired_events(self, before: datetime, app_name: str | None = None) -> int:
        """Delete events older than the cutoff within an optional application."""
        return self._delete_before(self._events_table, "timestamp", before, app_name)

    def delete_idle_user_states(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete user states untouched since the cutoff."""
        return self._delete_before(self._user_state_table, "update_time", updated_before, app_name)

    def delete_idle_sessions(self, updated_before: datetime, app_name: str | None = None) -> int:
        """Delete idle sessions after deleting their child events."""
        predicate = "update_time < ?" + (" AND app_name = ?" if app_name is not None else "")
        scoped_predicate = "s.update_time < ?" + (" AND s.app_name = ?" if app_name is not None else "")
        parameters: list[Any] = [updated_before] if app_name is None else [updated_before, app_name]
        try:
            with self._config.provide_session() as driver, driver.transaction():
                driver.execute(
                    f"DELETE FROM {self._events_table} WHERE EXISTS "
                    f"(SELECT 1 FROM {self._session_table} s WHERE s.id = {self._events_table}.session_id "
                    f"AND s.app_name = {self._events_table}.app_name "
                    f"AND s.user_id = {self._events_table}.user_id AND {scoped_predicate})",
                    parameters,
                )
                result = driver.execute(f"DELETE FROM {self._session_table} WHERE {predicate}", parameters)
                return max(result.get_affected_count(), 0)
        except SQLSpecError as exc:
            if _missing_table(exc):
                return 0
            raise

    def get_app_state(self, app_name: str) -> dict[str, Any] | None:
        """Read application-scoped state."""
        return self._select_state(self._app_state_table, app_name)

    def get_user_state(self, app_name: str, user_id: str) -> dict[str, Any] | None:
        """Read user-scoped state."""
        return self._select_state(self._user_state_table, app_name, user_id)

    def upsert_app_state(self, app_name: str, state: dict[str, Any]) -> None:
        """Insert or replace application-scoped state."""
        with self._config.provide_session() as driver:
            self._upsert_state(driver, self._app_state_table, app_name, state, datetime.now(UTC))
            driver.commit()

    def upsert_user_state(self, app_name: str, user_id: str, state: dict[str, Any]) -> None:
        """Insert or replace user-scoped state."""
        with self._config.provide_session() as driver:
            self._upsert_state(driver, self._user_state_table, app_name, state, datetime.now(UTC), user_id=user_id)
            driver.commit()

    def get_metadata(self, key: str) -> str | None:
        """Read internal persistence metadata by key."""
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(f"SELECT value FROM {self._metadata_table} WHERE key = ?", (key,))
                return None if row is None else str(_lower_row(row)["value"])
        except SQLSpecError as exc:
            if _missing_table(exc):
                return None
            raise

    def set_metadata(self, key: str, value: str) -> None:
        """Insert or replace internal persistence metadata."""
        with self._config.provide_session() as driver:
            driver.execute(
                f"MERGE INTO {self._metadata_table} t USING (SELECT ? AS key, ? AS value) s ON t.key = s.key "
                "WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)",
                (key, value),
            )
            driver.commit()

    def _insert_event(self, driver: Any, event_record: StoredEvent) -> None:
        driver.execute(
            f"INSERT INTO {self._events_table} "
            "(id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
            "SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)",
            (
                event_record["id"],
                event_record["app_name"],
                event_record["user_id"],
                event_record["session_id"],
                event_record["invocation_id"],
                event_record["timestamp"],
                to_json(event_record["event_data"]),
            ),
        )

    def _delete_before(self, table: str, column: str, before: datetime, app_name: str | None) -> int:
        sql = f"DELETE FROM {table} WHERE {column} < ?"
        parameters: list[Any] = [before]
        if app_name is not None:
            sql += " AND app_name = ?"
            parameters.append(app_name)
        try:
            with self._config.provide_session() as driver:
                result = driver.execute(sql, parameters)
                driver.commit()
                return max(result.get_affected_count(), 0)
        except SQLSpecError as exc:
            if _missing_table(exc):
                return 0
            raise

    def _select_state(
        self, table: str, app_name: str, user_id: str | None = None, *, driver: Any | None = None
    ) -> dict[str, Any] | None:
        sql = f"SELECT state FROM {table} WHERE app_name = ?"
        parameters: list[Any] = [app_name]
        if user_id is not None:
            sql += " AND user_id = ?"
            parameters.append(user_id)
        try:
            if driver is not None:
                row = driver.select_one_or_none(sql, parameters)
                return None if row is None else _decode_variant(_lower_row(row)["state"])
            with self._config.provide_session() as session_driver:
                row = session_driver.select_one_or_none(sql, parameters)
                return None if row is None else _decode_variant(_lower_row(row)["state"])
        except SQLSpecError as exc:
            if _missing_table(exc):
                return None
            raise

    def _apply_state_delta(
        self, driver: Any, table: str, app_name: str, delta: dict[str, Any], *, user_id: str | None = None
    ) -> dict[str, Any]:
        predicate = "app_name = ?"
        columns = "app_name"
        values = "?"
        key_parameters: list[Any] = [app_name]
        if user_id is not None:
            predicate += " AND user_id = ?"
            columns += ", user_id"
            values += ", ?"
            key_parameters.append(user_id)
        for _ in range(_MAX_STATE_MERGE_ATTEMPTS):
            row = driver.select_one_or_none(f"SELECT state, update_time FROM {table} WHERE {predicate}", key_parameters)
            if row is None:
                merged = dict(delta)
                now = datetime.now(UTC)
                result = driver.execute(
                    f"INSERT INTO {table} ({columns}, state, update_time) "
                    f"SELECT {values}, PARSE_JSON(?), ? WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {predicate})",
                    (*key_parameters, to_json(merged), now, *key_parameters),
                )
                if max(result.get_affected_count(), 0) > 0:
                    return merged
                continue
            lower = _lower_row(row)
            existing = _decode_variant(lower.get("state"))
            expected_update_time = lower.get("update_time")
            merged = {**existing, **delta}
            comparable_prev = _normalize_utc(expected_update_time) if expected_update_time is not None else None
            new_update_time = _next_monotonic_timestamp(comparable_prev)
            if expected_update_time is None:
                result = driver.execute(
                    f"UPDATE {table} SET state = PARSE_JSON(?), update_time = ? "
                    f"WHERE {predicate} AND update_time IS NULL",
                    (to_json(merged), new_update_time, *key_parameters),
                )
            else:
                result = driver.execute(
                    f"UPDATE {table} SET state = PARSE_JSON(?), update_time = ? WHERE {predicate} AND update_time = ?",
                    (to_json(merged), new_update_time, *key_parameters, expected_update_time),
                )
            if max(result.get_affected_count(), 0) > 0:
                return merged
        msg = f"Failed to merge state into {table} after {_MAX_STATE_MERGE_ATTEMPTS} compare-and-swap attempts."
        raise SerializationConflictError(msg)

    def _upsert_state(
        self,
        driver: Any,
        table: str,
        app_name: str,
        state: dict[str, Any],
        now: datetime,
        *,
        user_id: str | None = None,
    ) -> None:
        source = "? AS app_name"
        columns = "app_name"
        values = "s.app_name"
        predicate = "t.app_name = s.app_name"
        parameters: list[Any] = [app_name]
        if user_id is not None:
            source += ", ? AS user_id"
            columns += ", user_id"
            values += ", s.user_id"
            predicate += " AND t.user_id = s.user_id"
            parameters.append(user_id)
        parameters.extend((to_json(state), now))
        driver.execute(
            f"MERGE INTO {table} t USING (SELECT {source}, PARSE_JSON(?) AS state, ? AS update_time) s "
            f"ON {predicate} WHEN MATCHED THEN UPDATE SET state = s.state, update_time = s.update_time "
            f"WHEN NOT MATCHED THEN INSERT ({columns}, state, update_time) VALUES ({values}, s.state, s.update_time)",
            parameters,
        )

    def _sessions_table_ddl(self) -> str:
        owner = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._session_table} (id VARCHAR(128) PRIMARY KEY, "
            f"app_name VARCHAR(128) NOT NULL, user_id VARCHAR(128) NOT NULL{owner}, state VARIANT NOT NULL, "
            "create_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), update_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP())"
        )

    def _events_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._events_table} (id VARCHAR(128) PRIMARY KEY, app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, session_id VARCHAR(128) NOT NULL, invocation_id VARCHAR(256), "
            "timestamp TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), event_data VARIANT NOT NULL, "
            f"FOREIGN KEY (session_id) REFERENCES {self._session_table}(id))"
        )

    def _app_states_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._app_state_table} (app_name VARCHAR(128) PRIMARY KEY, "
            "state VARIANT NOT NULL, update_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP())"
        )

    def _user_states_table_ddl(self) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {self._user_state_table} (app_name VARCHAR(128) NOT NULL, "
            "user_id VARCHAR(128) NOT NULL, state VARIANT NOT NULL, "
            "update_time TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), PRIMARY KEY (app_name, user_id))"
        )

    def _metadata_table_ddl(self) -> str:
        return f"CREATE TABLE IF NOT EXISTS {self._metadata_table} (key VARCHAR(128) PRIMARY KEY, value VARCHAR(512) NOT NULL)"

    def _drop_app_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._app_state_table}"

    def _drop_user_states_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._user_state_table}"

    def _drop_metadata_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS {self._metadata_table}"

    def _drop_tables_sql(self) -> list[str]:
        return [
            self._drop_metadata_table_sql(),
            self._drop_user_states_table_sql(),
            self._drop_app_states_table_sql(),
            f"DROP TABLE IF EXISTS {self._events_table}",
            f"DROP TABLE IF EXISTS {self._session_table}",
        ]


class SnowflakeADKMemoryStore(BaseSyncADKMemoryStore[SnowflakeConfig]):
    """Store ADK memories with event deduplication and ILIKE text recall."""

    __slots__ = ()
    _max_table_name_parts: ClassVar[int] = 3

    def create_tables(self) -> None:
        """Create and reconcile the memory table when memory persistence is enabled."""
        if not self._enabled:
            return
        if not self.create_schema_enabled:
            self.reconcile_schema()
            return
        with self._config.provide_session() as driver:
            driver.execute(self._memory_table_ddl())
            driver.commit()
        self.reconcile_schema(assume_existing=True)

    def insert_memory_entries(self, entries: list[StoredMemory], owner_id: object | None = None) -> int:
        """Insert new event memories without replacing already persisted events."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if any(entry.get("embedding") is not None for entry in entries):
            msg = "SnowflakeADKMemoryStore does not support vector embeddings."
            raise NotImplementedError(msg)
        if not entries:
            return 0
        count = 0
        columns = ["id", "session_id", "app_name", "user_id", "scope", "event_id", "author"]
        if self._owner_id_column_name:
            columns.append(self._owner_id_column_name)
        columns.extend(("timestamp", "content_json", "content_text", "metadata_json", "inserted_at"))
        source = ", ".join(
            f"PARSE_JSON(?) AS {column}" if column in {"content_json", "metadata_json"} else f"? AS {column}"
            for column in columns
        )
        sql = (
            f"MERGE INTO {self._memory_table} t USING (SELECT {source}) s "
            "ON t.app_name = s.app_name AND t.user_id = s.user_id AND t.event_id = s.event_id "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join(f's.{column}' for column in columns)})"
        )
        with self._config.provide_session() as driver, driver.transaction():
            for entry in entries:
                parameters: list[Any] = []
                entry_values: dict[str, Any] = dict(entry)
                for column in columns:
                    value = owner_id if column == self._owner_id_column_name else entry_values[column]
                    parameters.append(
                        to_json(value) if column in {"content_json", "metadata_json"} and value is not None else value
                    )
                count += max(driver.execute(sql, parameters).get_affected_count(), 0)
        return count

    def search_entries(
        self,
        query: str,
        app_name: str,
        user_id: str,
        limit: int | None = None,
        scope_filter: Literal["all", "user", "app"] = "all",
        embedding: Sequence[float] | None = None,
    ) -> list[StoredMemory]:
        """Recall memories by case-insensitive substring and visibility scope."""
        if not self._enabled:
            msg = "Memory store is disabled"
            raise RuntimeError(msg)
        if embedding is not None:
            msg = "SnowflakeADKMemoryStore does not support vector embeddings."
            raise NotImplementedError(msg)
        if limit is not None and limit < 0:
            msg = "limit must be non-negative"
            raise ValueError(msg)
        if not query.strip() or limit == 0:
            return []
        if self._use_fts:
            logger.debug("Snowflake memory search uses ILIKE substring matching.")
        parameters: list[Any] = [app_name]
        predicate = "app_name = ?"
        if scope_filter == "user":
            predicate += " AND user_id = ? AND scope = 'user'"
            parameters.append(user_id)
        elif scope_filter == "app":
            predicate += " AND scope = 'app'"
        elif scope_filter == "all":
            predicate += " AND ((scope = 'user' AND user_id = ?) OR scope = 'app')"
            parameters.append(user_id)
        else:
            msg = "scope_filter must be 'all', 'user', or 'app'"
            raise ValueError(msg)
        parameters.extend((f"%{query.strip()}%", self._max_results if limit is None else limit))
        columns = "id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_json, content_text, metadata_json, inserted_at"
        try:
            with self._provide_replica_session(primary=False) as driver:
                rows = driver.select(
                    f"SELECT {columns} FROM {self._memory_table} WHERE {predicate} "
                    "AND content_text ILIKE ? ORDER BY timestamp DESC, id DESC LIMIT ?",
                    parameters,
                )
            memories: list[StoredMemory] = []
            for raw in rows:
                row = _lower_row(raw)
                memories.append(
                    StoredMemory(
                        id=str(row["id"]),
                        session_id=str(row["session_id"]),
                        app_name=str(row["app_name"]),
                        user_id=str(row["user_id"]),
                        scope=str(row["scope"]),
                        event_id=str(row["event_id"]),
                        author=row.get("author"),
                        timestamp=_normalize_utc(row["timestamp"]),
                        content_json=_decode_variant(row["content_json"]),
                        content_text=str(row["content_text"]),
                        metadata_json=None
                        if row.get("metadata_json") is None
                        else _decode_variant(row["metadata_json"]),
                        inserted_at=_normalize_utc(row["inserted_at"]),
                        embedding=None,
                    )
                )
        except SQLSpecError as exc:
            if _missing_table(exc):
                return []
            raise
        else:
            return memories

    def delete_entries_by_session(self, session_id: str, app_name: str, user_id: str) -> int:
        """Delete memories originating from the specified session, owned by an app and user."""
        predicate = "session_id = ?"
        parameters: list[Any] = [session_id]
        predicate += " AND app_name = ?"
        parameters.append(app_name)
        predicate += " AND user_id = ?"
        parameters.append(user_id)
        return self._delete(predicate, parameters)

    def delete_entries_older_than(self, days: int, app_name: str | None = None, scope: str | None = None) -> int:
        """Delete memories older than the cutoff within optional scope filters."""
        predicate = "inserted_at < ?"
        parameters: list[Any] = [datetime.now(UTC) - timedelta(days=days)]
        if app_name is not None:
            predicate += " AND app_name = ?"
            parameters.append(app_name)
        if scope is not None:
            predicate += " AND scope = ?"
            parameters.append(scope)
        return self._delete(predicate, parameters)

    def _delete(self, predicate: str, parameters: list[Any]) -> int:
        try:
            with self._config.provide_session() as driver:
                result = driver.execute(f"DELETE FROM {self._memory_table} WHERE {predicate}", parameters)
                driver.commit()
                return max(result.get_affected_count(), 0)
        except SQLSpecError as exc:
            if _missing_table(exc):
                return 0
            raise

    def _memory_table_ddl(self) -> str:
        owner = f", {self._owner_id_column_ddl}" if self._owner_id_column_ddl else ""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._memory_table} (id VARCHAR(128) PRIMARY KEY, "
            "session_id VARCHAR(128) NOT NULL, app_name VARCHAR(128) NOT NULL, user_id VARCHAR(128) NOT NULL, "
            f"scope VARCHAR(16) NOT NULL DEFAULT 'user', event_id VARCHAR(128) NOT NULL, author VARCHAR(256){owner}, "
            "timestamp TIMESTAMP_TZ NOT NULL, content_json VARIANT NOT NULL, content_text VARCHAR NOT NULL, "
            "metadata_json VARIANT, inserted_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
            "UNIQUE (app_name, user_id, event_id))"
        )

    def _drop_memory_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._memory_table}"]


def _decode_variant(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    value = from_json(raw)
    if not isinstance(value, dict):
        msg = "Expected a JSON object in a Snowflake VARIANT column."
        raise TypeError(msg)
    return value


def _normalize_utc(value: str | datetime) -> datetime:
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _missing_table(exc: SQLSpecError) -> bool:
    return isinstance(exc, NotFoundError) and "does not exist" in str(exc).lower()


def _lower_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key.lower(): value for key, value in row.items()}


def _session(row: dict[str, Any]) -> StoredSession:
    value = _lower_row(row)
    return StoredSession(
        id=str(value["id"]),
        app_name=str(value["app_name"]),
        user_id=str(value["user_id"]),
        state=_decode_variant(value["state"]),
        create_time=_normalize_utc(value["create_time"]),
        update_time=_normalize_utc(value["update_time"]),
    )


def _validate_event_belongs_to_session(event_record: StoredEvent, app_name: str, user_id: str, session_id: str) -> None:
    record_scope = (event_record["app_name"], event_record["user_id"], event_record["session_id"])
    expected_scope = (app_name, user_id, session_id)
    if record_scope != expected_scope:
        msg = f"event_record scope {record_scope!r} does not match {expected_scope!r}"
        raise ValueError(msg)


def _next_monotonic_timestamp(previous: datetime | None) -> datetime:
    now = datetime.now(UTC)
    if previous is None:
        return now
    comparable = previous if previous.tzinfo is not None else previous.replace(tzinfo=UTC)
    if now <= comparable:
        return comparable + timedelta(microseconds=1)
    return now
