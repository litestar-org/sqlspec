"""SQLSpec-backed session service for Google ADK."""

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from google.adk.sessions.base_session_service import BaseSessionService, GetSessionConfig, ListSessionsResponse

from sqlspec.extensions.adk.converters import (
    compute_update_marker,
    event_to_record,
    filter_temp_state,
    record_to_session,
)
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from google.adk.events.event import Event
    from google.adk.sessions import Session

    from sqlspec.extensions.adk.store import BaseAsyncADKStore

logger = get_logger("sqlspec.extensions.adk.service")

__all__ = ("SQLSpecSessionService",)


class SQLSpecSessionService(BaseSessionService):
    """SQLSpec-backed implementation of BaseSessionService.

    Provides session and event storage using SQLSpec database adapters.
    Delegates all database operations to a store implementation.

    Args:
        store: Database store implementation (e.g., AsyncpgADKStore).

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.adk.store import AsyncpgADKStore
        from sqlspec.extensions.adk.service import SQLSpecSessionService

        config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
        store = AsyncpgADKStore(config)
        await store.ensure_tables()

        service = SQLSpecSessionService(store)
        session = await service.create_session(
            app_name="my_app",
            user_id="user123",
            state={"key": "value"}
        )
    """

    def __init__(self, store: "BaseAsyncADKStore") -> None:
        """Initialize the session service.

        Args:
            store: Database store implementation.
        """
        self._store = store

    @property
    def store(self) -> "BaseAsyncADKStore":
        """Return the database store."""
        return self._store

    async def create_session(
        self, *, app_name: str, user_id: str, state: "dict[str, Any] | None" = None, session_id: "str | None" = None
    ) -> "Session":
        """Create a new session.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            state: Initial state of the session.
            session_id: Client-provided session ID. If None, generates a UUID.

        Returns:
            The newly created session.
        """
        if session_id is None:
            session_id = str(uuid.uuid4())

        if state is None:
            state = {}

        persisted_state = filter_temp_state(state)

        record = await self._store.create_session(
            session_id=session_id, app_name=app_name, user_id=user_id, state=persisted_state
        )
        log_with_context(
            logger, logging.DEBUG, "adk.session.create", app_name=app_name, session_id=session_id, has_state=bool(state)
        )

        return record_to_session(record, events=[])

    async def get_session(
        self, *, app_name: str, user_id: str, session_id: str, config: "GetSessionConfig | None" = None
    ) -> "Session | None":
        """Get a session by ID.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.
            config: Configuration for retrieving events.

        Returns:
            Session object if found, None otherwise.
        """
        record = await self._store.get_session(session_id)

        if not record:
            log_with_context(
                logger, logging.DEBUG, "adk.session.get", app_name=app_name, session_id=session_id, found=False
            )
            return None

        if record["app_name"] != app_name or record["user_id"] != user_id:
            log_with_context(
                logger, logging.DEBUG, "adk.session.get", app_name=app_name, session_id=session_id, found=False
            )
            return None

        after_timestamp = None
        limit = None

        if config:
            if config.after_timestamp:
                after_timestamp = datetime.fromtimestamp(config.after_timestamp, tz=timezone.utc)
            limit = config.num_recent_events

        events = await self._store.get_events(session_id=session_id, after_timestamp=after_timestamp, limit=limit)
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.session.get",
            app_name=app_name,
            session_id=session_id,
            found=True,
            event_count=len(events),
        )

        return record_to_session(record, events)

    async def list_sessions(self, *, app_name: str, user_id: str | None = None) -> "ListSessionsResponse":
        """List all sessions for an app, optionally filtered by user.

        Args:
            app_name: Name of the application.
            user_id: ID of the user. If None, all sessions for the app are listed.

        Returns:
            Response containing list of sessions (without events).
        """
        records = await self._store.list_sessions(app_name=app_name, user_id=user_id)

        sessions = [record_to_session(record, events=[]) for record in records]
        log_with_context(
            logger,
            logging.DEBUG,
            "adk.session.list",
            app_name=app_name,
            has_user_id=user_id is not None,
            count=len(sessions),
        )

        return ListSessionsResponse(sessions=sessions)

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        """Delete a session and all its events.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.
            session_id: Session identifier.
        """
        record = await self._store.get_session(session_id)

        if not record:
            log_with_context(
                logger, logging.DEBUG, "adk.session.delete", app_name=app_name, session_id=session_id, deleted=False
            )
            return

        if record["app_name"] != app_name or record["user_id"] != user_id:
            log_with_context(
                logger, logging.DEBUG, "adk.session.delete", app_name=app_name, session_id=session_id, deleted=False
            )
            return

        await self._store.delete_session(session_id)
        log_with_context(
            logger, logging.DEBUG, "adk.session.delete", app_name=app_name, session_id=session_id, deleted=True
        )

    async def append_event(self, session: "Session", event: "Event") -> "Event":
        """Append an event to a session.

        Persists the event record and the post-append durable state
        atomically via ``store.append_event_and_update_state()``, then
        updates the in-memory session only after persistence succeeds.

        Implements stale-session detection: if the session has been
        modified in storage since it was last loaded, a ``ValueError``
        is raised instead of silently overwriting.

        ``temp:`` keys are stripped from the persisted state snapshot so
        they never survive a reload.

        Args:
            session: Session to append to.
            event: Event to append.

        Returns:
            The appended event.

        Raises:
            ValueError: If the session has been modified in storage since
                it was loaded (stale session).
        """
        if event.partial:
            return event

        # Apply temp state to in-memory session so subsequent agents in
        # the same invocation can read temp values, then strip temp keys
        # from the event delta before persistence.
        self._apply_temp_state(session, event)
        event = self._trim_temp_delta_state(event)

        event_record = event_to_record(event=event, session_id=session.id)

        # Build durable state: current state minus temp keys, plus the
        # event's state delta (temp keys already stripped by _trim above).
        durable_state = filter_temp_state(session.state)
        if event.actions and event.actions.state_delta:
            durable_state.update(event.actions.state_delta)

        # --- Stale-session detection ---
        current_record = await self._store.get_session(session.id)
        if current_record is None:
            msg = f"Session {session.id} not found."
            raise ValueError(msg)

        if session._storage_update_marker is not None:  # pyright: ignore[reportPrivateUsage]
            current_marker = compute_update_marker(current_record["update_time"])
            if session._storage_update_marker != current_marker:  # pyright: ignore[reportPrivateUsage]
                msg = (
                    "The session has been modified in storage since it was loaded. "
                    "Please reload the session before appending more events."
                )
                raise ValueError(msg)
        elif current_record["update_time"].timestamp() > session.last_update_time:
            msg = (
                "The session has been modified in storage since it was loaded. "
                "Please reload the session before appending more events."
            )
            raise ValueError(msg)

        # --- Persist event and state atomically ---
        await self._store.append_event_and_update_state(
            event_record=event_record, session_id=session.id, state=durable_state
        )

        # Fetch updated session to refresh marker and timestamp
        updated_record = await self._store.get_session(session.id)
        if updated_record:
            session.last_update_time = updated_record["update_time"].timestamp()
            session._storage_update_marker = compute_update_marker(updated_record["update_time"])  # pyright: ignore[reportPrivateUsage]

        # Update in-memory session AFTER successful persistence
        self._update_session_state(session, event)
        session.events.append(event)

        log_with_context(
            logger, logging.DEBUG, "adk.session.event.append", app_name=session.app_name, session_id=session.id
        )

        return event
