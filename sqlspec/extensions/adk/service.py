"""SQLSpec-backed session service for Google ADK."""

import inspect
import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from google.adk.sessions.base_session_service import BaseSessionService, GetSessionConfig, ListSessionsResponse

from sqlspec.extensions.adk.converters import (
    compute_update_marker,
    event_to_record,
    filter_temp_state,
    merge_scoped_state,
    record_to_session,
    split_scoped_state,
)
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.adk.events.event import Event
    from google.adk.sessions import Session

    from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

    ADKStore = BaseAsyncADKStore | BaseSyncADKStore

__all__ = ("SQLSpecSessionService",)

logger = get_logger("sqlspec.extensions.adk.service")


class SQLSpecSessionService(BaseSessionService):
    """SQLSpec-backed implementation of BaseSessionService.

    Provides session and event storage using SQLSpec database adapters.
    Delegates all database operations to a store implementation.

    Args:
        store: Database store implementation.
    """

    def __init__(self, store: "ADKStore") -> None:
        """Initialize the session service.

        Args:
            store: Database store implementation.
        """
        self._store = store

    @property
    def store(self) -> "ADKStore":
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
        app_state_delta, user_state_delta, session_state = split_scoped_state(persisted_state)
        current_app_state = await self._call_store("get_app_state", app_name)
        current_user_state = await self._call_store("get_user_state", app_name, user_id)

        app_state = dict(current_app_state or {})
        if app_state_delta:
            app_state.update(app_state_delta)
        user_state = dict(current_user_state or {})
        if user_state_delta:
            user_state.update(user_state_delta)

        record = await self._call_store(
            "create_session", session_id=session_id, app_name=app_name, user_id=user_id, state=session_state
        )
        if app_state_delta:
            await self._call_store("upsert_app_state", app_name, app_state)
        if user_state_delta:
            await self._call_store("upsert_user_state", app_name, user_id, user_state)
        record["state"] = merge_scoped_state(record["state"], app_state, user_state)
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
        record = await self._call_store("get_session", app_name, user_id, session_id)

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

        app_state = await self._call_store("get_app_state", app_name)
        user_state = await self._call_store("get_user_state", app_name, user_id)
        record["state"] = merge_scoped_state(record["state"], app_state, user_state)

        after_timestamp = None
        limit = None

        if config:
            if config.after_timestamp:
                after_timestamp = datetime.fromtimestamp(config.after_timestamp, tz=timezone.utc)
            limit = config.num_recent_events

        if limit == 0:
            events = []
        else:
            events = await self._call_store(
                "get_events", app_name, user_id, session_id, after_timestamp=after_timestamp, limit=limit
            )
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

    async def get_user_state(self, *, app_name: str, user_id: str) -> "dict[str, Any]":
        """Get user-scoped state for an app and user.

        ADK's service API returns unprefixed user state keys, while SQLSpec
        stores the durable state using ADK's documented ``user:`` prefix.

        Args:
            app_name: Name of the application.
            user_id: ID of the user.

        Returns:
            User-scoped state with ``user:`` prefixes removed.
        """
        state = await self._call_store("get_user_state", app_name, user_id)
        if not state:
            return {}
        return {key.removeprefix("user:") if key.startswith("user:") else key: value for key, value in state.items()}

    async def list_sessions(self, *, app_name: str, user_id: str | None = None) -> "ListSessionsResponse":
        """List all sessions for an app, optionally filtered by user.

        Args:
            app_name: Name of the application.
            user_id: ID of the user. If None, all sessions for the app are listed.

        Returns:
            Response containing list of sessions (without events).
        """
        records = await self._call_store("list_sessions", app_name, user_id=user_id)

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
        record = await self._call_store("get_session", app_name, user_id, session_id)

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

        await self._call_store("delete_session", app_name, user_id, session_id)
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

        event_record = event_to_record(
            event=event, app_name=session.app_name, user_id=session.user_id, session_id=session.id
        )

        # --- Stale-session detection ---
        current_record = await self._call_store("get_session", session.app_name, session.user_id, session.id)
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

        state_delta = (event.actions.state_delta if event.actions else None) or {}
        app_state_delta, user_state_delta, session_state_delta = split_scoped_state(filter_temp_state(state_delta))

        _, _, session_state = split_scoped_state(filter_temp_state(session.state))
        session_state.update(session_state_delta)

        app_state = None
        if app_state_delta:
            app_state = dict(await self._call_store("get_app_state", session.app_name) or {})
            app_state.update(app_state_delta)

        user_state = None
        if user_state_delta:
            user_state = dict(await self._call_store("get_user_state", session.app_name, session.user_id) or {})
            user_state.update(user_state_delta)

        # --- Persist event and state atomically ---
        updated_record = await self._call_store(
            "append_event_and_update_state",
            event_record,
            session.app_name,
            session.user_id,
            session.id,
            session_state,
            app_state=app_state,
            user_state=user_state,
        )
        updated_record["state"] = merge_scoped_state(updated_record["state"], app_state, user_state)

        # Use the returned record directly — saves a round-trip vs a follow-up get_session().
        session.last_update_time = updated_record["update_time"].timestamp()
        session._storage_update_marker = compute_update_marker(updated_record["update_time"])  # pyright: ignore[reportPrivateUsage]

        # Update in-memory session AFTER successful persistence
        self._update_session_state(session, event)
        session.events.append(event)

        log_with_context(
            logger, logging.DEBUG, "adk.session.event.append", app_name=session.app_name, session_id=session.id
        )

        return event

    async def _call_store(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """Call an async store method or offload a sync store method."""
        method = getattr(self._store, method_name)
        if inspect.iscoroutinefunction(method):
            return await method(*args, **kwargs)
        sync_method = method
        if TYPE_CHECKING:
            sync_method = cast("Callable[..., Any]", method)
        return await async_(sync_method)(*args, **kwargs)
