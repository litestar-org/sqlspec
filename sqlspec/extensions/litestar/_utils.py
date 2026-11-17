"""ASGI scope state utilities for the Litestar SQLSpec plugin."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Final, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Generator

    from litestar.types import Scope

__all__ = (
    "StoreLease",
    "clear_request_session",
    "delete_sqlspec_scope_state",
    "get_sqlspec_scope_state",
    "publish_request_session",
    "request_session_scope",
    "resolve_store_lease",
    "set_sqlspec_scope_state",
)

_SCOPE_NAMESPACE = "_sqlspec"


def get_sqlspec_scope_state(scope: "Scope", key: str, default: Any = None, pop: bool = False) -> Any:
    """Get an internal value from connection scope state.

    Args:
        scope: The connection scope.
        key: Key to get from internal namespace in scope state.
        default: Default value to return.
        pop: Boolean flag dictating whether the value should be deleted from the state.

    Returns:
        Value mapped to ``key`` in internal connection scope namespace.
    """
    namespace = scope.setdefault(_SCOPE_NAMESPACE, {})  # type: ignore[misc]
    return namespace.pop(key, default) if pop else namespace.get(key, default)  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]


def set_sqlspec_scope_state(scope: "Scope", key: str, value: Any) -> None:
    """Set an internal value in connection scope state.

    Args:
        scope: The connection scope.
        key: Key to set under internal namespace in scope state.
        value: Value for key.
    """
    scope.setdefault(_SCOPE_NAMESPACE, {})[key] = value  # type: ignore[misc]


def delete_sqlspec_scope_state(scope: "Scope", key: str) -> None:
    """Remove an internal value from connection scope state.

    Args:
        scope: The connection scope.
        key: Key to remove under internal namespace in scope state.
    """
    scope.setdefault(_SCOPE_NAMESPACE, {}).pop(key, None)  # type: ignore[misc]


class StoreLease(NamedTuple):
    """How a session store operation reaches the database for the calling task.

    Attributes:
        session: Open request session to borrow, or ``None`` when the store opens its own session.
        manual: Whether the borrowed request session runs in ``manual`` commit mode.
        discard: Whether the store must roll back its own session because the request failed.
    """

    session: Any
    manual: bool
    discard: bool


_OWN_SESSION_LEASE: Final = StoreLease(None, False, False)
_DISCARD_LEASE: Final = StoreLease(None, False, True)


class _HeldSession:
    """Request session state for one database configuration."""

    __slots__ = ("closed", "config", "failed", "manual", "session")

    def __init__(self, config: Any, session: Any, manual: bool) -> None:
        self.config = config
        self.session = session
        self.manual = manual
        self.closed = False
        self.failed = False


class _RequestSessionHolder:
    """Mutable record of the database sessions one Litestar request opens.

    The plugin middleware installs one holder in the outer request context before
    dependencies resolve. Dependency providers fill it from any task or worker thread,
    and session stores borrow from it only in the owning request task.

    Args:
        owner: The asyncio task serving the request, or ``None`` outside a running loop.
    """

    __slots__ = ("_entries", "_owner")

    def __init__(self, owner: "asyncio.Task[Any] | None") -> None:
        self._owner = owner
        self._entries: list[_HeldSession] = []

    def fill(self, config: Any, session: Any, *, manual: bool = False) -> None:
        """Record the open request session for a database configuration.

        Args:
            config: Database configuration that owns the session.
            session: Open database session holding the request connection.
            manual: Whether the request session uses ``manual`` commit mode.
        """
        self._entries = [entry for entry in self._entries if entry.config is not config]
        self._entries.append(_HeldSession(config, session, manual))

    def release(self, session: Any, *, failed: bool = False) -> None:
        """Mark a recorded request session closed.

        Args:
            session: The recorded session object.
            failed: Whether the request ended with an error.
        """
        for entry in self._entries:
            if not entry.closed and entry.session is session:
                entry.closed = True
                entry.failed = failed

    def lease(self, config: Any, task: "asyncio.Task[Any] | None") -> StoreLease:
        """Return how a store operation in ``task`` reaches the database for ``config``.

        Args:
            config: Database configuration to match by identity.
            task: The task running the store operation.

        Returns:
            The open request session for the owning task, a discard lease for the owning
            task after a failed request, and an own-session lease otherwise.
        """
        if task is None or task is not self._owner:
            return _OWN_SESSION_LEASE
        for entry in self._entries:
            if entry.config is not config:
                continue
            if not entry.closed:
                return StoreLease(entry.session, entry.manual, False)
            return _DISCARD_LEASE if entry.failed else _OWN_SESSION_LEASE
        return _OWN_SESSION_LEASE


_REQUEST_HOLDER: "ContextVar[_RequestSessionHolder | None]" = ContextVar(
    "sqlspec_litestar_request_session_holder", default=None
)


def _current_task() -> "asyncio.Task[Any] | None":
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


@contextmanager
def request_session_scope() -> "Generator[_RequestSessionHolder, None, None]":
    """Install a request session holder owned by the current task for the enclosed request.

    Yields:
        The installed holder.
    """
    holder = _RequestSessionHolder(_current_task())
    token = _REQUEST_HOLDER.set(holder)
    try:
        yield holder
    finally:
        _REQUEST_HOLDER.reset(token)


def publish_request_session(config: Any, session: Any, *, manual: bool = False) -> None:
    """Fill the current request's holder with the open session for a database configuration.

    Args:
        config: Database configuration that owns the session.
        session: Open database session holding the request connection.
        manual: Whether the request session uses ``manual`` commit mode.
    """
    holder = _REQUEST_HOLDER.get()
    if holder is not None:
        holder.fill(config, session, manual=manual)


def clear_request_session(session: Any, *, failed: bool = False) -> None:
    """Mark a request session closed in the current request's holder.

    Args:
        session: The published session object.
        failed: Whether the request ended with an error. Later store operations in the
            owning request task then discard their writes instead of committing them.
    """
    holder = _REQUEST_HOLDER.get()
    if holder is not None:
        holder.release(session, failed=failed)


def resolve_store_lease(config: Any) -> StoreLease:
    """Return how a store operation in the current task reaches the database for ``config``.

    Args:
        config: Database configuration to match by identity.

    Returns:
        The lease for the current task, or an own-session lease outside a request.
    """
    holder = _REQUEST_HOLDER.get()
    if holder is None:
        return _OWN_SESSION_LEASE
    return holder.lease(config, _current_task())
