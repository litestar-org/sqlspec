"""Session backend for Litestar integration with SQLSpec."""

from typing import TYPE_CHECKING, Any, Optional, Union

from litestar.middleware.session.base import BaseSessionBackend
from litestar.types import Scopes

from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from litestar.connection import ASGIConnection
    from litestar.types import Message, ScopeSession

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, SyncConfigT

logger = get_logger("extensions.litestar.session")

__all__ = ("SQLSpecSessionBackend", "SQLSpecSessionConfig")


class SQLSpecSessionConfig:
    """Configuration for SQLSpec session backend."""

    def __init__(
        self,
        key: str = "session",
        max_age: int = 1209600,  # 14 days
        path: str = "/",
        domain: Optional[str] = None,
        secure: bool = False,
        httponly: bool = True,
        samesite: str = "lax",
        exclude: Optional[Union[str, list[str]]] = None,
        exclude_opt_key: str = "skip_session",
        scopes: Scopes = frozenset({"http", "websocket"}),
    ) -> None:
        """Initialize session configuration.

        Args:
            key: Cookie key name
            max_age: Cookie max age in seconds
            path: Cookie path
            domain: Cookie domain
            secure: Require HTTPS for cookie
            httponly: Make cookie HTTP-only
            samesite: SameSite policy for cookie
            exclude: Patterns to exclude from session middleware
            exclude_opt_key: Key to opt out of session middleware
            scopes: Scopes where session middleware applies
        """
        self.key = key
        self.max_age = max_age
        self.path = path
        self.domain = domain
        self.secure = secure
        self.httponly = httponly
        self.samesite = samesite
        self.exclude = exclude
        self.exclude_opt_key = exclude_opt_key
        self.scopes = scopes


class SQLSpecSessionBackend(BaseSessionBackend):
    """SQLSpec-based session backend for Litestar.

    This backend integrates the SQLSpec session store with Litestar's session
    middleware, providing transparent session management with database persistence.
    """

    __slots__ = ("_session_id_generator", "_session_lifetime", "_store", "config")

    def __init__(
        self,
        config: Union["SyncConfigT", "AsyncConfigT", "DatabaseConfigProtocol"],
        *,
        table_name: str = "litestar_sessions",
        session_id_column: str = "session_id",
        data_column: str = "data",
        expires_at_column: str = "expires_at",
        created_at_column: str = "created_at",
        session_lifetime: int = 24 * 60 * 60,  # 24 hours
        session_config: Optional[SQLSpecSessionConfig] = None,
    ) -> None:
        """Initialize the session backend.

        Args:
            config: SQLSpec database configuration
            table_name: Name of the session table
            session_id_column: Name of the session ID column
            data_column: Name of the session data column
            expires_at_column: Name of the expires at column
            created_at_column: Name of the created at column
            session_lifetime: Default session lifetime in seconds
            session_config: Session configuration for middleware
        """
        self._store = SQLSpecSessionStore(
            config,
            table_name=table_name,
            session_id_column=session_id_column,
            data_column=data_column,
            expires_at_column=expires_at_column,
            created_at_column=created_at_column,
        )
        self._session_id_generator = SQLSpecSessionStore.generate_session_id
        self._session_lifetime = session_lifetime
        self.config = session_config or SQLSpecSessionConfig()

    async def load_from_connection(self, connection: "ASGIConnection[Any, Any, Any, Any]") -> dict[str, Any]:
        """Load session data from the connection.

        Args:
            connection: ASGI connection instance

        Returns:
            Session data dictionary
        """
        session_id = self.get_session_id(connection)
        if not session_id:
            return {}

        try:
            session_data = await self._store.get(session_id)
            return session_data if isinstance(session_data, dict) else {}
        except Exception:
            logger.exception("Failed to load session %s", session_id)
            return {}

    async def dump_to_connection(self, data: dict[str, Any], connection: "ASGIConnection[Any, Any, Any, Any]") -> str:
        """Store session data to the connection.

        Args:
            data: Session data to store
            connection: ASGI connection instance

        Returns:
            Session identifier
        """
        session_id = self.get_session_id(connection)
        if not session_id:
            session_id = self._session_id_generator()

        try:
            await self._store.set(session_id, data, expires_in=self._session_lifetime)

        except Exception:
            logger.exception("Failed to store session %s", session_id)
            raise
        return session_id

    def get_session_id(self, connection: "ASGIConnection[Any, Any, Any, Any]") -> Optional[str]:
        """Get session ID from the connection.

        Args:
            connection: ASGI connection instance

        Returns:
            Session identifier if found
        """
        # Try to get session ID from cookies using the config key
        session_id = connection.cookies.get(self.config.key)
        if session_id and session_id != "null":
            return session_id

        # Fallback to getting session ID from connection state
        session_id = connection.get_session_id()
        if session_id:
            return session_id

        return None

    async def store_in_message(
        self, scope_session: "ScopeSession", message: "Message", connection: "ASGIConnection[Any, Any, Any, Any]"
    ) -> None:
        """Store session information in the outgoing message.

        For server-side sessions, this method sets a cookie containing the session ID.
        If the session is empty, a null-cookie will be set to clear any existing session.

        Args:
            scope_session: Current session data to store
            message: Outgoing ASGI message to modify
            connection: ASGI connection instance
        """
        if message["type"] != "http.response.start":
            return

        cookie_key = self.config.key

        # If session is empty, set a null cookie to clear any existing session
        if not scope_session:
            cookie_value = self._build_cookie_value(
                key=cookie_key,
                value="null",
                max_age=0,
                path=self.config.path,
                domain=self.config.domain,
                secure=self.config.secure,
                httponly=self.config.httponly,
                samesite=self.config.samesite,
            )
            self._add_cookie_to_message(message, cookie_value)
            return

        # Get or generate session ID
        session_id = self.get_session_id(connection)
        if not session_id:
            session_id = self._session_id_generator()

        # Store session data in the backend
        try:
            await self._store.set(session_id, scope_session, expires_in=self._session_lifetime)
        except Exception:
            logger.exception("Failed to store session data for session %s", session_id)
            # Don't set the cookie if we failed to store the data
            return

        # Set the session ID cookie
        cookie_value = self._build_cookie_value(
            key=cookie_key,
            value=session_id,
            max_age=self.config.max_age,
            path=self.config.path,
            domain=self.config.domain,
            secure=self.config.secure,
            httponly=self.config.httponly,
            samesite=self.config.samesite,
        )
        self._add_cookie_to_message(message, cookie_value)

    def _build_cookie_value(
        self,
        key: str,
        value: str,
        max_age: Optional[int] = None,
        path: Optional[str] = None,
        domain: Optional[str] = None,
        secure: bool = False,
        httponly: bool = False,
        samesite: Optional[str] = None,
    ) -> str:
        """Build a cookie value string with attributes."""
        cookie_parts = [f"{key}={value}"]

        if path:
            cookie_parts.append(f"Path={path}")
        if domain:
            cookie_parts.append(f"Domain={domain}")
        if max_age is not None:
            cookie_parts.append(f"Max-Age={max_age}")
        if secure:
            cookie_parts.append("Secure")
        if httponly:
            cookie_parts.append("HttpOnly")
        if samesite:
            cookie_parts.append(f"SameSite={samesite}")

        return "; ".join(cookie_parts)

    def _add_cookie_to_message(self, message: "Message", cookie_value: str) -> None:
        """Add a Set-Cookie header to the ASGI message."""
        if message["type"] == "http.response.start":
            headers = list(message.get("headers", []))
            headers.append([b"set-cookie", cookie_value.encode()])
            message["headers"] = headers

    async def delete_session(self, session_id: str) -> None:
        """Delete a session.

        Args:
            session_id: Session identifier to delete
        """
        try:
            await self._store.delete(session_id)
        except Exception:
            logger.exception("Failed to delete session %s", session_id)
            raise

    async def delete_expired_sessions(self) -> None:
        """Delete all expired sessions.

        This method should be called periodically to clean up expired sessions.
        """
        try:
            await self._store.delete_expired()
        except Exception:
            logger.exception("Failed to delete expired sessions")

    async def get_all_session_ids(self) -> list[str]:
        """Get all active session IDs.

        Returns:
            List of all active session identifiers
        """
        session_ids = []
        try:
            async for session_id, _ in self._store.get_all():
                session_ids.append(session_id)
        except Exception:
            logger.exception("Failed to get all session IDs")

        return session_ids

    @property
    def store(self) -> SQLSpecSessionStore:
        """Get the underlying session store.

        Returns:
            The session store instance
        """
        return self._store
