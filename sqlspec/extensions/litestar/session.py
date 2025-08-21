"""Session backend for Litestar integration with SQLSpec."""

from typing import TYPE_CHECKING, Any, Optional, Union

from litestar.middleware.session.base import BaseSessionBackend

from sqlspec.extensions.litestar.store import SessionStore
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from litestar.connection import ASGIConnection

    from sqlspec.config import AsyncConfigT, DatabaseConfigProtocol, SyncConfigT

logger = get_logger("extensions.litestar.session")

__all__ = ("SQLSpecSessionBackend",)


class SQLSpecSessionBackend(BaseSessionBackend):
    """SQLSpec-based session backend for Litestar.

    This backend integrates the SQLSpec session store with Litestar's session
    middleware, providing transparent session management with database persistence.
    """

    __slots__ = ("_session_id_generator", "_session_lifetime", "_store")

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
        """
        self._store = SessionStore(
            config,
            table_name=table_name,
            session_id_column=session_id_column,
            data_column=data_column,
            expires_at_column=expires_at_column,
            created_at_column=created_at_column,
        )
        self._session_id_generator = SessionStore.generate_session_id
        self._session_lifetime = session_lifetime

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
        # Look for session ID in cookies
        session_cookie_name = getattr(connection.app.session_config, "session_cookie_name", "session")  # type: ignore[union-attr]
        return connection.cookies.get(session_cookie_name)

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
    def store(self) -> SessionStore:
        """Get the underlying session store.

        Returns:
            The session store instance
        """
        return self._store
