"""Session backend for Litestar integration with SQLSpec."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from litestar.middleware.session.server_side import ServerSideSessionBackend, ServerSideSessionConfig

from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from litestar.stores.base import Store


logger = get_logger("extensions.litestar.session")

__all__ = ("SQLSpecSessionBackend", "SQLSpecSessionConfig")


@dataclass
class SQLSpecSessionConfig(ServerSideSessionConfig):
    """SQLSpec-specific session configuration extending Litestar's ServerSideSessionConfig.

    This configuration class provides native Litestar session middleware support
    with SQLSpec as the backing store.
    """

    _backend_class: type[ServerSideSessionBackend] = field(default=None, init=False)  # type: ignore[assignment]

    # SQLSpec-specific configuration
    table_name: str = field(default="litestar_sessions")
    """Name of the session table in the database."""

    session_id_column: str = field(default="session_id")
    """Name of the session ID column."""

    data_column: str = field(default="data")
    """Name of the session data column."""

    expires_at_column: str = field(default="expires_at")
    """Name of the expires at column."""

    created_at_column: str = field(default="created_at")
    """Name of the created at column."""

    def __post_init__(self) -> None:
        """Post-initialization hook to set the backend class."""
        super().__post_init__()
        self._backend_class = SQLSpecSessionBackend


class SQLSpecSessionBackend(ServerSideSessionBackend):
    """SQLSpec-based session backend for Litestar.

    This backend extends Litestar's ServerSideSessionBackend to work seamlessly
    with SQLSpec stores registered in the Litestar app.
    """

    def __init__(self, config: SQLSpecSessionConfig) -> None:
        """Initialize the SQLSpec session backend.

        Args:
            config: SQLSpec session configuration
        """
        super().__init__(config=config)

    async def get(self, session_id: str, store: "Store") -> Optional[bytes]:
        """Retrieve data associated with a session ID.

        Args:
            session_id: The session ID
            store: Store to retrieve the session data from

        Returns:
            The session data bytes if existing, otherwise None.
        """
        # The SQLSpecSessionStore returns the deserialized data,
        # but ServerSideSessionBackend expects bytes
        max_age = int(self.config.max_age) if self.config.max_age is not None else None
        data = await store.get(session_id, renew_for=max_age if self.config.renew_on_access else None)

        if data is None:
            return None

        # The data from the store is already deserialized (dict/list/etc)
        # But Litestar's session middleware expects bytes
        # The store handles JSON serialization internally, so we return the raw bytes
        # However, SQLSpecSessionStore returns deserialized data, so we need to check the type
        if isinstance(data, bytes):
            return data

        # If it's not bytes, it means the store already deserialized it
        # We need to serialize it back to bytes for the middleware
        return to_json(data).encode("utf-8")

    async def set(self, session_id: str, data: bytes, store: "Store") -> None:
        """Store data under the session ID for later retrieval.

        Args:
            session_id: The session ID
            data: Serialized session data
            store: Store to save the session data in
        """
        expires_in = int(self.config.max_age) if self.config.max_age is not None else None
        # The data is already JSON bytes from Litestar
        # We need to deserialize it so the store can re-serialize it (store expects Python objects)
        await store.set(session_id, from_json(data.decode("utf-8")), expires_in=expires_in)

    async def delete(self, session_id: str, store: "Store") -> None:
        """Delete the data associated with a session ID.

        Args:
            session_id: The session ID
            store: Store to delete the session data from
        """
        await store.delete(session_id)
