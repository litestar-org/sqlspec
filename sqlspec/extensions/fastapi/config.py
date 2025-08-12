"""Configuration classes for SQLSpec FastAPI integration."""

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Literal, Optional, Union

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from fastapi import FastAPI

    from sqlspec.config import AsyncConfigT, DriverT, SyncConfigT
    from sqlspec.typing import ConnectionT, PoolT


CommitMode = Literal["manual", "autocommit", "autocommit_include_redirect"]
DEFAULT_COMMIT_MODE: CommitMode = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"

__all__ = (
    "DEFAULT_COMMIT_MODE",
    "DEFAULT_CONNECTION_KEY",
    "DEFAULT_POOL_KEY",
    "DEFAULT_SESSION_KEY",
    "CommitMode",
    "DatabaseConfig",
)


@dataclass
class DatabaseConfig:
    """Configuration for SQLSpec database integration with FastAPI applications."""

    config: "Union[SyncConfigT, AsyncConfigT]" = field()  # type: ignore[valid-type]   # pyright: ignore[reportGeneralTypeIssues]
    connection_key: str = field(default=DEFAULT_CONNECTION_KEY)
    pool_key: str = field(default=DEFAULT_POOL_KEY)
    session_key: str = field(default=DEFAULT_SESSION_KEY)
    commit_mode: "CommitMode" = field(default=DEFAULT_COMMIT_MODE)
    extra_commit_statuses: "Optional[set[int]]" = field(default=None)
    extra_rollback_statuses: "Optional[set[int]]" = field(default=None)
    enable_middleware: bool = field(default=True)

    # Generated providers and dependencies
    connection_provider: "Optional[Callable[[], AsyncGenerator[ConnectionT, None]]]" = field(
        init=False, repr=False, hash=False, default=None
    )
    pool_provider: "Optional[Callable[[], Awaitable[PoolT]]]" = field(init=False, repr=False, hash=False, default=None)
    session_provider: "Optional[Callable[[ConnectionT], AsyncGenerator[DriverT, None]]]" = field(
        init=False, repr=False, hash=False, default=None
    )

    def __post_init__(self) -> None:
        """Initialize providers after object creation."""
        if not self.config.supports_connection_pooling and self.pool_key == DEFAULT_POOL_KEY:  # type: ignore[union-attr,unused-ignore]
            self.pool_key = f"_{self.pool_key}_{id(self.config)}"

        # Validate commit mode
        if self.commit_mode not in {"manual", "autocommit", "autocommit_include_redirect"}:
            msg = f"Invalid commit mode: {self.commit_mode}"
            raise ImproperConfigurationError(detail=msg)

        # Validate status code sets
        if (
            self.extra_commit_statuses
            and self.extra_rollback_statuses
            and self.extra_commit_statuses & self.extra_rollback_statuses
        ):
            msg = "Extra rollback statuses and commit statuses must not share any status codes"
            raise ImproperConfigurationError(msg)

    def init_app(self, app: "FastAPI") -> None:
        """Initialize SQLSpec configuration for FastAPI application.

        Args:
            app: The FastAPI application instance.
        """
        from sqlspec.extensions.fastapi._middleware import SessionMiddleware
        from sqlspec.extensions.fastapi._providers import (
            create_connection_provider,
            create_pool_provider,
            create_session_provider,
        )

        # Create providers
        self.pool_provider = create_pool_provider(self.config, self.pool_key)
        self.connection_provider = create_connection_provider(self.config, self.pool_key, self.connection_key)
        self.session_provider = create_session_provider(self.config, self.connection_key)

        # Add middleware if enabled
        if self.enable_middleware:
            app.add_middleware(
                SessionMiddleware,
                config=self,
                commit_mode=self.commit_mode,
                extra_commit_statuses=self.extra_commit_statuses,
                extra_rollback_statuses=self.extra_rollback_statuses,
            )

        # Add event handlers
        app.add_event_handler("startup", self._startup_handler(app))
        app.add_event_handler("shutdown", self._shutdown_handler(app))

    def _startup_handler(self, app: "FastAPI") -> "Callable[[], Awaitable[None]]":
        """Create startup handler for database pool initialization.

        Args:
            app: The FastAPI application instance.

        Returns:
            Startup handler function.
        """

        async def startup() -> None:
            from sqlspec.utils.sync_tools import ensure_async_

            db_pool = await ensure_async_(self.config.create_pool)()
            app.state.__dict__[self.pool_key] = db_pool

        return startup

    def _shutdown_handler(self, app: "FastAPI") -> "Callable[[], Awaitable[None]]":
        """Create shutdown handler for database pool cleanup.

        Args:
            app: The FastAPI application instance.

        Returns:
            Shutdown handler function.
        """

        async def shutdown() -> None:
            from sqlspec.utils.sync_tools import ensure_async_

            app.state.__dict__.pop(self.pool_key, None)
            with contextlib.suppress(Exception):
                await ensure_async_(self.config.close_pool)()

        return shutdown
