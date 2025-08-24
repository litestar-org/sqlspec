"""Configuration classes for SQLSpec FastAPI integration."""

from typing import TYPE_CHECKING

from sqlspec.extensions.starlette.config import (
    DEFAULT_COMMIT_MODE,
    DEFAULT_CONNECTION_KEY,
    DEFAULT_POOL_KEY,
    DEFAULT_SESSION_KEY,
)
from sqlspec.extensions.starlette.config import AsyncDatabaseConfig as StarletteAsyncConfig
from sqlspec.extensions.starlette.config import DatabaseConfig as StarletteConfig
from sqlspec.extensions.starlette.config import SyncDatabaseConfig as StarletteSyncConfig

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = (
    "DEFAULT_COMMIT_MODE",
    "DEFAULT_CONNECTION_KEY",
    "DEFAULT_POOL_KEY",
    "DEFAULT_SESSION_KEY",
    "AsyncDatabaseConfig",
    "CommitMode",
    "DatabaseConfig",
    "SyncDatabaseConfig",
)

# Re-export Starlette types with FastAPI-compatible typing
from sqlspec.extensions.starlette.config import CommitMode


class DatabaseConfig(StarletteConfig):
    """Configuration for SQLSpec database integration with FastAPI applications.

    FastAPI is built on Starlette, so this configuration inherits all functionality
    from the Starlette configuration. The only differences are type hints for FastAPI
    Request objects and middleware imports.
    """

    def init_app(self, app: "FastAPI") -> None:  # pyright: ignore
        """Initialize SQLSpec configuration for FastAPI application.

        Args:
            app: The FastAPI application instance.
        """
        from sqlspec.extensions.fastapi._middleware import SessionMiddleware
        from sqlspec.extensions.starlette._providers import (
            create_connection_provider,
            create_pool_provider,
            create_session_provider,
        )

        # Create providers using Starlette providers (FastAPI is compatible)
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

        # Add event handlers - delegate to parent logic but cast FastAPI to Starlette
        super().init_app(app)  # type: ignore[arg-type]


# Add typed subclasses for better developer experience
class SyncDatabaseConfig(StarletteSyncConfig):
    """Sync-specific DatabaseConfig with FastAPI-compatible type hints."""

    def init_app(self, app: "FastAPI") -> None:  # pyright: ignore
        """Initialize SQLSpec configuration for FastAPI application.

        Args:
            app: The FastAPI application instance.
        """
        DatabaseConfig.init_app(self, app)  # pyright: ignore


class AsyncDatabaseConfig(StarletteAsyncConfig):
    """Async-specific DatabaseConfig with FastAPI-compatible type hints."""

    def init_app(self, app: "FastAPI") -> None:  # pyright: ignore
        """Initialize SQLSpec configuration for FastAPI application.

        Args:
            app: The FastAPI application instance.
        """
        DatabaseConfig.init_app(self, app)  # pyright: ignore
