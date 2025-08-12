"""Configuration classes for SQLSpec Flask integration."""

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

    from flask import Flask

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
    """Configuration for SQLSpec database integration with Flask applications."""

    config: "Union[SyncConfigT, AsyncConfigT]" = field()  # type: ignore[valid-type]   # pyright: ignore[reportGeneralTypeIssues]
    connection_key: str = field(default=DEFAULT_CONNECTION_KEY)
    pool_key: str = field(default=DEFAULT_POOL_KEY)
    session_key: str = field(default=DEFAULT_SESSION_KEY)
    commit_mode: "CommitMode" = field(default=DEFAULT_COMMIT_MODE)
    extra_commit_statuses: "Optional[set[int]]" = field(default=None)
    extra_rollback_statuses: "Optional[set[int]]" = field(default=None)
    create_all: bool = field(default=False)

    # Generated providers and handlers
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

    def init_app(self, app: "Flask") -> None:
        """Initialize SQLSpec configuration for Flask application.

        Args:
            app: The Flask application instance.
        """
        from sqlspec.extensions.flask._providers import (
            create_connection_provider,
            create_pool_provider,
            create_session_provider,
        )

        # Create providers
        self.pool_provider = create_pool_provider(self.config, self.pool_key)
        self.connection_provider = create_connection_provider(self.config, self.pool_key, self.connection_key)
        self.session_provider = create_session_provider(self.config, self.connection_key)

        # Register teardown handlers
        app.teardown_appcontext(self._teardown_session)

        # Add before/after request handlers for transaction management
        if self.commit_mode != "manual":
            app.before_request(self._before_request)
            app.after_request(self._after_request_factory(app))

        # Store configuration reference in Flask app
        if not hasattr(app, "extensions"):
            app.extensions = {}
        if "sqlspec" not in app.extensions:
            app.extensions["sqlspec"] = {}
        app.extensions["sqlspec"][self.pool_key] = self

    def _before_request(self) -> None:
        """Set up database connection before request processing."""

        # Flask's g object is used to store request-scoped data
        # The connection will be created lazily when first accessed

    def _after_request_factory(self, app: "Flask") -> "Callable[[Any], Any]":
        """Create after request handler with app context.

        Args:
            app: The Flask application instance.

        Returns:
            After request handler function.
        """

        def after_request(response: Any) -> Any:
            """Handle transaction commit/rollback after request processing.

            Args:
                response: Flask response object.

            Returns:
                The response object.
            """
            from flask import g

            # Get connection from Flask's g object if it exists
            connection = getattr(g, self.connection_key, None)
            if connection is None:
                return response

            try:
                should_commit = self._should_commit_transaction(response.status_code)

                if should_commit and hasattr(connection, "commit") and callable(connection.commit):
                    connection.commit()
                elif hasattr(connection, "rollback") and callable(connection.rollback):
                    connection.rollback()
            except Exception:
                # Always try to rollback on exception
                if hasattr(connection, "rollback") and callable(connection.rollback):
                    with contextlib.suppress(Exception):
                        connection.rollback()

            return response

        return after_request

    def _should_commit_transaction(self, status_code: int) -> bool:
        """Determine if transaction should be committed based on status code.

        Args:
            status_code: HTTP response status code.

        Returns:
            True if transaction should be committed, False otherwise.
        """
        http_ok = 200
        http_multiple_choices = 300
        http_bad_request = 400

        should_commit = False

        if self.commit_mode == "autocommit":
            should_commit = http_ok <= status_code < http_multiple_choices
        elif self.commit_mode == "autocommit_include_redirect":
            should_commit = http_ok <= status_code < http_bad_request

        # Apply extra status overrides
        if self.extra_commit_statuses and status_code in self.extra_commit_statuses:
            should_commit = True
        elif self.extra_rollback_statuses and status_code in self.extra_rollback_statuses:
            should_commit = False

        return should_commit

    def _teardown_session(self, exception: "Optional[BaseException]" = None) -> None:
        """Clean up database connections at the end of request.

        Args:
            exception: Exception that occurred during request processing, if any.
        """
        from flask import g

        # Close any open connection
        connection = getattr(g, self.connection_key, None)
        if connection is not None:
            if hasattr(connection, "close") and callable(connection.close):
                with contextlib.suppress(Exception):
                    connection.close()
            delattr(g, self.connection_key)
