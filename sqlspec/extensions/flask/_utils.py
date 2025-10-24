"""Helper utilities for Flask extension."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flask import Flask

    from sqlspec.extensions.flask._portal import Portal
    from sqlspec.extensions.flask._state import FlaskConfigState

__all__ = ("get_or_create_session",)


def get_or_create_session(
    config_state: "FlaskConfigState",
    portal: "Portal | None",
) -> Any:
    """Get or create database session for current request.

    Sessions are cached per request in Flask g object to ensure
    the same session is reused throughout the request lifecycle.

    Args:
        config_state: Configuration state for this database.
        portal: Portal for async operations (None for sync).

    Returns:
        Database session (driver instance).
    """
    from flask import g

    cache_key = f"sqlspec_session_cache_{config_state.session_key}"

    cached_session = getattr(g, cache_key, None)
    if cached_session is not None:
        return cached_session

    connection = getattr(g, config_state.connection_key)

    session = config_state.config.driver_type(
        connection=connection,
        statement_config=config_state.config.statement_config,
    )

    setattr(g, cache_key, session)
    return session
