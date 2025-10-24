"""Flask extension for SQLSpec database integration."""

from typing import TYPE_CHECKING, Any, Literal

from sqlspec.base import SQLSpec
from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.flask._portal import PortalProvider
from sqlspec.extensions.flask._state import FlaskConfigState
from sqlspec.extensions.flask._utils import get_or_create_session
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from flask import Flask, Response

__all__ = ("SQLSpecPlugin",)

logger = get_logger("extensions.flask")

DEFAULT_COMMIT_MODE: Literal["manual"] = "manual"
DEFAULT_SESSION_KEY = "default"


class SQLSpecPlugin:
    """Flask extension for SQLSpec database integration.

    Provides request-scoped session management, automatic transaction handling,
    and async adapter support via portal pattern.

    Example:
        from flask import Flask
        from sqlspec import SQLSpec
        from sqlspec.adapters.sqlite import SqliteConfig
        from sqlspec.extensions.flask import SQLSpecPlugin

        sqlspec = SQLSpec()
        config = SqliteConfig(
            pool_config={"database": "app.db"},
            extension_config={
                "flask": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        plugin = SQLSpecPlugin(sqlspec, app)

        @app.route("/users")
        def list_users():
            db = plugin.get_session()
            result = db.execute("SELECT * FROM users")
            return {"users": result.all()}
    """

    def __init__(self, sqlspec: SQLSpec, app: "Flask | None" = None) -> None:
        """Initialize Flask extension with SQLSpec instance.

        Args:
            sqlspec: SQLSpec instance with registered configs.
            app: Optional Flask application to initialize immediately.
        """
        self._sqlspec = sqlspec
        self._config_states: list[FlaskConfigState] = []
        self._portal: PortalProvider | None = None
        self._has_async_configs = False

        for cfg in self._sqlspec.configs.values():
            state = self._create_config_state(cfg)
            self._config_states.append(state)

            if state.is_async:
                self._has_async_configs = True

        if app is not None:
            self.init_app(app)

    def _create_config_state(self, config: Any) -> FlaskConfigState:
        """Create configuration state from database config.

        Args:
            config: Database configuration instance.

        Returns:
            FlaskConfigState instance.
        """
        flask_config = config.extension_config.get("flask", {})

        session_key = flask_config.get("session_key", DEFAULT_SESSION_KEY)
        connection_key = flask_config.get("connection_key", f"sqlspec_connection_{session_key}")
        commit_mode = flask_config.get("commit_mode", DEFAULT_COMMIT_MODE)
        extra_commit_statuses = flask_config.get("extra_commit_statuses")
        extra_rollback_statuses = flask_config.get("extra_rollback_statuses")

        is_async = isinstance(config, (AsyncDatabaseConfig, NoPoolAsyncConfig))

        return FlaskConfigState(
            config=config,
            connection_key=connection_key,
            session_key=session_key,
            commit_mode=commit_mode,
            extra_commit_statuses=extra_commit_statuses,
            extra_rollback_statuses=extra_rollback_statuses,
            is_async=is_async,
        )

    def init_app(self, app: "Flask") -> None:
        """Initialize Flask application with SQLSpec.

        Validates configuration, creates portal if needed, creates pools,
        and registers hooks.

        Args:
            app: Flask application to initialize.

        Raises:
            ImproperConfigurationError: If extension already registered or keys not unique.
        """
        if "sqlspec" in app.extensions:
            msg = "SQLSpec extension already registered on this Flask application"
            raise ImproperConfigurationError(msg)

        self._validate_unique_keys()

        if self._has_async_configs:
            self._portal = PortalProvider()
            self._portal.start()
            logger.debug("Portal provider started for async adapters")

        pools: dict[str, Any] = {}
        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                if config_state.is_async:
                    pool = self._portal.portal.call(config_state.config.create_pool)  # type: ignore[union-attr]
                else:
                    pool = config_state.config.create_pool()
                pools[config_state.session_key] = pool

        app.extensions["sqlspec"] = {"plugin": self, "pools": pools}

        app.before_request(self._before_request_handler)
        app.after_request(self._after_request_handler)
        app.teardown_appcontext(self._teardown_appcontext_handler)

        logger.debug("SQLSpec Flask extension initialized")

    def _validate_unique_keys(self) -> None:
        """Validate that all state keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys found.
        """
        all_keys: set[str] = set()

        for state in self._config_states:
            keys = {state.connection_key, state.session_key}
            duplicates = all_keys & keys

            if duplicates:
                msg = f"Duplicate state keys found: {duplicates}. Use unique session_key values."
                raise ImproperConfigurationError(msg)

            all_keys.update(keys)

    def _before_request_handler(self) -> None:
        """Acquire connection before request.

        Stores connection in Flask g object for each configured database.
        Also stores context managers for proper cleanup.
        """
        from flask import current_app, g

        for config_state in self._config_states:
            if config_state.config.supports_connection_pooling:
                pool = current_app.extensions["sqlspec"]["pools"][config_state.session_key]
                conn_ctx = config_state.config.provide_connection(pool)

                if config_state.is_async:
                    connection = self._portal.portal.call(conn_ctx.__aenter__)  # type: ignore[union-attr]
                else:
                    connection = conn_ctx.__enter__()

                setattr(g, f"{config_state.connection_key}_ctx", conn_ctx)
            elif config_state.is_async:
                connection = self._portal.portal.call(config_state.config.create_connection)  # type: ignore[union-attr]
            else:
                connection = config_state.config.create_connection()

            setattr(g, config_state.connection_key, connection)

    def _after_request_handler(self, response: "Response") -> "Response":
        """Handle transaction after request based on response status.

        Args:
            response: Flask response object.

        Returns:
            Response object (unchanged).
        """
        from flask import g

        for config_state in self._config_states:
            if config_state.commit_mode == "manual":
                continue

            cache_key = f"sqlspec_session_cache_{config_state.session_key}"
            session = getattr(g, cache_key, None)

            if session is None:
                continue

            if config_state.should_commit(response.status_code):
                self._execute_commit(session, config_state)
            elif config_state.should_rollback(response.status_code):
                self._execute_rollback(session, config_state)

        return response

    def _teardown_appcontext_handler(self, _exc: "Exception | None" = None) -> None:
        """Clean up connections when request context ends.

        Closes all connections and cleans up g object.

        Args:
            _exc: Exception that occurred (if any).
        """
        from flask import g

        for config_state in self._config_states:
            connection = getattr(g, config_state.connection_key, None)
            ctx_key = f"{config_state.connection_key}_ctx"
            conn_ctx = getattr(g, ctx_key, None)

            if connection is not None:
                try:
                    if conn_ctx is not None:
                        if config_state.is_async:
                            self._portal.portal.call(conn_ctx.__aexit__, None, None, None)  # type: ignore[union-attr]
                        else:
                            conn_ctx.__exit__(None, None, None)
                    elif config_state.is_async:
                        self._portal.portal.call(connection.close)  # type: ignore[union-attr]
                    else:
                        connection.close()
                except Exception as close_exc:
                    logger.exception("Error closing connection: %s", close_exc)

                if hasattr(g, config_state.connection_key):
                    delattr(g, config_state.connection_key)
                if hasattr(g, ctx_key):
                    delattr(g, ctx_key)

            cache_key = f"sqlspec_session_cache_{config_state.session_key}"
            if hasattr(g, cache_key):
                delattr(g, cache_key)

    def get_session(self, key: "str | None" = None) -> Any:
        """Get or create database session for current request.

        Sessions are cached per request for consistency.

        Args:
            key: Session key for multi-database configs. Defaults to first config if None.

        Returns:
            Database session (driver instance).

        Raises:
            ImproperConfigurationError: If key not found.
        """
        if key is None:
            config_state = self._config_states[0]
        else:
            config_state = self._get_config_state_by_key(key)

        return get_or_create_session(config_state, self._portal.portal if self._portal else None)

    def get_connection(self, key: "str | None" = None) -> Any:
        """Get database connection for current request.

        Args:
            key: Session key for multi-database configs. Defaults to first config if None.

        Returns:
            Raw database connection.

        Raises:
            ImproperConfigurationError: If key not found.
        """
        from flask import g

        if key is None:
            config_state = self._config_states[0]
        else:
            config_state = self._get_config_state_by_key(key)

        return getattr(g, config_state.connection_key)

    def _get_config_state_by_key(self, key: str) -> FlaskConfigState:
        """Get config state by session key.

        Args:
            key: Session key to look up.

        Returns:
            FlaskConfigState for the key.

        Raises:
            ImproperConfigurationError: If key not found.
        """
        for state in self._config_states:
            if state.session_key == key:
                return state

        msg = f"No configuration found for key: {key}"
        raise ImproperConfigurationError(msg)

    def _execute_commit(self, session: Any, config_state: FlaskConfigState) -> None:
        """Execute commit on session.

        Args:
            session: Database session.
            config_state: Configuration state.
        """
        try:
            if config_state.is_async:
                connection = self.get_connection(config_state.session_key)
                self._portal.portal.call(connection.commit)  # type: ignore[union-attr]
            else:
                connection = self.get_connection(config_state.session_key)
                connection.commit()
        except Exception:
            logger.exception("Error committing transaction")

    def _execute_rollback(self, session: Any, config_state: FlaskConfigState) -> None:
        """Execute rollback on session.

        Args:
            session: Database session.
            config_state: Configuration state.
        """
        try:
            if config_state.is_async:
                connection = self.get_connection(config_state.session_key)
                self._portal.portal.call(connection.rollback)  # type: ignore[union-attr]
            else:
                connection = self.get_connection(config_state.session_key)
                connection.rollback()
        except Exception as exc:
            logger.debug("Rollback failed (may be no active transaction): %s", exc)
