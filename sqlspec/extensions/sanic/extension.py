import logging
from typing import TYPE_CHECKING, Any

from sqlspec.base import SQLSpec
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic._state import SanicConfigState
from sqlspec.extensions.sanic._utils import get_context_value, get_or_create_session
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sanic import Sanic

__all__ = ("SQLSpecPlugin",)

logger = get_logger("sqlspec.extensions.sanic")

DEFAULT_COMMIT_MODE = "manual"
DEFAULT_CONNECTION_KEY = "db_connection"
DEFAULT_POOL_KEY = "db_pool"
DEFAULT_SESSION_KEY = "db_session"


class SQLSpecPlugin:
    """SQLSpec integration for Sanic applications.

    Provides Sanic-native configuration parsing and request helper methods.
    Runtime listener and middleware behavior is registered by ``init_app``.

    Example:
        from sanic import Sanic
        from sqlspec import SQLSpec
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.extensions.sanic import SQLSpecPlugin

        sqlspec = SQLSpec()
        sqlspec.add_config(
            AsyncpgConfig(
                connection_config={"dsn": "postgresql://localhost/mydb"},
                extension_config={
                    "sanic": {
                        "commit_mode": "autocommit",
                        "session_key": "db",
                    }
                },
            )
        )

        app = Sanic("app")
        db_ext = SQLSpecPlugin(sqlspec, app)
    """

    __slots__ = ("_config_states", "_sqlspec")

    def __init__(self, sqlspec: SQLSpec, app: "Sanic[Any, Any] | None" = None) -> None:
        """Initialize SQLSpec Sanic extension.

        Args:
            sqlspec: Pre-configured SQLSpec instance with registered configs.
            app: Optional Sanic application to initialize immediately.
        """
        self._sqlspec = sqlspec
        self._config_states: list[SanicConfigState] = []

        for cfg in self._sqlspec.configs.values():
            settings = self._extract_sanic_settings(cfg)
            state = self._create_config_state(cfg, settings)
            self._config_states.append(state)

        if app is not None:
            self.init_app(app)

        log_with_context(
            logger,
            logging.DEBUG,
            "extension.init",
            framework="sanic",
            stage="init",
            config_count=len(self._config_states),
        )

    def _extract_sanic_settings(self, config: Any) -> "dict[str, Any]":
        """Extract Sanic settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of Sanic-specific settings.
        """
        sanic_config = config.extension_config.get("sanic", {})

        connection_key = sanic_config.get("connection_key", DEFAULT_CONNECTION_KEY)
        pool_key = sanic_config.get("pool_key", DEFAULT_POOL_KEY)
        session_key = sanic_config.get("session_key", DEFAULT_SESSION_KEY)
        commit_mode = sanic_config.get("commit_mode", DEFAULT_COMMIT_MODE)

        if not config.supports_connection_pooling and pool_key == DEFAULT_POOL_KEY:
            pool_key = f"_{DEFAULT_POOL_KEY}_{id(config)}"

        correlation_headers = sanic_config.get("correlation_headers")
        if correlation_headers is not None:
            correlation_headers = tuple(correlation_headers)

        return {
            "connection_key": connection_key,
            "pool_key": pool_key,
            "session_key": session_key,
            "commit_mode": commit_mode,
            "extra_commit_statuses": sanic_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": sanic_config.get("extra_rollback_statuses"),
            "disable_di": sanic_config.get("disable_di", False),
            "enable_correlation_middleware": sanic_config.get("enable_correlation_middleware", False),
            "correlation_header": sanic_config.get("correlation_header", "x-request-id"),
            "correlation_headers": correlation_headers,
            "auto_trace_headers": sanic_config.get("auto_trace_headers", True),
            "enable_sqlcommenter_middleware": sanic_config.get("enable_sqlcommenter_middleware", True),
            "sqlcommenter_framework": sanic_config.get("sqlcommenter_framework", "sanic"),
        }

    def _create_config_state(self, config: Any, settings: "dict[str, Any]") -> SanicConfigState:
        """Create configuration state object.

        Args:
            config: Database configuration instance.
            settings: Extracted Sanic settings.

        Returns:
            Configuration state instance.
        """
        return SanicConfigState(
            config=config,
            connection_key=settings["connection_key"],
            pool_key=settings["pool_key"],
            session_key=settings["session_key"],
            commit_mode=settings["commit_mode"],
            extra_commit_statuses=settings["extra_commit_statuses"],
            extra_rollback_statuses=settings["extra_rollback_statuses"],
            disable_di=settings["disable_di"],
            enable_correlation_middleware=settings["enable_correlation_middleware"],
            correlation_header=settings["correlation_header"],
            correlation_headers=settings["correlation_headers"],
            auto_trace_headers=settings["auto_trace_headers"],
            enable_sqlcommenter_middleware=settings["enable_sqlcommenter_middleware"],
            sqlcommenter_framework=settings["sqlcommenter_framework"],
        )

    def init_app(self, app: "Sanic[Any, Any]") -> None:
        """Initialize Sanic application with SQLSpec.

        Args:
            app: Sanic application instance.
        """
        self._validate_unique_keys()
        setattr(app.ctx, "sqlspec_plugin", self)

    def _validate_unique_keys(self) -> None:
        """Validate that all context keys are unique across configs.

        Raises:
            ImproperConfigurationError: If duplicate keys are found.
        """
        all_keys: set[str] = set()

        for state in self._config_states:
            keys = {state.connection_key, state.pool_key, state.session_key}
            duplicates = all_keys & keys

            if duplicates:
                msg = f"Duplicate context keys found: {duplicates}"
                raise ImproperConfigurationError(msg)

            all_keys.update(keys)

    def get_session(self, request: Any, key: "str | None" = None) -> Any:
        """Get or create database session for request.

        Args:
            request: Sanic request instance.
            key: Optional session key to retrieve a specific database session.

        Returns:
            Database session driver instance.
        """
        config_state = self._config_states[0] if key is None else self._get_config_state_by_key(key)
        return get_or_create_session(request, config_state)

    def get_connection(self, request: Any, key: "str | None" = None) -> Any:
        """Get database connection from request context.

        Args:
            request: Sanic request instance.
            key: Optional session key to retrieve a specific database connection.

        Returns:
            Database connection object.
        """
        config_state = self._config_states[0] if key is None else self._get_config_state_by_key(key)
        return get_context_value(request.ctx, config_state.connection_key)

    def _get_config_state_by_key(self, key: str) -> SanicConfigState:
        """Get configuration state by session key.

        Args:
            key: Session key to search for.

        Returns:
            Configuration state matching the key.

        Raises:
            ValueError: If no configuration is found with the specified key.
        """
        for state in self._config_states:
            if state.session_key == key:
                return state

        msg = f"No configuration found with session_key: {key}"
        raise ValueError(msg)
