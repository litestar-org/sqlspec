from typing import TYPE_CHECKING, Any

from starlette.middleware.base import BaseHTTPMiddleware

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from starlette.requests import Request

    from sqlspec.extensions.starlette._state import _ConfigState

__all__ = ("SQLSpecAutocommitMiddleware", "SQLSpecManualMiddleware")

logger = get_logger("extensions.starlette.middleware")


class SQLSpecManualMiddleware(BaseHTTPMiddleware):
    """Middleware for manual transaction mode.

    Acquires connection from pool, stores in request.state, releases after request.
    No automatic commit or rollback - user code must handle transactions.
    """

    def __init__(self, app: Any, config_state: "_ConfigState") -> None:
        """Initialize middleware.

        Args:
            app: Starlette application instance.
            config_state: Configuration state for this database.
        """
        super().__init__(app)
        self.config_state = config_state

    async def dispatch(self, request: "Request", call_next: Any) -> Any:
        """Process request with manual transaction mode.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response.
        """
        config = self.config_state.config
        connection_key = self.config_state.connection_key

        if config.supports_connection_pooling:
            pool = getattr(request.app.state, self.config_state.pool_key)
            async with config.provide_connection(pool) as connection:
                setattr(request.state, connection_key, connection)
                try:
                    return await call_next(request)
                finally:
                    delattr(request.state, connection_key)
        else:
            connection = await config.create_connection()
            setattr(request.state, connection_key, connection)
            try:
                return await call_next(request)
            finally:
                await connection.close()


class SQLSpecAutocommitMiddleware(BaseHTTPMiddleware):
    """Middleware for autocommit transaction mode.

    Acquires connection, commits on success status codes, rollbacks on error status codes.
    """

    def __init__(self, app: Any, config_state: "_ConfigState", include_redirect: bool = False) -> None:
        """Initialize middleware.

        Args:
            app: Starlette application instance.
            config_state: Configuration state for this database.
            include_redirect: If True, commit on 3xx status codes as well.
        """
        super().__init__(app)
        self.config_state = config_state
        self.include_redirect = include_redirect

    async def dispatch(self, request: "Request", call_next: Any) -> Any:
        """Process request with autocommit transaction mode.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response.
        """
        config = self.config_state.config
        connection_key = self.config_state.connection_key

        if config.supports_connection_pooling:
            pool = getattr(request.app.state, self.config_state.pool_key)
            async with config.provide_connection(pool) as connection:
                setattr(request.state, connection_key, connection)
                try:
                    response = await call_next(request)

                    if self._should_commit(response.status_code):
                        await connection.commit()
                    else:
                        await connection.rollback()

                    return response
                except Exception:
                    await connection.rollback()
                    raise
                finally:
                    delattr(request.state, connection_key)
        else:
            connection = await config.create_connection()
            setattr(request.state, connection_key, connection)
            try:
                response = await call_next(request)

                if self._should_commit(response.status_code):
                    await connection.commit()
                else:
                    await connection.rollback()

                return response
            except Exception:
                await connection.rollback()
                raise
            finally:
                await connection.close()

    def _should_commit(self, status_code: int) -> bool:
        """Determine if response status code should trigger commit.

        Args:
            status_code: HTTP status code.

        Returns:
            True if should commit, False if should rollback.
        """
        extra_commit = self.config_state.extra_commit_statuses or set()
        extra_rollback = self.config_state.extra_rollback_statuses or set()

        if status_code in extra_commit:
            return True
        if status_code in extra_rollback:
            return False

        if 200 <= status_code < 300:
            return True
        return bool(self.include_redirect and 300 <= status_code < 400)
