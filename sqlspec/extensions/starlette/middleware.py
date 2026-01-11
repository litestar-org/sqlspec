from typing import TYPE_CHECKING, Any

from starlette.middleware.base import BaseHTTPMiddleware

from sqlspec.extensions.starlette._utils import get_state_value, pop_state_value, set_state_value

if TYPE_CHECKING:
    from starlette.requests import Request

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = ("SQLSpecAutocommitMiddleware", "SQLSpecManualMiddleware")

HTTP_200_OK = 200
HTTP_300_MULTIPLE_CHOICES = 300
HTTP_400_BAD_REQUEST = 400


class SQLSpecManualMiddleware(BaseHTTPMiddleware):
    """Middleware for manual transaction mode.

    Acquires connection from pool, stores in request.state, releases after request.
    No automatic commit or rollback - user code must handle transactions.
    """

    def __init__(self, app: Any, config_state: "SQLSpecConfigState") -> None:
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
            pool = get_state_value(request.app.state, self.config_state.pool_key)
            async with config.provide_connection(pool) as connection:  # type: ignore[union-attr]
                set_state_value(request.state, connection_key, connection)
                try:
                    return await call_next(request)
                finally:
                    pop_state_value(request.state, connection_key)
        else:
            connection = await config.create_connection()
            set_state_value(request.state, connection_key, connection)
            try:
                return await call_next(request)
            finally:
                await connection.close()


class SQLSpecAutocommitMiddleware(BaseHTTPMiddleware):
    """Middleware for autocommit transaction mode.

    Acquires connection, commits on success status codes, rollbacks on error status codes.
    """

    def __init__(self, app: Any, config_state: "SQLSpecConfigState", include_redirect: bool = False) -> None:
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
            pool = get_state_value(request.app.state, self.config_state.pool_key)
            async with config.provide_connection(pool) as connection:  # type: ignore[union-attr]
                set_state_value(request.state, connection_key, connection)
                try:
                    response = await call_next(request)

                    if self._should_commit(response.status_code):
                        await connection.commit()
                    else:
                        await connection.rollback()
                except Exception:
                    await connection.rollback()
                    raise
                else:
                    return response
                finally:
                    pop_state_value(request.state, connection_key)
        else:
            connection = await config.create_connection()
            set_state_value(request.state, connection_key, connection)
            try:
                response = await call_next(request)

                if self._should_commit(response.status_code):
                    await connection.commit()
                else:
                    await connection.rollback()
            except Exception:
                await connection.rollback()
                raise
            else:
                return response
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

        if HTTP_200_OK <= status_code < HTTP_300_MULTIPLE_CHOICES:
            return True
        return bool(self.include_redirect and HTTP_300_MULTIPLE_CHOICES <= status_code < HTTP_400_BAD_REQUEST)
