from typing import TYPE_CHECKING, Any

from starlette.middleware.base import BaseHTTPMiddleware

from sqlspec.core import CorrelationExtractor
from sqlspec.extensions.starlette._utils import get_state_value, pop_state_value, set_state_value
from sqlspec.utils.correlation import CorrelationContext

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response

    from sqlspec.extensions.starlette._state import SQLSpecConfigState

__all__ = ("CorrelationMiddleware", "SQLSpecAutocommitMiddleware", "SQLSpecManualMiddleware")

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


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Middleware for correlation ID extraction and propagation.

    Extracts correlation IDs from request headers (or generates new ones)
    and propagates them through the request lifecycle via CorrelationContext.

    The middleware:
    1. Extracts correlation ID from configurable headers
    2. Sets it in the CorrelationContext for async/sync access
    3. Stores it in request.state.correlation_id
    4. Adds X-Correlation-ID header to the response
    5. Cleans up the context on request completion

    Example:
        ```python
        from starlette.applications import Starlette
        from sqlspec.extensions.starlette.middleware import (
            CorrelationMiddleware,
        )

        app = Starlette()
        app.add_middleware(
            CorrelationMiddleware,
            primary_header="x-request-id",
            auto_trace_headers=True,
        )
        ```
    """

    def __init__(
        self,
        app: Any,
        *,
        primary_header: str = "x-request-id",
        additional_headers: tuple[str, ...] | None = None,
        auto_trace_headers: bool = True,
        max_length: int = 128,
    ) -> None:
        """Initialize correlation middleware.

        Args:
            app: Starlette application instance.
            primary_header: The primary header to check first. Defaults to "x-request-id".
            additional_headers: Additional headers to check after the primary header.
            auto_trace_headers: If True, include standard trace context headers as fallbacks.
            max_length: Maximum length for correlation IDs. Defaults to 128.
        """
        super().__init__(app)
        self._extractor = CorrelationExtractor(
            primary_header=primary_header,
            additional_headers=additional_headers,
            auto_trace_headers=auto_trace_headers,
            max_length=max_length,
        )

    async def dispatch(self, request: "Request", call_next: Any) -> "Response":
        """Extract correlation ID and propagate through request lifecycle.

        Args:
            request: Incoming HTTP request.
            call_next: Next middleware or route handler.

        Returns:
            HTTP response with X-Correlation-ID header.
        """
        correlation_id = self._extractor.extract(lambda h: request.headers.get(h))
        previous_id = CorrelationContext.get()

        CorrelationContext.set(correlation_id)
        set_state_value(request.state, "correlation_id", correlation_id)

        try:
            response: Response = await call_next(request)
            response.headers["X-Correlation-ID"] = correlation_id
            return response
        finally:
            CorrelationContext.set(previous_id)
            pop_state_value(request.state, "correlation_id")
