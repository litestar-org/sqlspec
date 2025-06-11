"""Litestar middleware for SQLSpec integration."""

from typing import TYPE_CHECKING, Any, Optional, cast
from uuid import uuid4

from litestar import Request
from litestar.datastructures import MutableScopeHeaders
from litestar.enums import ScopeType
from litestar.middleware import ASGIMiddleware

from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from litestar.config.app import AppConfig
    from litestar.types import ASGIApp, Message, Receive, Scope, Send

__all__ = ("CorrelationMiddleware",)

logger = get_logger("extensions.litestar.middleware")


class CorrelationMiddleware(ASGIMiddleware):
    """Middleware to track correlation IDs across requests.

    This middleware:
    1. Extracts correlation ID from incoming request headers (X-Correlation-ID)
    2. Generates a new correlation ID if none exists
    3. Sets the correlation ID in the CorrelationContext for the request lifetime
    4. Adds the correlation ID to the response headers
    """

    scopes = (ScopeType.HTTP,)

    def __init__(self, config: "AppConfig") -> None:
        """Initialize the middleware.

        Args:
            config: The application configuration
        """
        self.config = config
        self.header_name = b"x-correlation-id"

    async def handle(self, scope: "Scope", receive: "Receive", send: "Send", next_app: "ASGIApp") -> None:
        """Process the request and response.

        Args:
            scope: ASGI connection scope
            receive: ASGI receive channel
            send: ASGI send channel
            next_app: The next ASGI application in the chain
        """
        if scope["type"] != ScopeType.HTTP:
            await next_app(scope, receive, send)
            return

        # Extract or generate correlation ID
        headers = MutableScopeHeaders(scope)
        correlation_id = headers.get("x-correlation-id", None)

        if not correlation_id:
            correlation_id = str(uuid4())
            logger.debug("Generated new correlation ID", extra={"correlation_id": correlation_id})
        else:
            logger.debug("Using existing correlation ID from request", extra={"correlation_id": correlation_id})

        # Store correlation ID in scope for other middleware/handlers
        scope["state"]["correlation_id"] = correlation_id

        # Set correlation context for this request
        with CorrelationContext.context(correlation_id):
            # Wrap send to add correlation ID to response headers
            async def send_wrapper(message: "Message") -> None:
                if message["type"] == "http.response.start":
                    # Add correlation ID to response headers
                    headers = list(message.get("headers", []))

                    # Check if header already exists
                    has_correlation_header = any(header[0].lower() == self.header_name for header in headers)

                    if not has_correlation_header:
                        headers.append((self.header_name, correlation_id.encode()))
                        message["headers"] = headers

                await send(message)

            # Process the request with correlation context
            try:
                await next_app(scope, receive, send_wrapper)
            except Exception:
                logger.exception("Error processing request", extra={"correlation_id": correlation_id})
                raise


def get_correlation_id_from_request(request: Request) -> Optional[str]:
    """Extract correlation ID from request.

    Args:
        request: The Litestar request object

    Returns:
        The correlation ID if found, None otherwise
    """
    # Check state first (set by middleware)
    correlation_id = request.state.get("correlation_id")
    if correlation_id:
        return cast("str", correlation_id)

    # Fall back to header
    return request.headers.get("x-correlation-id")


def correlation_id_extractor(request: Request) -> dict[str, Any]:
    """Extract correlation ID for logging.

    This can be used with Litestar's logging configuration to automatically
    include correlation IDs in all log messages.

    Args:
        request: The Litestar request object

    Returns:
        Dict with correlation_id key
    """
    return {"correlation_id": get_correlation_id_from_request(request)}
