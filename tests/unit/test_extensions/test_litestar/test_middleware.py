"""Unit tests for Litestar correlation middleware."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import UUID

import pytest
from litestar.datastructures import MutableScopeHeaders

from sqlspec.extensions.litestar.middleware import CorrelationMiddleware


class TestCorrelationMiddleware:
    """Test the correlation tracking middleware."""

    @pytest.fixture
    def middleware(self) -> CorrelationMiddleware:
        """Create middleware instance."""
        config = MagicMock()  # Mock AppConfig
        return CorrelationMiddleware(config)

    @pytest.fixture
    def http_scope(self) -> dict[str, Any]:
        """Create a mock HTTP scope."""
        return {
            "type": "http",
            "headers": [(b"host", b"example.com"), (b"user-agent", b"test-agent")],
            "path": "/test",
            "method": "GET",
            "state": {},  # Litestar puts state in scope
        }

    @pytest.fixture
    def websocket_scope(self):
        """Create a mock WebSocket scope."""
        return {"type": "websocket", "headers": [], "path": "/ws"}

    @pytest.mark.asyncio
    async def test_non_http_passthrough(
        self, middleware: CorrelationMiddleware, websocket_scope: dict[str, Any]
    ) -> None:
        """Test that non-HTTP requests are passed through."""
        receive = AsyncMock()
        send = AsyncMock()
        next_app = AsyncMock()

        await middleware.handle(websocket_scope, receive, send, next_app)

        next_app.assert_called_once_with(websocket_scope, receive, send)

    @pytest.mark.asyncio
    async def test_existing_correlation_id(self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]) -> None:
        """Test handling of existing correlation ID in headers."""
        # Add correlation ID to headers
        http_scope["headers"].append((b"x-correlation-id", b"existing-id-123"))

        receive = AsyncMock()
        send = AsyncMock()

        with patch("sqlspec.utils.correlation.CorrelationContext.set") as mock_set:
            await middleware.handle(http_scope, receive, send, AsyncMock())

            # Should set existing ID and then restore None (the previous value)
            assert mock_set.call_count == 2
            assert mock_set.call_args_list[0] == call("existing-id-123")
            assert mock_set.call_args_list[1] == call(None)

    @pytest.mark.asyncio
    async def test_generate_correlation_id(self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]) -> None:
        """Test generation of new correlation ID when not present."""
        receive = AsyncMock()
        send = AsyncMock()

        with (
            patch("sqlspec.utils.correlation.CorrelationContext.set") as mock_set,
            patch("sqlspec.extensions.litestar.middleware.uuid4") as mock_uuid,
        ):
            mock_uuid.return_value = UUID("12345678-1234-5678-1234-567812345678")

            await middleware.handle(http_scope, receive, send, AsyncMock())

            # Should generate and set new ID, then restore None
            assert mock_set.call_count == 2
            assert mock_set.call_args_list[0] == call("12345678-1234-5678-1234-567812345678")
            assert mock_set.call_args_list[1] == call(None)

    @pytest.mark.asyncio
    async def test_response_header_injection(
        self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]
    ) -> None:
        """Test that correlation ID is added to response headers."""
        receive = AsyncMock()

        # Capture the modified send function
        actual_send = None

        async def capture_send(scope, receive, send) -> None:
            nonlocal actual_send
            actual_send = send

        # Create a mock for the original send
        original_send = AsyncMock()

        with patch("sqlspec.extensions.litestar.middleware.uuid4") as mock_uuid:
            mock_uuid.return_value = UUID("12345678-1234-5678-1234-567812345678")

            await middleware.handle(http_scope, receive, original_send, capture_send)

            # Simulate sending response start
            await actual_send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"application/json")],
                }
            )

            # Check that correlation ID was added to headers
            original_send.assert_called_once()
            message = original_send.call_args[0][0]
            assert message["type"] == "http.response.start"
            assert message["status"] == 200

            # Find correlation ID header
            headers = message["headers"]
            correlation_header = None
            for name, value in headers:
                if name == b"x-correlation-id":
                    correlation_header = value
                    break

            assert correlation_header == b"12345678-1234-5678-1234-567812345678"

    @pytest.mark.asyncio
    async def test_passthrough_other_messages(
        self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]
    ) -> None:
        """Test that non-response-start messages are passed through unchanged."""
        receive = AsyncMock()

        # Capture the modified send function
        actual_send = None

        async def capture_send(scope, receive, send) -> None:
            nonlocal actual_send
            actual_send = send

        original_send = AsyncMock()

        await middleware.handle(http_scope, receive, original_send, capture_send)

        # Send a body message
        body_message = {"type": "http.response.body", "body": b"test body"}
        await actual_send(body_message)

        # Should pass through unchanged
        original_send.assert_called_with(body_message)

    @pytest.mark.asyncio
    async def test_context_cleanup(self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]) -> None:
        """Test that correlation context is restored after request."""
        receive = AsyncMock()
        send = AsyncMock()

        with patch("sqlspec.utils.correlation.CorrelationContext.set") as mock_set:
            await middleware.handle(http_scope, receive, send, AsyncMock())

            # Context should be set and then restored to None
            assert mock_set.call_count == 2
            # First call sets the correlation ID, second call restores None
            assert mock_set.call_args_list[1] == call(None)

    @pytest.mark.asyncio
    async def test_error_handling(self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]) -> None:
        """Test that errors in app are propagated and context is still restored."""
        receive = AsyncMock()
        send = AsyncMock()

        # Make next_app raise an error
        error = ValueError("Test error")
        error_app = AsyncMock(side_effect=error)

        with patch("sqlspec.utils.correlation.CorrelationContext.set") as mock_set:
            with pytest.raises(ValueError, match="Test error"):
                await middleware.handle(http_scope, receive, send, error_app)

            # Context should still be restored to None even on error
            assert mock_set.call_count == 2
            assert mock_set.call_args_list[1] == call(None)

    @pytest.mark.asyncio
    async def test_case_insensitive_header_lookup(
        self, middleware: CorrelationMiddleware, http_scope: dict[str, Any]
    ) -> None:
        """Test that header lookup is case-insensitive."""
        # Add correlation ID with different case
        http_scope["headers"].append((b"X-Correlation-ID", b"case-test-123"))

        receive = AsyncMock()
        send = AsyncMock()

        with patch("sqlspec.utils.correlation.CorrelationContext.set") as mock_set:
            await middleware.handle(http_scope, receive, send, AsyncMock())

            # Should still find and use the ID (then restore None)
            assert mock_set.call_count == 2
            assert mock_set.call_args_list[0] == call("case-test-123")
            assert mock_set.call_args_list[1] == call(None)

    def test_mutable_scope_headers_integration(self) -> None:
        """Test that MutableScopeHeaders is used correctly."""
        scope = {
            "type": "http",
            "headers": [(b"x-correlation-id", b"test-123"), (b"content-type", b"application/json")],
        }

        headers = MutableScopeHeaders(scope)

        # Test get
        assert headers.get("x-correlation-id") == "test-123"
        assert headers.get("X-Correlation-ID") == "test-123"  # Case insensitive
        assert headers.get("missing") is None
        assert headers.get("missing", "default") == "default"
