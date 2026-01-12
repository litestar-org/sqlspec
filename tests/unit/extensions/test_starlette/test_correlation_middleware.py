"""Tests for Starlette CorrelationMiddleware behavior."""

from collections.abc import MutableMapping
from typing import Any

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from sqlspec.extensions.starlette.middleware import CorrelationMiddleware
from sqlspec.utils.correlation import CorrelationContext


class TestCorrelationMiddlewareBasic:
    """Basic tests for CorrelationMiddleware."""

    def test_extracts_correlation_id_from_header(self) -> None:
        """Should extract correlation ID from x-request-id header."""
        seen_correlation_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_correlation_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app)

        response = client.get("/", headers={"x-request-id": "test-correlation-123"})

        assert response.status_code == 200
        assert seen_correlation_id[0] == "test-correlation-123"

    def test_returns_correlation_id_in_response_header(self) -> None:
        """Should include X-Correlation-ID in response headers."""

        def endpoint(request: Request) -> Response:
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app)

        response = client.get("/", headers={"x-request-id": "response-test-123"})

        assert response.headers.get("x-correlation-id") == "response-test-123"

    def test_generates_uuid_when_no_header(self) -> None:
        """Should generate UUID when no correlation header provided."""
        seen_correlation_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_correlation_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app)

        response = client.get("/")

        assert response.status_code == 200
        assert seen_correlation_id[0] is not None
        assert len(seen_correlation_id[0]) == 36  # UUID format

    def test_stores_in_request_state(self) -> None:
        """Should store correlation ID in request.state."""
        seen_state_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_state_id.append(getattr(request.state, "correlation_id", None))
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app)

        response = client.get("/", headers={"x-request-id": "state-test-123"})

        assert response.status_code == 200
        assert seen_state_id[0] == "state-test-123"


class TestCorrelationMiddlewareContextPreservation:
    """Tests for correlation context preservation."""

    def test_restores_previous_context_after_request(self) -> None:
        """Should restore previous correlation context after request completes."""
        CorrelationContext.set("outer-context")

        def endpoint(request: Request) -> Response:
            assert CorrelationContext.get() == "inner-123"
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app, raise_server_exceptions=False)

        try:
            client.get("/", headers={"x-request-id": "inner-123"})
            assert CorrelationContext.get() == "outer-context"
        finally:
            CorrelationContext.clear()

    def test_restores_context_on_exception(self) -> None:
        """Should restore context even when endpoint raises exception."""
        CorrelationContext.set("preserved-context")

        def endpoint(request: Request) -> Response:
            msg = "Test error"
            raise ValueError(msg)

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app, raise_server_exceptions=False)

        try:
            client.get("/", headers={"x-request-id": "error-123"})
            assert CorrelationContext.get() == "preserved-context"
        finally:
            CorrelationContext.clear()


class TestCorrelationMiddlewareHeaderPriority:
    """Tests for header extraction priority."""

    def test_primary_header_takes_precedence(self) -> None:
        """Primary header should take precedence over others."""
        seen_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app, primary_header="x-custom-id")
        client = TestClient(app)

        response = client.get("/", headers={"x-custom-id": "custom-primary", "x-request-id": "request-fallback"})

        assert response.status_code == 200
        assert seen_id[0] == "custom-primary"

    def test_additional_headers_checked(self) -> None:
        """Should check additional headers when configured."""
        seen_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app, additional_headers=("x-my-trace",))
        client = TestClient(app)

        response = client.get("/", headers={"x-my-trace": "my-trace-value"})

        assert response.status_code == 200
        assert seen_id[0] == "my-trace-value"

    def test_traceparent_header(self) -> None:
        """Should extract from W3C traceparent header."""
        seen_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app)

        response = client.get("/", headers={"traceparent": "00-trace-span-01"})

        assert response.status_code == 200
        assert seen_id[0] == "00-trace-span-01"

    def test_disable_auto_trace_headers(self) -> None:
        """Should not check trace headers when auto_trace_headers=False."""
        seen_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app, auto_trace_headers=False)
        client = TestClient(app)

        response = client.get("/", headers={"x-amzn-trace-id": "aws-trace"})

        assert response.status_code == 200
        assert seen_id[0] != "aws-trace"


class TestCorrelationMiddlewareSanitization:
    """Tests for value sanitization in middleware."""

    def test_truncates_long_correlation_id(self) -> None:
        """Should truncate correlation IDs exceeding max_length."""
        seen_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app, max_length=20)
        client = TestClient(app)

        long_id = "a" * 50
        response = client.get("/", headers={"x-request-id": long_id})

        assert response.status_code == 200
        assert len(seen_id[0] or "") == 20

    def test_strips_whitespace(self) -> None:
        """Should strip whitespace from correlation ID."""
        seen_id: list[str | None] = []

        def endpoint(request: Request) -> Response:
            seen_id.append(CorrelationContext.get())
            return PlainTextResponse("OK")

        app = Starlette(routes=[Route("/", endpoint)])
        app = CorrelationMiddleware(app)
        client = TestClient(app)

        response = client.get("/", headers={"x-request-id": "  trimmed  "})

        assert response.status_code == 200
        assert seen_id[0] == "trimmed"


class TestCorrelationMiddlewareNonHTTP:
    """Tests for non-HTTP request handling."""

    def test_passes_through_non_http_requests(self) -> None:
        """Should pass through non-HTTP requests without modification."""
        app_called = []

        async def mock_app(scope: Any, receive: Any, send: Any) -> None:
            app_called.append(scope["type"])

        middleware = CorrelationMiddleware(mock_app)

        import asyncio

        scope = {"type": "websocket"}

        async def mock_receive() -> "MutableMapping[str, Any]":
            return {}

        async def mock_send(message: "MutableMapping[str, Any]") -> None:
            pass

        async def run_test() -> None:
            await middleware(scope, mock_receive, mock_send)

        asyncio.run(run_test())
        assert app_called == ["websocket"]


class TestCorrelationMiddlewareEquality:
    """Tests for middleware equality and representation."""

    def test_repr(self) -> None:
        """Should have informative repr."""

        async def mock_app(scope: Any, receive: Any, send: Any) -> None:
            pass

        middleware = CorrelationMiddleware(mock_app, primary_header="x-test")
        repr_str = repr(middleware)
        assert "CorrelationMiddleware" in repr_str
