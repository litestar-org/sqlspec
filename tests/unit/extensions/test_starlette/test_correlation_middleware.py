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


def test_correlation_middleware_basic_extracts_correlation_id_from_header() -> None:
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


def test_correlation_middleware_basic_returns_correlation_id_in_response_header() -> None:
    """Should include X-Correlation-ID in response headers."""

    def endpoint(request: Request) -> Response:
        return PlainTextResponse("OK")

    app = Starlette(routes=[Route("/", endpoint)])
    app = CorrelationMiddleware(app)
    client = TestClient(app)
    response = client.get("/", headers={"x-request-id": "response-test-123"})
    assert response.headers.get("x-correlation-id") == "response-test-123"


def test_correlation_middleware_basic_generates_uuid_when_no_header() -> None:
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
    assert len(seen_correlation_id[0]) == 36


def test_correlation_middleware_basic_stores_in_request_state() -> None:
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


def test_correlation_middleware_context_preservation_restores_previous_context_after_request() -> None:
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


def test_correlation_middleware_context_preservation_restores_context_on_exception() -> None:
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


def test_correlation_middleware_header_priority_primary_header_takes_precedence() -> None:
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


def test_correlation_middleware_header_priority_additional_headers_checked() -> None:
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


def test_correlation_middleware_header_priority_traceparent_header() -> None:
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


def test_correlation_middleware_header_priority_disable_auto_trace_headers() -> None:
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


def test_correlation_middleware_sanitization_truncates_long_correlation_id() -> None:
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


def test_correlation_middleware_sanitization_strips_whitespace() -> None:
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


def test_correlation_middleware_non_http_passes_through_non_http_requests() -> None:
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


def test_correlation_middleware_equality_repr() -> None:
    """Should have informative repr."""

    async def mock_app(scope: Any, receive: Any, send: Any) -> None:
        pass

    middleware = CorrelationMiddleware(mock_app, primary_header="x-test")
    repr_str = repr(middleware)
    assert "CorrelationMiddleware" in repr_str
