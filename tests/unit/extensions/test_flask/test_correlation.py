"""Tests for Flask correlation ID handling."""

import pytest

pytest.importorskip("flask")

from flask import Flask, g

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.flask import SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext


class TestFlaskCorrelationExtraction:
    """Tests for correlation ID extraction in Flask."""

    def test_extracts_correlation_from_header(self) -> None:
        """Should extract correlation ID from x-request-id header."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[str | None] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(getattr(g, "correlation_id", None))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-request-id": "flask-corr-123"})
            assert response.status_code == 200
            assert seen_correlation_id[0] == "flask-corr-123"

    def test_extracts_from_custom_header(self) -> None:
        """Should extract from custom correlation header."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={
                "flask": {"enable_correlation_middleware": True, "correlation_header": "x-custom-correlation"}
            },
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[str | None] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(getattr(g, "correlation_id", None))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-custom-correlation": "custom-123"})
            assert response.status_code == 200
            assert seen_correlation_id[0] == "custom-123"

    def test_generates_uuid_when_no_header(self) -> None:
        """Should generate UUID when no correlation header present."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[str | None] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(getattr(g, "correlation_id", None))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test")
            assert response.status_code == 200
            assert seen_correlation_id[0] is not None
            assert len(seen_correlation_id[0]) == 36  # UUID format


class TestFlaskCorrelationResponseHeader:
    """Tests for correlation ID in response headers."""

    def test_includes_correlation_in_response(self) -> None:
        """Should include X-Correlation-ID in response headers."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        @app.route("/test")
        def test_endpoint() -> str:
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-request-id": "response-corr-123"})
            assert response.headers.get("X-Correlation-ID") == "response-corr-123"


class TestFlaskCorrelationContext:
    """Tests for CorrelationContext integration."""

    def test_sets_correlation_context(self) -> None:
        """Should set CorrelationContext during request."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_context: list[str | None] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_context.append(CorrelationContext.get())
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-request-id": "context-test-123"})
            assert response.status_code == 200
            assert seen_context[0] == "context-test-123"

    def test_clears_correlation_context_after_request(self) -> None:
        """Should clear CorrelationContext after request completes."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        @app.route("/test")
        def test_endpoint() -> str:
            return "OK"

        with app.test_client() as client:
            client.get("/test", headers={"x-request-id": "cleared-test-123"})
            assert CorrelationContext.get() is None


class TestFlaskCorrelationDisabled:
    """Tests for disabled correlation middleware."""

    def test_no_correlation_when_disabled(self) -> None:
        """Should not set correlation ID when middleware disabled."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": False}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[bool] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(hasattr(g, "correlation_id"))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-request-id": "disabled-test-123"})
            assert response.status_code == 200
            assert seen_correlation_id[0] is False

    def test_default_is_disabled(self) -> None:
        """Correlation middleware should be disabled by default."""
        sqlspec = SQLSpec()
        config = SqliteConfig(connection_config={"database": ":memory:"})
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[bool] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(hasattr(g, "correlation_id"))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-request-id": "default-test-123"})
            assert response.status_code == 200
            assert seen_correlation_id[0] is False


class TestFlaskCorrelationTraceHeaders:
    """Tests for trace header extraction."""

    def test_extracts_traceparent(self) -> None:
        """Should extract from W3C traceparent header."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True, "auto_trace_headers": True}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[str | None] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(getattr(g, "correlation_id", None))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"traceparent": "00-trace-span-01"})
            assert response.status_code == 200
            assert seen_correlation_id[0] == "00-trace-span-01"

    def test_disable_auto_trace_headers(self) -> None:
        """Should not check trace headers when disabled."""
        sqlspec = SQLSpec()
        config = SqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"flask": {"enable_correlation_middleware": True, "auto_trace_headers": False}},
        )
        sqlspec.add_config(config)

        app = Flask(__name__)
        SQLSpecPlugin(sqlspec, app)

        seen_correlation_id: list[str | None] = []

        @app.route("/test")
        def test_endpoint() -> str:
            seen_correlation_id.append(getattr(g, "correlation_id", None))
            return "OK"

        with app.test_client() as client:
            response = client.get("/test", headers={"x-amzn-trace-id": "aws-trace-123"})
            assert response.status_code == 200
            assert seen_correlation_id[0] != "aws-trace-123"
            assert len(seen_correlation_id[0] or "") == 36  # Generated UUID
