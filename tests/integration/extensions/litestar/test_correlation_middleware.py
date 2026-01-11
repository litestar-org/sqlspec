from typing import Any, cast

from litestar import Litestar, get
from litestar.testing import TestClient

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.config import ExtensionConfigs
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext


def setup_function() -> None:
    """Clear correlation context before each test to prevent pollution."""
    CorrelationContext.clear()


def teardown_function() -> None:
    """Clear correlation context after each test to prevent pollution."""
    CorrelationContext.clear()


@get("/correlation")
async def correlation_handler() -> dict[str, str | None]:
    return {"correlation_id": CorrelationContext.get()}


def _build_app(
    *,
    enable: bool = True,
    header: str | None = None,
    headers: list[str] | None = None,
    auto_trace_headers: bool | None = None,
) -> Litestar:
    extension_config = cast("ExtensionConfigs", {"litestar": {"enable_correlation_middleware": enable}})
    litestar_settings = cast("dict[str, Any]", extension_config["litestar"])
    if header is not None:
        litestar_settings["correlation_header"] = header
    if headers is not None:
        litestar_settings["correlation_headers"] = headers
    if auto_trace_headers is not None:
        litestar_settings["auto_trace_headers"] = auto_trace_headers

    spec = SQLSpec()
    spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}, extension_config=extension_config))

    return Litestar(route_handlers=[correlation_handler], plugins=[SQLSpecPlugin(sqlspec=spec)])


def test_correlation_middleware_uses_default_header() -> None:
    app = _build_app()

    with TestClient(app) as client:
        response = client.get("/correlation", headers={"X-Request-ID": "abc-123"})
        assert response.json()["correlation_id"] == "abc-123"


def test_correlation_middleware_custom_header() -> None:
    app = _build_app(header="x-correlation-id")

    with TestClient(app) as client:
        response = client.get("/correlation", headers={"X-Correlation-ID": "custom-id"})
        assert response.json()["correlation_id"] == "custom-id"


def test_correlation_middleware_can_be_disabled() -> None:
    app = _build_app(enable=False)

    with TestClient(app) as client:
        response = client.get("/correlation", headers={"X-Request-ID": "should-not-stick"})
        assert response.json()["correlation_id"] is None


def test_correlation_middleware_detects_traceparent_header() -> None:
    app = _build_app()
    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

    with TestClient(app) as client:
        response = client.get("/correlation", headers={"traceparent": traceparent})
        assert response.json()["correlation_id"] == traceparent


def test_correlation_middleware_detects_x_cloud_trace_context_header() -> None:
    app = _build_app()
    header_value = "105445aa7843bc8bf206b120001000/1;o=1"

    with TestClient(app) as client:
        response = client.get("/correlation", headers={"X-Cloud-Trace-Context": header_value})
        assert response.json()["correlation_id"] == header_value


def test_correlation_middleware_auto_detection_can_be_disabled() -> None:
    app = _build_app(header="x-custom-id", auto_trace_headers=False)
    traceparent = "00-11111111111111111111111111111111-2222222222222222-01"

    with TestClient(app) as client:
        response = client.get("/correlation", headers={"traceparent": traceparent})
        assert response.json()["correlation_id"] != traceparent

        response = client.get("/correlation", headers={"X-Custom-ID": "custom-value"})
        assert response.json()["correlation_id"] == "custom-value"
