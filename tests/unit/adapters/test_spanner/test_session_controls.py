"""Unit tests for Spanner session-control behavior."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

import sqlspec.adapters.spanner.config as spanner_config
from sqlspec.adapters.spanner.config import SpannerSyncConfig


def test_provide_session_uses_config_driver_features(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    request_options = {"priority": 1, "request_tag": "sqlspec-test"}
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"request_options": request_options},
    )
    monkeypatch.setattr(spanner_config, "SpannerSessionContext", _SessionContext)

    context = config.provide_session()

    assert isinstance(context, _SessionContext)
    assert captured["driver_features"] is config.driver_features
    assert captured["driver_features"]["request_options"] is request_options
    assert "database_provider" not in captured["driver_features"]


def test_provide_session_accepts_spanner_execution_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _SessionContext:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    config_request_options = {"request_tag": "config"}
    session_request_options = {"request_tag": "session"}
    directed_read_options = cast(Any, SimpleNamespace(tag="directed"))
    retry = cast(Any, object())
    config = SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"},
        driver_features={"request_options": config_request_options},
    )
    monkeypatch.setattr(spanner_config, "SpannerSessionContext", _SessionContext)

    config.provide_session(
        request_options=session_request_options, directed_read_options=directed_read_options, retry=retry, timeout=12.0
    )

    assert captured["driver_features"] is not config.driver_features
    assert captured["driver_features"]["request_options"] is session_request_options
    assert captured["driver_features"]["directed_read_options"] is directed_read_options
    assert captured["driver_features"]["retry"] is retry
    assert captured["driver_features"]["timeout"] == 12.0
    assert config.driver_features["request_options"] is config_request_options
    assert "database_provider" not in captured["driver_features"]
