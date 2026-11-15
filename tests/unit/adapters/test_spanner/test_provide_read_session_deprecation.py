"""Tests for the deprecated Spanner ``provide_read_session`` alias."""

from typing import Any

import pytest

from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig

_CONNECTION_CONFIG = {"project": "p", "instance_id": "i", "database_id": "d"}


@pytest.mark.parametrize("config_type", [SpannerSyncConfig, SpannerAsyncConfig], ids=["sync", "async"])
def test_provide_read_session_warns_and_delegates_to_replica_session(
    config_type: type[SpannerSyncConfig | SpannerAsyncConfig], monkeypatch: pytest.MonkeyPatch
) -> None:
    config = config_type(connection_config=_CONNECTION_CONFIG)
    received: dict[str, Any] = {}
    sentinel = object()

    def provide_replica_session(*args: Any, **kwargs: Any) -> object:
        received["args"] = args
        received["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(config, "provide_replica_session", provide_replica_session)

    with pytest.warns(DeprecationWarning, match="provide_replica_session"):
        result = config.provide_read_session("a", timeout=1.5)

    assert result is sentinel
    assert received == {"args": ("a",), "kwargs": {"timeout": 1.5}}
