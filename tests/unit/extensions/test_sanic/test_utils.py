"""Tests for Sanic extension context utilities."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic import SanicConfigState, get_connection_from_request, get_or_create_session
from sqlspec.extensions.sanic._utils import get_context_value, pop_context_value, set_context_value


def test_context_set_get_pop() -> None:
    """Sanic ctx helpers should use attribute storage."""
    ctx = SimpleNamespace()
    value = object()

    set_context_value(ctx, "db_connection", value)

    assert get_context_value(ctx, "db_connection") is value
    assert pop_context_value(ctx, "db_connection") is value
    assert pop_context_value(ctx, "db_connection") is None


def test_get_connection_from_request() -> None:
    """get_connection_from_request should read request.ctx."""
    connection = object()
    request = SimpleNamespace(ctx=SimpleNamespace(db_connection=connection))
    config_state = _make_state()

    result = get_connection_from_request(request, config_state)

    assert result is connection


def test_get_connection_from_request_raises_when_missing() -> None:
    """Missing request connections should raise a SQLSpec configuration error."""
    request = SimpleNamespace(ctx=SimpleNamespace())
    config_state = _make_state()

    with pytest.raises(ImproperConfigurationError) as exc_info:
        get_connection_from_request(request, config_state)

    assert "db_connection" in str(exc_info.value)


def test_get_or_create_session_creates_and_caches_session() -> None:
    """get_or_create_session should cache one driver instance per request."""
    connection = object()
    request = SimpleNamespace(ctx=SimpleNamespace(db_connection=connection))
    config = MagicMock()
    config.driver_type = MagicMock()
    config.statement_config = {"test": "config"}
    config.driver_features = {"feature": True}
    config_state = _make_state(config=config)

    session = get_or_create_session(request, config_state)
    cached_session = get_or_create_session(request, config_state)

    assert cached_session is session
    config.driver_type.assert_called_once_with(
        connection=connection, statement_config={"test": "config"}, driver_features={"feature": True}
    )


def _make_state(config: MagicMock | None = None) -> SanicConfigState:
    return SanicConfigState(
        config=config or MagicMock(),
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )
