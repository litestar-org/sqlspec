"""Tests for Flask extension utility helpers."""

from typing import Any

import pytest

pytest.importorskip("flask")

from flask import Flask, g

from sqlspec.extensions.flask import FlaskConfigState
from sqlspec.extensions.flask._utils import get_or_create_session


class _Driver:
    def __init__(self, *, connection: Any, statement_config: Any, driver_features: dict[str, Any]) -> None:
        self.connection = connection
        self.statement_config = statement_config
        self.driver_features = driver_features


class _Config:
    driver_type = _Driver
    driver_features = {"returning_support": True}
    statement_config = object()


def _make_state() -> FlaskConfigState:
    return FlaskConfigState(
        config=_Config(),  # type: ignore[arg-type]
        connection_key="sqlspec_connection",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )


def test_get_or_create_session_passes_driver_features() -> None:
    app = Flask(__name__)
    state = _make_state()
    connection = object()

    with app.app_context():
        setattr(g, state.connection_key, connection)
        session = get_or_create_session(state, portal=None)

    assert session.connection is connection
    assert session.statement_config is _Config.statement_config
    assert session.driver_features == _Config.driver_features


def test_get_or_create_session_returns_cached_session() -> None:
    app = Flask(__name__)
    state = _make_state()

    with app.app_context():
        setattr(g, state.connection_key, object())
        first = get_or_create_session(state, portal=None)
        second = get_or_create_session(state, portal=None)

    assert second is first
