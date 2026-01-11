"""Tests for Flask configuration state."""

from sqlspec.extensions.flask import FlaskConfigState


def test_should_commit_manual_mode() -> None:
    """Test should_commit in manual mode never commits."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert not state.should_commit(200)
    assert not state.should_commit(201)
    assert not state.should_commit(204)
    assert not state.should_commit(400)
    assert not state.should_commit(500)


def test_should_commit_autocommit_mode() -> None:
    """Test should_commit in autocommit mode commits on 2xx."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="autocommit",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert state.should_commit(200)
    assert state.should_commit(201)
    assert state.should_commit(204)
    assert state.should_commit(299)

    assert not state.should_commit(300)
    assert not state.should_commit(301)
    assert not state.should_commit(400)
    assert not state.should_commit(500)


def test_should_commit_autocommit_include_redirect_mode() -> None:
    """Test should_commit in autocommit_include_redirect mode commits on 2xx-3xx."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="autocommit_include_redirect",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert state.should_commit(200)
    assert state.should_commit(201)
    assert state.should_commit(299)
    assert state.should_commit(300)
    assert state.should_commit(301)
    assert state.should_commit(302)
    assert state.should_commit(303)
    assert state.should_commit(399)

    assert not state.should_commit(400)
    assert not state.should_commit(404)
    assert not state.should_commit(500)


def test_should_commit_extra_commit_statuses() -> None:
    """Test extra_commit_statuses override default behavior."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="autocommit",
        extra_commit_statuses={404, 500},
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert state.should_commit(200)
    assert state.should_commit(404)
    assert state.should_commit(500)


def test_should_commit_extra_rollback_statuses() -> None:
    """Test extra_rollback_statuses override default behavior."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="autocommit",
        extra_commit_statuses=None,
        extra_rollback_statuses={201},
        is_async=False,
        disable_di=False,
    )

    assert state.should_commit(200)
    assert not state.should_commit(201)


def test_should_rollback_manual_mode() -> None:
    """Test should_rollback in manual mode never rolls back."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert not state.should_rollback(200)
    assert not state.should_rollback(400)
    assert not state.should_rollback(500)


def test_should_rollback_autocommit_mode() -> None:
    """Test should_rollback in autocommit mode rolls back on non-2xx."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="autocommit",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert not state.should_rollback(200)
    assert not state.should_rollback(201)

    assert state.should_rollback(300)
    assert state.should_rollback(301)
    assert state.should_rollback(400)
    assert state.should_rollback(500)


def test_should_rollback_autocommit_include_redirect_mode() -> None:
    """Test should_rollback in autocommit_include_redirect mode rolls back on non-2xx-3xx."""
    state = FlaskConfigState(
        config=None,  # type: ignore[arg-type]
        connection_key="conn",
        session_key="db",
        commit_mode="autocommit_include_redirect",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        is_async=False,
        disable_di=False,
    )

    assert not state.should_rollback(200)
    assert not state.should_rollback(300)

    assert state.should_rollback(400)
    assert state.should_rollback(500)
