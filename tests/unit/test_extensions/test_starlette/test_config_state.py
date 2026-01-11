"""Test _ConfigState dataclass for Starlette extension."""

from unittest.mock import MagicMock

from sqlspec.extensions.starlette import SQLSpecConfigState


def test_config_state_creation() -> None:
    """Test _ConfigState dataclass creation."""
    mock_config = MagicMock()

    state = SQLSpecConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )

    assert state.config is mock_config
    assert state.connection_key == "db_connection"
    assert state.pool_key == "db_pool"
    assert state.session_key == "db_session"
    assert state.commit_mode == "manual"
    assert state.extra_commit_statuses is None
    assert state.extra_rollback_statuses is None


def test_config_state_with_extra_statuses() -> None:
    """Test _ConfigState with extra commit and rollback statuses."""
    mock_config = MagicMock()
    extra_commit = {201, 202}
    extra_rollback = {409, 418}

    state = SQLSpecConfigState(
        config=mock_config,
        connection_key="conn",
        pool_key="pool",
        session_key="session",
        commit_mode="autocommit",
        extra_commit_statuses=extra_commit,
        extra_rollback_statuses=extra_rollback,
        disable_di=False,
    )

    assert state.extra_commit_statuses == extra_commit
    assert state.extra_rollback_statuses == extra_rollback


def test_config_state_commit_modes() -> None:
    """Test _ConfigState with different commit modes."""
    mock_config = MagicMock()

    for mode in ["manual", "autocommit", "autocommit_include_redirect"]:
        state = SQLSpecConfigState(
            config=mock_config,
            connection_key="conn",
            pool_key="pool",
            session_key="session",
            commit_mode=mode,  # type: ignore[arg-type]
            extra_commit_statuses=None,
            extra_rollback_statuses=None,
            disable_di=False,
        )
        assert state.commit_mode == mode
