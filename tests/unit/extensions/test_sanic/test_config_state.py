"""Tests for Sanic extension configuration state."""

from unittest.mock import MagicMock

import pytest

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic import SanicConfigState, SQLSpecPlugin


def test_config_state_creation() -> None:
    """SanicConfigState should preserve configured context keys."""
    mock_config = MagicMock()

    state = SanicConfigState(
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
    assert state.sqlcommenter_framework == "sanic"


def test_config_state_rejects_conflicting_extra_statuses() -> None:
    """Extra commit and rollback statuses cannot overlap."""
    mock_config = MagicMock()

    with pytest.raises(ImproperConfigurationError) as exc_info:
        SanicConfigState(
            config=mock_config,
            connection_key="conn",
            pool_key="pool",
            session_key="session",
            commit_mode="autocommit",
            extra_commit_statuses={418},
            extra_rollback_statuses={418},
            disable_di=False,
        )

    assert "must not share" in str(exc_info.value)


def test_duplicate_context_keys_are_rejected() -> None:
    """Each configured Sanic context key should be unique across configs."""
    mock_config = MagicMock()
    plugin = SQLSpecPlugin(MagicMock(configs={}))
    state_one = SanicConfigState(
        config=mock_config,
        connection_key="db_connection",
        pool_key="db_pool",
        session_key="db_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )
    state_two = SanicConfigState(
        config=mock_config,
        connection_key="other_connection",
        pool_key="db_pool",
        session_key="other_session",
        commit_mode="manual",
        extra_commit_statuses=None,
        extra_rollback_statuses=None,
        disable_di=False,
    )
    plugin._config_states = [state_one, state_two]  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ImproperConfigurationError) as exc_info:
        plugin._validate_unique_keys()  # pyright: ignore[reportPrivateUsage]

    assert "Duplicate context keys" in str(exc_info.value)


@pytest.mark.parametrize(
    ("supports_connection_pooling", "expected_pool_key_prefix"), [(True, "db_pool"), (False, "_db_pool_")]
)
def test_pool_key_defaults_handle_pooled_and_non_pooled_configs(
    supports_connection_pooling: bool, expected_pool_key_prefix: str
) -> None:
    """Non-pooled configs should get isolated pool keys for duplicate-key validation."""
    plugin = SQLSpecPlugin(MagicMock(configs={}))
    config = MagicMock()
    config.supports_connection_pooling = supports_connection_pooling
    config.extension_config = {"sanic": {}}

    settings = plugin._extract_sanic_settings(config)  # pyright: ignore[reportPrivateUsage]

    assert settings["pool_key"].startswith(expected_pool_key_prefix)
