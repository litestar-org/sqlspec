"""Tests for Sanic extension configuration state."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sqlspec.core import CorrelationExtractor
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.sanic import SanicConfigState, SQLSpecPlugin
from sqlspec.utils.correlation import CorrelationContext


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
    plugin._config_states = [state_one, state_two]
    with pytest.raises(ImproperConfigurationError) as exc_info:
        plugin._ensure_unique_keys()
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
    settings = plugin._extract_extension_settings(config)
    assert settings["pool_key"].startswith(expected_pool_key_prefix)


pytestmark = pytest.mark.anyio


def _make_config(*, enable_correlation: bool = False) -> MagicMock:
    config = MagicMock()
    config.supports_connection_pooling = True
    config.extension_config = {
        "sanic": {
            "disable_di": True,
            "enable_correlation_middleware": enable_correlation,
            "enable_sqlcommenter_middleware": False,
        }
    }
    return config


def _make_request(correlation_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(ctx=SimpleNamespace()),
        ctx=SimpleNamespace(),
        endpoint=None,
        headers={"x-request-id": correlation_id},
        path="/test",
        uri_template=None,
    )


def test_extractor_init_sanic_plugin_extractor_is_none_when_correlation_disabled() -> None:
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=False)}))
    assert plugin._extractor is None


def test_extractor_init_sanic_plugin_builds_extractor_when_correlation_enabled() -> None:
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=True)}))
    assert isinstance(plugin._extractor, CorrelationExtractor)


async def test_extractor_init_sanic_plugin_reuses_same_extractor_across_requests() -> None:
    plugin = SQLSpecPlugin(MagicMock(configs={"default": _make_config(enable_correlation=True)}))
    extractor_id = id(plugin._extractor)
    try:
        for index in range(5):
            request = _make_request(f"request-{index}")
            plugin._set_correlation_context(request)
            assert CorrelationContext.get() == f"request-{index}"
            assert id(plugin._extractor) == extractor_id
            plugin._restore_correlation_context(request, SimpleNamespace(headers={}))
    finally:
        CorrelationContext.clear()
