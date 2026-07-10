"""Tests for shared framework extension helpers."""

from dataclasses import fields

import pytest

from sqlspec.extensions._framework_common import should_commit, should_rollback
from sqlspec.extensions.flask._state import FlaskConfigState
from sqlspec.extensions.sanic._state import SanicConfigState
from sqlspec.extensions.starlette._state import SQLSpecConfigState

_COMMON_FIELDS = [
    "config",
    "connection_key",
    "pool_key",
    "session_key",
    "commit_mode",
    "extra_commit_statuses",
    "extra_rollback_statuses",
    "disable_di",
    "enable_correlation_middleware",
    "correlation_header",
    "correlation_headers",
    "auto_trace_headers",
    "enable_sqlcommenter_middleware",
]


def test_starlette_config_state_field_order() -> None:
    """SQLSpecConfigState keeps its historical field set and order."""
    assert [f.name for f in fields(SQLSpecConfigState)] == [*_COMMON_FIELDS, "sqlcommenter_framework"]


def test_sanic_config_state_field_order() -> None:
    """SanicConfigState keeps its historical field set and order."""
    assert [f.name for f in fields(SanicConfigState)] == [*_COMMON_FIELDS, "sqlcommenter_framework"]


def test_flask_config_state_field_order() -> None:
    """FlaskConfigState keeps its historical field set and order."""
    assert [f.name for f in fields(FlaskConfigState)] == [
        "config",
        "connection_key",
        "session_key",
        "commit_mode",
        "extra_commit_statuses",
        "extra_rollback_statuses",
        "is_async",
        "disable_di",
        "enable_correlation_middleware",
        "correlation_header",
        "correlation_headers",
        "auto_trace_headers",
        "enable_sqlcommenter_middleware",
    ]


def test_sqlcommenter_framework_defaults() -> None:
    """Each subclass keeps its framework-specific sqlcommenter default."""
    starlette_default = next(f for f in fields(SQLSpecConfigState) if f.name == "sqlcommenter_framework").default
    sanic_default = next(f for f in fields(SanicConfigState) if f.name == "sqlcommenter_framework").default
    assert starlette_default == "starlette"
    assert sanic_default == "sanic"


@pytest.mark.parametrize("status_code", [200, 204, 299, 300, 302, 399, 400, 500])
def test_should_commit_manual_mode_never_commits(status_code: int) -> None:
    """Manual mode never commits regardless of status code."""
    assert not should_commit(status_code, "manual", None, None)


def test_should_commit_autocommit_mode_commits_2xx_only() -> None:
    """Autocommit mode commits on 2xx and rolls back everything else."""
    assert should_commit(200, "autocommit", None, None)
    assert should_commit(201, "autocommit", None, None)
    assert should_commit(299, "autocommit", None, None)

    assert not should_commit(300, "autocommit", None, None)
    assert not should_commit(302, "autocommit", None, None)
    assert not should_commit(400, "autocommit", None, None)
    assert not should_commit(500, "autocommit", None, None)


def test_should_commit_autocommit_include_redirect_mode_commits_2xx_3xx() -> None:
    """Redirect-inclusive autocommit commits on 2xx-3xx."""
    assert should_commit(200, "autocommit_include_redirect", None, None)
    assert should_commit(300, "autocommit_include_redirect", None, None)
    assert should_commit(302, "autocommit_include_redirect", None, None)
    assert should_commit(399, "autocommit_include_redirect", None, None)

    assert not should_commit(400, "autocommit_include_redirect", None, None)
    assert not should_commit(500, "autocommit_include_redirect", None, None)


@pytest.mark.parametrize("commit_mode", ["manual", "autocommit", "autocommit_include_redirect"])
def test_should_commit_extra_commit_statuses_override(commit_mode: str) -> None:
    """Extra commit statuses force a commit in every mode."""
    assert should_commit(404, commit_mode, {404}, None)
    assert should_commit(500, commit_mode, {500}, None)


@pytest.mark.parametrize("commit_mode", ["manual", "autocommit", "autocommit_include_redirect"])
def test_should_commit_extra_rollback_statuses_override(commit_mode: str) -> None:
    """Extra rollback statuses force a rollback in every mode."""
    assert not should_commit(200, commit_mode, None, {200})
    assert not should_commit(201, commit_mode, None, {201})


def test_should_commit_extra_commit_takes_precedence_over_extra_rollback() -> None:
    """Extra commit statuses are checked before extra rollback statuses."""
    assert should_commit(404, "autocommit", {404}, {418})
    assert not should_commit(418, "autocommit", {404}, {418})


@pytest.mark.parametrize("status_code", [200, 300, 400, 500])
def test_should_rollback_manual_mode_never_rolls_back(status_code: int) -> None:
    """Manual mode never rolls back regardless of status code."""
    assert not should_rollback(status_code, "manual", None, None)


def test_should_rollback_autocommit_mode_inverts_commit() -> None:
    """Autocommit mode rolls back any status that does not commit."""
    assert not should_rollback(200, "autocommit", None, None)
    assert not should_rollback(299, "autocommit", None, None)

    assert should_rollback(300, "autocommit", None, None)
    assert should_rollback(400, "autocommit", None, None)
    assert should_rollback(500, "autocommit", None, None)


def test_should_rollback_autocommit_include_redirect_mode_inverts_commit() -> None:
    """Redirect-inclusive autocommit rolls back only 4xx and above."""
    assert not should_rollback(200, "autocommit_include_redirect", None, None)
    assert not should_rollback(302, "autocommit_include_redirect", None, None)

    assert should_rollback(400, "autocommit_include_redirect", None, None)
    assert should_rollback(500, "autocommit_include_redirect", None, None)


def test_should_rollback_respects_extra_statuses() -> None:
    """Extra status overrides feed through to the rollback decision."""
    assert not should_rollback(404, "autocommit", {404}, None)
    assert should_rollback(200, "autocommit", None, {200})
