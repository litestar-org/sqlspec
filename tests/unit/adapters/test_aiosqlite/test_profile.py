"""Unit tests for aiosqlite profile parity with sqlite."""

from sqlspec.adapters.aiosqlite.core import (
    build_profile,
    build_statement_config,
    default_statement_config,
    driver_profile,
)
from sqlspec.core import ParameterStyle


def test_build_profile_includes_named_colon_in_supported_styles() -> None:
    profile = build_profile()
    assert ParameterStyle.NAMED_COLON in profile.supported_styles


def test_build_profile_includes_named_colon_in_supported_execution_styles() -> None:
    profile = build_profile()
    assert ParameterStyle.NAMED_COLON in profile.supported_execution_styles


def test_build_profile_default_style_remains_qmark() -> None:
    profile = build_profile()
    assert profile.default_style == ParameterStyle.QMARK
    assert profile.default_execution_style == ParameterStyle.QMARK


def test_build_profile_qmark_still_supported() -> None:
    profile = build_profile()
    assert ParameterStyle.QMARK in profile.supported_styles
    assert ParameterStyle.QMARK in profile.supported_execution_styles


def test_build_statement_config_disables_parameter_type_wrapping() -> None:
    config = build_statement_config()
    assert config.enable_parameter_type_wrapping is False


def test_default_statement_config_disables_parameter_type_wrapping() -> None:
    assert default_statement_config.enable_parameter_type_wrapping is False


def test_driver_profile_module_singleton_has_named_colon() -> None:
    assert ParameterStyle.NAMED_COLON in driver_profile.supported_styles
    assert ParameterStyle.NAMED_COLON in driver_profile.supported_execution_styles


def test_aiosqlite_profile_parity_with_sqlite_profile() -> None:
    from sqlspec.adapters.sqlite.core import build_profile as sqlite_build_profile

    aio_profile = build_profile()
    sqlite_profile = sqlite_build_profile()

    assert aio_profile.supported_styles == sqlite_profile.supported_styles
    assert aio_profile.supported_execution_styles == sqlite_profile.supported_execution_styles
    assert aio_profile.default_style == sqlite_profile.default_style
    assert aio_profile.default_execution_style == sqlite_profile.default_execution_style


def test_aiosqlite_statement_config_parity_with_sqlite() -> None:
    from sqlspec.adapters.sqlite.core import build_statement_config as sqlite_build_statement_config

    aio_config = build_statement_config()
    sqlite_config = sqlite_build_statement_config()

    assert aio_config.enable_parameter_type_wrapping == sqlite_config.enable_parameter_type_wrapping
