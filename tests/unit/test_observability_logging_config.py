"""Unit tests for observability logging configuration."""

import pytest

from sqlspec.observability import LoggingConfig


def test_logging_config_defaults() -> None:
    config = LoggingConfig()
    assert config.include_sql_hash is True
    assert config.sql_truncation_length == 2000
    assert config.parameter_truncation_count == 100
    assert config.include_trace_context is True


def test_logging_config_custom_values() -> None:
    config = LoggingConfig(
        include_sql_hash=False, sql_truncation_length=512, parameter_truncation_count=25, include_trace_context=False
    )
    assert config.include_sql_hash is False
    assert config.sql_truncation_length == 512
    assert config.parameter_truncation_count == 25
    assert config.include_trace_context is False


def test_logging_config_copy_and_equality() -> None:
    config = LoggingConfig(
        include_sql_hash=False, sql_truncation_length=128, parameter_truncation_count=10, include_trace_context=False
    )
    clone = config.copy()
    assert clone == config
    assert clone is not config


def test_logging_config_unhashable() -> None:
    config = LoggingConfig()
    with pytest.raises(TypeError):
        hash(config)
