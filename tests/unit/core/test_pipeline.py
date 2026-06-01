# pyright: reportPrivateUsage = false
"""Unit tests for the shared statement pipeline registry."""

from unittest.mock import patch

import sqlspec.core.pipeline as pipeline_module
from sqlspec.core import SQL, get_pipeline_metrics, reset_pipeline_registry
from sqlspec.core.parameters import ParameterConverter, ParameterValidator
from sqlspec.core.pipeline import StatementPipelineRegistry
from sqlspec.core.statement import StatementConfig


def test_record_pipeline_metrics_constant_is_bool() -> None:
    assert isinstance(getattr(pipeline_module, "_RECORD_PIPELINE_METRICS", None), bool)


def test_os_getenv_not_called_during_compile() -> None:
    registry = StatementPipelineRegistry()
    config = StatementConfig()

    with patch("os.getenv") as mock_getenv:
        registry.compile(config, "SELECT 1", {})

    mock_getenv.assert_not_called()


def test_os_getenv_not_called_during_metrics() -> None:
    registry = StatementPipelineRegistry()

    with patch("os.getenv") as mock_getenv:
        registry.metrics()

    mock_getenv.assert_not_called()


def test_record_pipeline_metrics_patch_controls_metrics_output() -> None:
    with patch.object(pipeline_module, "_RECORD_PIPELINE_METRICS", True):
        reset_pipeline_registry()
        SQL("SELECT 42").compile()
        SQL("SELECT 42").compile()
        metrics = get_pipeline_metrics()

    reset_pipeline_registry()
    assert metrics
    assert sum(entry.get("hits", 0) for entry in metrics) >= 1


def test_fingerprint_cache_uses_cached_slot_value() -> None:
    registry = StatementPipelineRegistry()
    config = StatementConfig()

    first_fingerprint = registry._fingerprint_config(config)
    assert first_fingerprint.startswith("pipeline::")
    assert config._fingerprint_cache == first_fingerprint

    with patch("hashlib.blake2b") as mock_blake2b:
        second_fingerprint = registry._fingerprint_config(config)

    mock_blake2b.assert_not_called()
    assert second_fingerprint == first_fingerprint


def test_fingerprint_uses_config_hash_plus_unhashed_parameter_discriminators() -> None:
    """Parameter converter type and parameter-level transformer identity remain discriminators."""
    registry = StatementPipelineRegistry()

    class ConverterA(ParameterConverter):
        pass

    class ConverterB(ParameterConverter):
        pass

    def transformer_a(value: object) -> object:
        return value

    def transformer_b(value: object) -> object:
        return value

    validator = ParameterValidator()
    converter_a_config = StatementConfig(parameter_converter=ConverterA(validator), parameter_validator=validator)
    converter_b_config = StatementConfig(parameter_converter=ConverterB(validator), parameter_validator=validator)
    transformer_a_config = StatementConfig(
        parameter_config=converter_a_config.parameter_config.replace(output_transformer=transformer_a)
    )
    transformer_b_config = StatementConfig(
        parameter_config=converter_a_config.parameter_config.replace(output_transformer=transformer_b)
    )

    assert registry._fingerprint_config(converter_a_config) != registry._fingerprint_config(converter_b_config)
    assert registry._fingerprint_config(transformer_a_config) != registry._fingerprint_config(transformer_b_config)
