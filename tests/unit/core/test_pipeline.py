# pyright: reportPrivateUsage = false
"""Unit tests for the shared statement pipeline registry."""

from unittest.mock import patch

import sqlspec.core.pipeline as pipeline_module
from sqlspec.core import SQL, get_pipeline_metrics, reset_pipeline_registry
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
