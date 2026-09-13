"""CacheConfig propagation into pipeline-owned SQLProcessor caches."""

from unittest.mock import patch

from sqlspec.core import (
    SQL,
    CacheConfig,
    get_cache_config,
    get_pipeline_metrics,
    reset_pipeline_registry,
    update_cache_config,
)
from sqlspec.core import _pipeline as pipeline_module
from tests.conftest import requires_interpreted


def _metrics_after_compiles(count: int) -> "list[dict[str, int]]":
    reset_pipeline_registry()
    for _ in range(count):
        SQL("SELECT 1").compile()
    return get_pipeline_metrics()


@requires_interpreted
def test_cache_config_sizes_reach_pipeline_processor() -> None:
    """sql_cache_size and fragment_cache_size size the processor's compiled and parse caches."""
    original = get_cache_config()
    try:
        with patch.object(pipeline_module, "_RECORD_PIPELINE_METRICS", True):
            update_cache_config(CacheConfig(sql_cache_size=7, fragment_cache_size=11))
            metrics = _metrics_after_compiles(2)
    finally:
        update_cache_config(original)
    assert len(metrics) == 1
    entry = metrics[0]
    assert entry["max_size"] == 7
    assert entry["parse_max_size"] == 11
    assert entry["parameter_max_size"] == 11
    assert entry["validator_max_size"] == 11
    assert entry["hits"] == 1


@requires_interpreted
def test_compiled_cache_disabled_zeroes_pipeline_caches() -> None:
    """compiled_cache_enabled=False disables compiled caching and empties the parse cache."""
    original = get_cache_config()
    try:
        with patch.object(pipeline_module, "_RECORD_PIPELINE_METRICS", True):
            update_cache_config(CacheConfig(compiled_cache_enabled=False))
            metrics = _metrics_after_compiles(2)
    finally:
        update_cache_config(original)
    entry = metrics[0]
    assert entry["max_size"] == 0
    assert entry["parse_max_size"] == 0
    assert entry["hits"] == 0
    assert entry["misses"] == 0
