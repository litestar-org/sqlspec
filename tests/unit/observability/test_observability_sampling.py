"""Unit tests for ObservabilityConfig sampling and cloud formatter integration."""

import pytest

from sqlspec.observability import AWSLogFormatter, GCPLogFormatter, ObservabilityConfig, SamplingConfig, StatementEvent

# =============================================================================
# ObservabilityConfig Sampling Tests
# =============================================================================


def test_observability_config_default_sampling_is_none() -> None:
    """Default sampling should be None."""
    config = ObservabilityConfig()
    assert config.sampling is None


def test_observability_config_accepts_sampling_config() -> None:
    """Should accept SamplingConfig in constructor."""
    sampling = SamplingConfig(sample_rate=0.5)
    config = ObservabilityConfig(sampling=sampling)
    assert config.sampling is sampling


def test_observability_config_copy_preserves_sampling() -> None:
    """Copy should preserve sampling configuration."""
    sampling = SamplingConfig(sample_rate=0.3, deterministic=True)
    config = ObservabilityConfig(sampling=sampling)
    copied = config.copy()
    assert copied.sampling is not None
    assert copied.sampling.sample_rate == 0.3
    assert copied.sampling.deterministic is True
    assert copied.sampling is not config.sampling


def test_observability_config_copy_with_none_sampling() -> None:
    """Copy should handle None sampling."""
    config = ObservabilityConfig(sampling=None)
    copied = config.copy()
    assert copied.sampling is None


# =============================================================================
# ObservabilityConfig Sampling Merge Tests
# =============================================================================


def test_sampling_merge_base_none_override_none() -> None:
    """Merging two None sampling configs should produce None."""
    base = ObservabilityConfig(sampling=None)
    override = ObservabilityConfig(sampling=None)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.sampling is None


def test_sampling_merge_base_sampling_override_none() -> None:
    """Base sampling should be preserved when override is None."""
    base_sampling = SamplingConfig(sample_rate=0.5)
    base = ObservabilityConfig(sampling=base_sampling)
    override = ObservabilityConfig(sampling=None)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.sampling is not None
    assert merged.sampling.sample_rate == 0.5


def test_sampling_merge_base_none_override_sampling() -> None:
    """Override sampling should be used when base is None."""
    override_sampling = SamplingConfig(sample_rate=0.3)
    base = ObservabilityConfig(sampling=None)
    override = ObservabilityConfig(sampling=override_sampling)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.sampling is not None
    assert merged.sampling.sample_rate == 0.3


def test_sampling_merge_both_sampling_configs() -> None:
    """Override sampling values should take precedence."""
    base_sampling = SamplingConfig(sample_rate=0.5, deterministic=False)
    override_sampling = SamplingConfig(sample_rate=0.2, deterministic=True)
    base = ObservabilityConfig(sampling=base_sampling)
    override = ObservabilityConfig(sampling=override_sampling)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.sampling is not None
    assert merged.sampling.sample_rate == 0.2
    assert merged.sampling.deterministic is True


def test_sampling_merge_partial_override() -> None:
    """Partial overrides should merge with base values."""
    base_sampling = SamplingConfig(
        sample_rate=0.5, deterministic=True, force_sample_on_error=True, force_sample_slow_queries_ms=100.0
    )
    override_sampling = SamplingConfig(sample_rate=0.3)
    base = ObservabilityConfig(sampling=base_sampling)
    override = ObservabilityConfig(sampling=override_sampling)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.sampling is not None
    assert merged.sampling.sample_rate == 0.3
    assert merged.sampling.deterministic is True
    assert merged.sampling.force_sample_on_error is True
    assert merged.sampling.force_sample_slow_queries_ms == 100.0


# =============================================================================
# ObservabilityConfig Sampling Equality Tests
# =============================================================================


def test_sampling_equal_with_same_sampling() -> None:
    """Configs with same sampling should be equal."""
    sampling1 = SamplingConfig(sample_rate=0.5)
    sampling2 = SamplingConfig(sample_rate=0.5)
    config1 = ObservabilityConfig(sampling=sampling1)
    config2 = ObservabilityConfig(sampling=sampling2)
    assert config1 == config2


def test_sampling_not_equal_with_different_sampling() -> None:
    """Configs with different sampling should not be equal."""
    config1 = ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.5))
    config2 = ObservabilityConfig(sampling=SamplingConfig(sample_rate=0.3))
    assert config1 != config2


def test_sampling_equal_with_both_none_sampling() -> None:
    """Configs with both None sampling should be equal."""
    config1 = ObservabilityConfig(sampling=None)
    config2 = ObservabilityConfig(sampling=None)
    assert config1 == config2


# =============================================================================
# ObservabilityConfig Sampling Repr Tests
# =============================================================================


def test_sampling_repr_includes_sampling() -> None:
    """Repr should include sampling information."""
    sampling = SamplingConfig(sample_rate=0.5)
    config = ObservabilityConfig(sampling=sampling)
    repr_str = repr(config)
    assert "sampling=" in repr_str
    assert "SamplingConfig" in repr_str


def test_sampling_repr_with_none_sampling() -> None:
    """Repr should handle None sampling."""
    config = ObservabilityConfig(sampling=None)
    repr_str = repr(config)
    assert "sampling=None" in repr_str


# =============================================================================
# StatementEvent Sampled Tests
# =============================================================================


@pytest.fixture
def base_statement_event_kwargs() -> dict:
    """Base kwargs for creating StatementEvent instances."""
    return {
        "sql": "SELECT 1",
        "parameters": None,
        "driver": "TestDriver",
        "adapter": "TestAdapter",
        "bind_key": None,
        "db_system": "test",
        "operation": "SELECT",
        "execution_mode": "single",
        "is_many": False,
        "is_script": False,
        "rows_affected": 1,
        "duration_s": 0.01,
        "started_at": 0.0,
        "correlation_id": None,
        "storage_backend": None,
        "sql_hash": None,
        "sql_truncated": False,
        "sql_original_length": None,
        "transaction_state": None,
        "prepared_statement": None,
        "trace_id": None,
        "span_id": None,
    }


def test_statement_event_default_sampled_is_true(base_statement_event_kwargs: dict) -> None:
    """Default sampled should be True."""
    event = StatementEvent(**base_statement_event_kwargs)
    assert event.sampled is True


def test_statement_event_sampled_can_be_set_false(base_statement_event_kwargs: dict) -> None:
    """Sampled can be explicitly set to False."""
    event = StatementEvent(**base_statement_event_kwargs, sampled=False)
    assert event.sampled is False


def test_statement_event_as_dict_includes_sampled(base_statement_event_kwargs: dict) -> None:
    """as_dict should include sampled field."""
    event = StatementEvent(**base_statement_event_kwargs, sampled=False)
    event_dict = event.as_dict()
    assert "sampled" in event_dict
    assert event_dict["sampled"] is False


def test_statement_event_equality_considers_sampled(base_statement_event_kwargs: dict) -> None:
    """Equality should consider sampled field."""
    event1 = StatementEvent(**base_statement_event_kwargs, sampled=True)
    event2 = StatementEvent(**base_statement_event_kwargs, sampled=False)
    assert event1 != event2


def test_statement_event_repr_includes_sampled(base_statement_event_kwargs: dict) -> None:
    """Repr should include sampled field."""
    event = StatementEvent(**base_statement_event_kwargs, sampled=False)
    repr_str = repr(event)
    assert "sampled=False" in repr_str


# =============================================================================
# ObservabilityConfig Cloud Formatter Tests
# =============================================================================


def test_cloud_formatter_default_is_none() -> None:
    """Default cloud_formatter should be None."""
    config = ObservabilityConfig()
    assert config.cloud_formatter is None


def test_cloud_formatter_accepts_gcp_formatter() -> None:
    """Should accept GCPLogFormatter in constructor."""
    formatter = GCPLogFormatter(project_id="test-project")
    config = ObservabilityConfig(cloud_formatter=formatter)
    assert config.cloud_formatter is formatter


def test_cloud_formatter_accepts_aws_formatter() -> None:
    """Should accept AWSLogFormatter in constructor."""
    formatter = AWSLogFormatter()
    config = ObservabilityConfig(cloud_formatter=formatter)
    assert config.cloud_formatter is formatter


def test_cloud_formatter_copy_preserves_formatter() -> None:
    """Copy should preserve cloud_formatter."""
    formatter = GCPLogFormatter(project_id="test-project")
    config = ObservabilityConfig(cloud_formatter=formatter)
    copied = config.copy()
    assert copied.cloud_formatter is formatter


def test_cloud_formatter_copy_with_none() -> None:
    """Copy should handle None cloud_formatter."""
    config = ObservabilityConfig(cloud_formatter=None)
    copied = config.copy()
    assert copied.cloud_formatter is None


# =============================================================================
# ObservabilityConfig Cloud Formatter Merge Tests
# =============================================================================


def test_cloud_formatter_merge_base_none_override_none() -> None:
    """Merging two None cloud_formatter should produce None."""
    base = ObservabilityConfig(cloud_formatter=None)
    override = ObservabilityConfig(cloud_formatter=None)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.cloud_formatter is None


def test_cloud_formatter_merge_base_formatter_override_none() -> None:
    """Base cloud_formatter should be preserved when override is None."""
    formatter = GCPLogFormatter(project_id="test-project")
    base = ObservabilityConfig(cloud_formatter=formatter)
    override = ObservabilityConfig(cloud_formatter=None)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.cloud_formatter is formatter


def test_cloud_formatter_merge_base_none_override_formatter() -> None:
    """Override cloud_formatter should be used when base is None."""
    formatter = AWSLogFormatter()
    base = ObservabilityConfig(cloud_formatter=None)
    override = ObservabilityConfig(cloud_formatter=formatter)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.cloud_formatter is formatter


def test_cloud_formatter_merge_override_replaces_base() -> None:
    """Override cloud_formatter should replace base."""
    base_formatter = GCPLogFormatter(project_id="base-project")
    override_formatter = AWSLogFormatter()
    base = ObservabilityConfig(cloud_formatter=base_formatter)
    override = ObservabilityConfig(cloud_formatter=override_formatter)
    merged = ObservabilityConfig.merge(base, override)
    assert merged.cloud_formatter is override_formatter


# =============================================================================
# ObservabilityConfig Cloud Formatter Equality Tests
# =============================================================================


def test_cloud_formatter_equal_with_same_formatter() -> None:
    """Configs with same cloud_formatter should be equal."""
    formatter = GCPLogFormatter(project_id="test-project")
    config1 = ObservabilityConfig(cloud_formatter=formatter)
    config2 = ObservabilityConfig(cloud_formatter=formatter)
    assert config1 == config2


def test_cloud_formatter_not_equal_with_different_formatter() -> None:
    """Configs with different cloud_formatter should not be equal."""
    config1 = ObservabilityConfig(cloud_formatter=GCPLogFormatter(project_id="project-1"))
    config2 = ObservabilityConfig(cloud_formatter=GCPLogFormatter(project_id="project-2"))
    assert config1 != config2


def test_cloud_formatter_equal_with_both_none() -> None:
    """Configs with both None cloud_formatter should be equal."""
    config1 = ObservabilityConfig(cloud_formatter=None)
    config2 = ObservabilityConfig(cloud_formatter=None)
    assert config1 == config2


# =============================================================================
# ObservabilityConfig Cloud Formatter Repr Tests
# =============================================================================


def test_cloud_formatter_repr_includes_formatter() -> None:
    """Repr should include cloud_formatter information."""
    formatter = GCPLogFormatter(project_id="test-project")
    config = ObservabilityConfig(cloud_formatter=formatter)
    repr_str = repr(config)
    assert "cloud_formatter=" in repr_str
    assert "GCPLogFormatter" in repr_str


def test_cloud_formatter_repr_with_none() -> None:
    """Repr should handle None cloud_formatter."""
    config = ObservabilityConfig(cloud_formatter=None)
    repr_str = repr(config)
    assert "cloud_formatter=None" in repr_str
