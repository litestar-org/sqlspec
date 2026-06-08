"""Unit tests for SamplingConfig."""

from typing import Any, cast

from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime, SamplingConfig, StatementEvent
from sqlspec.observability._config import StatementObserver
from sqlspec.utils.correlation import CorrelationContext


def test_sampling_config_defaults_default_sample_rate_is_one() -> None:
    """Default sample_rate should be 1.0 (sample everything)."""
    config = SamplingConfig()
    assert config.sample_rate == 1.0


def test_sampling_config_defaults_default_deterministic_is_true() -> None:
    """Default deterministic should be True for consistent distributed tracing."""
    config = SamplingConfig()
    assert config.deterministic is True


def test_sampling_config_defaults_default_force_sample_on_error_is_true() -> None:
    """Default force_sample_on_error should be True to always capture errors."""
    config = SamplingConfig()
    assert config.force_sample_on_error is True


def test_sampling_config_defaults_default_force_sample_slow_queries_is_100ms() -> None:
    """Default force_sample_slow_queries_ms should be 100.0ms."""
    config = SamplingConfig()
    assert config.force_sample_slow_queries_ms == 100.0


def test_sampling_config_rate_clamping_clamps_rate_above_one() -> None:
    """Sample rates above 1.0 should be clamped to 1.0."""
    config = SamplingConfig(sample_rate=1.5)
    assert config.sample_rate == 1.0


def test_sampling_config_rate_clamping_clamps_rate_below_zero() -> None:
    """Sample rates below 0.0 should be clamped to 0.0."""
    config = SamplingConfig(sample_rate=-0.5)
    assert config.sample_rate == 0.0


def test_sampling_config_rate_clamping_accepts_valid_rate() -> None:
    """Valid sample rates should be accepted as-is."""
    config = SamplingConfig(sample_rate=0.5)
    assert config.sample_rate == 0.5


def test_sampling_config_rate_clamping_accepts_boundary_rates() -> None:
    """Boundary values 0.0 and 1.0 should be accepted."""
    config_zero = SamplingConfig(sample_rate=0.0)
    config_one = SamplingConfig(sample_rate=1.0)
    assert config_zero.sample_rate == 0.0
    assert config_one.sample_rate == 1.0


def test_sampling_config_should_sample_always_samples_at_rate_one() -> None:
    """Should always sample when rate is 1.0."""
    config = SamplingConfig(sample_rate=1.0)
    results = [config.should_sample() for _ in range(100)]
    assert all(results)


def test_sampling_config_should_sample_never_samples_at_rate_zero() -> None:
    """Should never sample when rate is 0.0."""
    config = SamplingConfig(sample_rate=0.0)
    results = [config.should_sample() for _ in range(100)]
    assert not any(results)


def test_sampling_config_should_sample_force_parameter_overrides_rate() -> None:
    """force=True should always sample regardless of rate."""
    config = SamplingConfig(sample_rate=0.0)
    assert config.should_sample(force=True) is True


def test_sampling_config_should_sample_force_sample_on_error() -> None:
    """Should force sample when is_error=True and force_sample_on_error=True."""
    config = SamplingConfig(sample_rate=0.0, force_sample_on_error=True)
    assert config.should_sample(is_error=True) is True
    assert config.should_sample(is_error=False) is False


def test_sampling_config_should_sample_force_sample_slow_queries() -> None:
    """Should force sample slow queries exceeding threshold."""
    config = SamplingConfig(sample_rate=0.0, force_sample_slow_queries_ms=100.0)
    assert config.should_sample(duration_ms=150.0) is True
    assert config.should_sample(duration_ms=50.0) is False


def test_sampling_config_should_sample_force_sample_slow_queries_exact_threshold() -> None:
    """Should sample when duration exactly equals threshold."""
    config = SamplingConfig(sample_rate=0.0, force_sample_slow_queries_ms=100.0)
    assert config.should_sample(duration_ms=100.0) is True


def test_sampling_config_should_sample_force_conditions_combined() -> None:
    """Multiple force conditions should work together."""
    config = SamplingConfig(sample_rate=0.0, force_sample_on_error=True, force_sample_slow_queries_ms=100.0)
    assert config.should_sample(is_error=True) is True
    assert config.should_sample(duration_ms=150.0) is True
    assert config.should_sample(is_error=False, duration_ms=50.0) is False


def test_sampling_config_deterministic_deterministic_sampling_consistent() -> None:
    """Same correlation_id should always produce same result."""
    config = SamplingConfig(sample_rate=0.5, deterministic=True)
    correlation_id = "test-correlation-id-12345"
    results = [config.should_sample(correlation_id=correlation_id) for _ in range(10)]
    assert len(set(results)) == 1


def test_sampling_config_deterministic_deterministic_sampling_varies_by_id() -> None:
    """Different correlation_ids should produce different sampling decisions."""
    config = SamplingConfig(sample_rate=0.5, deterministic=True)
    ids = [f"correlation-{i}" for i in range(100)]
    results = {config.should_sample(correlation_id=cid) for cid in ids}
    assert len(results) == 2


def test_sampling_config_deterministic_deterministic_requires_correlation_id() -> None:
    """Deterministic sampling without correlation_id falls back to random."""
    config = SamplingConfig(sample_rate=0.5, deterministic=True)
    results = {config.should_sample(correlation_id=None) for _ in range(100)}
    assert len(results) == 2


def test_sampling_config_deterministic_deterministic_respects_rate() -> None:
    """Deterministic sampling should respect sample_rate percentage."""
    config = SamplingConfig(sample_rate=0.5, deterministic=True)
    ids = [f"id-{i}" for i in range(1000)]
    sampled = sum(1 for cid in ids if config.should_sample(correlation_id=cid))
    assert 400 < sampled < 600


def test_sampling_config_random_sampling_random_sampling_varies() -> None:
    """Random sampling should produce varied results."""
    config = SamplingConfig(sample_rate=0.5, deterministic=False)
    results = {config.should_sample() for _ in range(100)}
    assert len(results) == 2


def test_sampling_config_random_sampling_random_sampling_respects_rate() -> None:
    """Random sampling should approximately respect sample_rate."""
    config = SamplingConfig(sample_rate=0.5, deterministic=False)
    sampled = sum(1 for _ in range(1000) if config.should_sample())
    assert 400 < sampled < 600


def test_sampling_config_copy_copy_creates_independent_instance() -> None:
    """Copy should create an independent instance."""
    config = SamplingConfig(sample_rate=0.5, deterministic=True)
    copied = config.copy()
    assert copied == config
    assert copied is not config


def test_sampling_config_copy_copy_preserves_all_fields() -> None:
    """Copy should preserve all configuration fields."""
    config = SamplingConfig(
        sample_rate=0.3, deterministic=True, force_sample_on_error=True, force_sample_slow_queries_ms=200.0
    )
    copied = config.copy()
    assert copied.sample_rate == 0.3
    assert copied.deterministic is True
    assert copied.force_sample_on_error is True
    assert copied.force_sample_slow_queries_ms == 200.0


def test_sampling_config_equality_repr() -> None:
    """Should have informative repr."""
    config = SamplingConfig(sample_rate=0.5, deterministic=True)
    repr_str = repr(config)
    assert "SamplingConfig" in repr_str
    assert "0.5" in repr_str


def test_sampling_config_equality_equality_same_config() -> None:
    """Configs with same values should be equal."""
    config1 = SamplingConfig(sample_rate=0.5, deterministic=True)
    config2 = SamplingConfig(sample_rate=0.5, deterministic=True)
    assert config1 == config2


def test_sampling_config_equality_equality_different_config() -> None:
    """Configs with different values should not be equal."""
    config1 = SamplingConfig(sample_rate=0.5)
    config2 = SamplingConfig(sample_rate=0.3)
    assert config1 != config2


def test_sampling_config_equality_equality_different_type() -> None:
    """Comparison with non-SamplingConfig should return NotImplemented."""
    config = SamplingConfig()
    assert config.__eq__("not a config") is NotImplemented


def test_sampling_config_equality_hash_raises_type_error() -> None:
    """Hash should raise TypeError for mutable objects."""
    config = SamplingConfig()
    try:
        hash(config)
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "unhashable" in str(e)


def _emit_statement(runtime: ObservabilityRuntime, *, duration_s: float = 0.001) -> None:
    runtime.emit_statement_event(
        sql="SELECT 1",
        parameters=None,
        driver="DummyDriver",
        operation="SELECT",
        execution_mode="single",
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=duration_s,
        storage_backend=None,
    )


def test_runtime_sampling_emit_statement_event_skips_observers_when_sampling_rejects() -> None:
    observed: list[StatementEvent] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(
            sampling=SamplingConfig(sample_rate=0.0, force_sample_on_error=False, force_sample_slow_queries_ms=None),
            statement_observers=(observed.append,),
        )
    )
    _emit_statement(runtime)
    assert observed == []


def test_runtime_sampling_emit_statement_event_marks_sampled_when_sampling_accepts() -> None:
    observed: list[StatementEvent] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(sampling=SamplingConfig(sample_rate=1.0), statement_observers=(observed.append,))
    )
    _emit_statement(runtime)
    assert len(observed) == 1
    assert observed[0].sampled is True


def test_runtime_sampling_emit_statement_event_force_samples_slow_query() -> None:
    observed: list[StatementEvent] = []
    runtime = ObservabilityRuntime(
        ObservabilityConfig(
            sampling=SamplingConfig(sample_rate=0.0, force_sample_slow_queries_ms=10.0),
            statement_observers=(observed.append,),
        )
    )
    _emit_statement(runtime, duration_s=0.1)
    assert len(observed) == 1
    assert observed[0].sampled is True


def test_runtime_sampling_emit_statement_event_without_sampling_preserves_previous_behavior() -> None:
    observed: list[dict[str, Any]] = []

    def observer(event: StatementEvent) -> None:
        observed.append(event.as_dict())

    runtime = ObservabilityRuntime(
        ObservabilityConfig(sampling=None, statement_observers=(cast(StatementObserver, observer),))
    )
    with CorrelationContext.context("sample-correlation"):
        _emit_statement(runtime)
    assert len(observed) == 1
    assert observed[0]["correlation_id"] == "sample-correlation"
    assert observed[0]["sampled"] is True
