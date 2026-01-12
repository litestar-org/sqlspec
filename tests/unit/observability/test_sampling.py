"""Unit tests for SamplingConfig."""

from sqlspec.observability import SamplingConfig


class TestSamplingConfigDefaults:
    """Tests for default SamplingConfig values."""

    def test_default_sample_rate_is_one(self) -> None:
        """Default sample_rate should be 1.0 (sample everything)."""
        config = SamplingConfig()
        assert config.sample_rate == 1.0

    def test_default_deterministic_is_true(self) -> None:
        """Default deterministic should be True for consistent distributed tracing."""
        config = SamplingConfig()
        assert config.deterministic is True

    def test_default_force_sample_on_error_is_true(self) -> None:
        """Default force_sample_on_error should be True to always capture errors."""
        config = SamplingConfig()
        assert config.force_sample_on_error is True

    def test_default_force_sample_slow_queries_is_100ms(self) -> None:
        """Default force_sample_slow_queries_ms should be 100.0ms."""
        config = SamplingConfig()
        assert config.force_sample_slow_queries_ms == 100.0


class TestSamplingConfigRateClamping:
    """Tests for sample_rate clamping behavior."""

    def test_clamps_rate_above_one(self) -> None:
        """Sample rates above 1.0 should be clamped to 1.0."""
        config = SamplingConfig(sample_rate=1.5)
        assert config.sample_rate == 1.0

    def test_clamps_rate_below_zero(self) -> None:
        """Sample rates below 0.0 should be clamped to 0.0."""
        config = SamplingConfig(sample_rate=-0.5)
        assert config.sample_rate == 0.0

    def test_accepts_valid_rate(self) -> None:
        """Valid sample rates should be accepted as-is."""
        config = SamplingConfig(sample_rate=0.5)
        assert config.sample_rate == 0.5

    def test_accepts_boundary_rates(self) -> None:
        """Boundary values 0.0 and 1.0 should be accepted."""
        config_zero = SamplingConfig(sample_rate=0.0)
        config_one = SamplingConfig(sample_rate=1.0)
        assert config_zero.sample_rate == 0.0
        assert config_one.sample_rate == 1.0


class TestSamplingConfigShouldSample:
    """Tests for should_sample() method."""

    def test_always_samples_at_rate_one(self) -> None:
        """Should always sample when rate is 1.0."""
        config = SamplingConfig(sample_rate=1.0)
        results = [config.should_sample() for _ in range(100)]
        assert all(results)

    def test_never_samples_at_rate_zero(self) -> None:
        """Should never sample when rate is 0.0."""
        config = SamplingConfig(sample_rate=0.0)
        results = [config.should_sample() for _ in range(100)]
        assert not any(results)

    def test_force_parameter_overrides_rate(self) -> None:
        """force=True should always sample regardless of rate."""
        config = SamplingConfig(sample_rate=0.0)
        assert config.should_sample(force=True) is True

    def test_force_sample_on_error(self) -> None:
        """Should force sample when is_error=True and force_sample_on_error=True."""
        config = SamplingConfig(sample_rate=0.0, force_sample_on_error=True)
        assert config.should_sample(is_error=True) is True
        assert config.should_sample(is_error=False) is False

    def test_force_sample_slow_queries(self) -> None:
        """Should force sample slow queries exceeding threshold."""
        config = SamplingConfig(sample_rate=0.0, force_sample_slow_queries_ms=100.0)
        assert config.should_sample(duration_ms=150.0) is True
        assert config.should_sample(duration_ms=50.0) is False

    def test_force_sample_slow_queries_exact_threshold(self) -> None:
        """Should sample when duration exactly equals threshold."""
        config = SamplingConfig(sample_rate=0.0, force_sample_slow_queries_ms=100.0)
        assert config.should_sample(duration_ms=100.0) is True

    def test_force_conditions_combined(self) -> None:
        """Multiple force conditions should work together."""
        config = SamplingConfig(sample_rate=0.0, force_sample_on_error=True, force_sample_slow_queries_ms=100.0)
        assert config.should_sample(is_error=True) is True
        assert config.should_sample(duration_ms=150.0) is True
        assert config.should_sample(is_error=False, duration_ms=50.0) is False


class TestSamplingConfigDeterministic:
    """Tests for deterministic sampling."""

    def test_deterministic_sampling_consistent(self) -> None:
        """Same correlation_id should always produce same result."""
        config = SamplingConfig(sample_rate=0.5, deterministic=True)
        correlation_id = "test-correlation-id-12345"
        results = [config.should_sample(correlation_id=correlation_id) for _ in range(10)]
        assert len(set(results)) == 1

    def test_deterministic_sampling_varies_by_id(self) -> None:
        """Different correlation_ids should produce different sampling decisions."""
        config = SamplingConfig(sample_rate=0.5, deterministic=True)
        ids = [f"correlation-{i}" for i in range(100)]
        results = {config.should_sample(correlation_id=cid) for cid in ids}
        assert len(results) == 2

    def test_deterministic_requires_correlation_id(self) -> None:
        """Deterministic sampling without correlation_id falls back to random."""
        config = SamplingConfig(sample_rate=0.5, deterministic=True)
        results = {config.should_sample(correlation_id=None) for _ in range(100)}
        assert len(results) == 2

    def test_deterministic_respects_rate(self) -> None:
        """Deterministic sampling should respect sample_rate percentage."""
        config = SamplingConfig(sample_rate=0.5, deterministic=True)
        ids = [f"id-{i}" for i in range(1000)]
        sampled = sum(1 for cid in ids if config.should_sample(correlation_id=cid))
        assert 400 < sampled < 600


class TestSamplingConfigRandomSampling:
    """Tests for random (non-deterministic) sampling."""

    def test_random_sampling_varies(self) -> None:
        """Random sampling should produce varied results."""
        config = SamplingConfig(sample_rate=0.5, deterministic=False)
        results = {config.should_sample() for _ in range(100)}
        assert len(results) == 2

    def test_random_sampling_respects_rate(self) -> None:
        """Random sampling should approximately respect sample_rate."""
        config = SamplingConfig(sample_rate=0.5, deterministic=False)
        sampled = sum(1 for _ in range(1000) if config.should_sample())
        assert 400 < sampled < 600


class TestSamplingConfigCopy:
    """Tests for copy functionality."""

    def test_copy_creates_independent_instance(self) -> None:
        """Copy should create an independent instance."""
        config = SamplingConfig(sample_rate=0.5, deterministic=True)
        copied = config.copy()
        assert copied == config
        assert copied is not config

    def test_copy_preserves_all_fields(self) -> None:
        """Copy should preserve all configuration fields."""
        config = SamplingConfig(
            sample_rate=0.3, deterministic=True, force_sample_on_error=True, force_sample_slow_queries_ms=200.0
        )
        copied = config.copy()
        assert copied.sample_rate == 0.3
        assert copied.deterministic is True
        assert copied.force_sample_on_error is True
        assert copied.force_sample_slow_queries_ms == 200.0


class TestSamplingConfigEquality:
    """Tests for equality and representation."""

    def test_repr(self) -> None:
        """Should have informative repr."""
        config = SamplingConfig(sample_rate=0.5, deterministic=True)
        repr_str = repr(config)
        assert "SamplingConfig" in repr_str
        assert "0.5" in repr_str

    def test_equality_same_config(self) -> None:
        """Configs with same values should be equal."""
        config1 = SamplingConfig(sample_rate=0.5, deterministic=True)
        config2 = SamplingConfig(sample_rate=0.5, deterministic=True)
        assert config1 == config2

    def test_equality_different_config(self) -> None:
        """Configs with different values should not be equal."""
        config1 = SamplingConfig(sample_rate=0.5)
        config2 = SamplingConfig(sample_rate=0.3)
        assert config1 != config2

    def test_equality_different_type(self) -> None:
        """Comparison with non-SamplingConfig should return NotImplemented."""
        config = SamplingConfig()
        assert config.__eq__("not a config") is NotImplemented

    def test_hash_raises_type_error(self) -> None:
        """Hash should raise TypeError for mutable objects."""
        config = SamplingConfig()
        try:
            hash(config)
            raise AssertionError("Expected TypeError")
        except TypeError as e:
            assert "unhashable" in str(e)
