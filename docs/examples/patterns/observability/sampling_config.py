"""Configure deterministic trace and statement sampling with error-forced capture."""

from sqlspec.observability import ObservabilityConfig, SamplingConfig

__all__ = ("main",)


def main() -> ObservabilityConfig:
    """Create an ObservabilityConfig with 10% deterministic sampling and forced error capture."""
    sampling = SamplingConfig(sample_rate=0.1, force_sample_on_error=True, deterministic=True)
    observability = ObservabilityConfig(sampling=sampling, print_sql=False)
    assert observability.sampling is sampling
    return observability


if __name__ == "__main__":
    print(main())
