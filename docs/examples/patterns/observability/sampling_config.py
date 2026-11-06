"""Configure deterministic trace and statement sampling with error-forced capture."""

# --8<-- [start:example]
from sqlspec.observability import ObservabilityConfig, SamplingConfig

sampling = SamplingConfig(sample_rate=0.1, force_sample_on_error=True)
observability = ObservabilityConfig(sampling=sampling, print_sql=False)
# --8<-- [end:example]

__all__ = ("main", "observability", "sampling")


def main() -> ObservabilityConfig:
    """Create an ObservabilityConfig with 10% deterministic sampling and forced error capture."""
    assert observability.sampling is sampling
    assert sampling.deterministic is True
    return observability


if __name__ == "__main__":
    print(main())
