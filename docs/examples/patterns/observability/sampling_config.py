from __future__ import annotations

__all__ = ("test_sampling_config",)


def test_sampling_config() -> None:
    # start-example
    from sqlspec.observability import ObservabilityConfig, SamplingConfig

    sampling = SamplingConfig(sample_rate=0.1, force_sample_on_error=True, deterministic=True)
    observability = ObservabilityConfig(sampling=sampling, print_sql=False)
    # end-example

    assert observability.sampling is sampling
