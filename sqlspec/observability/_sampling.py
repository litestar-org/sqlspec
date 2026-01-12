"""Sampling configuration for observability data.

This module provides sampling configuration to control the volume of
logs, metrics, and traces emitted by the observability system.
Sampling can significantly reduce cloud logging costs while maintaining
visibility into errors and slow queries.
"""

import random
from typing import ClassVar

__all__ = ("SamplingConfig",)


class SamplingConfig:
    """Configuration for log and metric sampling.

    Controls when observability data (logs, spans, metrics) is emitted.
    Supports both random and deterministic sampling modes, with
    force-sample conditions for errors and slow queries.

    Attributes:
        sample_rate: Probability of sampling (0.0 to 1.0). 1.0 means always sample.
        force_sample_on_error: If True, always sample when an error occurs.
        force_sample_slow_queries_ms: If set, always sample queries slower than this threshold.
        deterministic: If True, use hash-based sampling for consistency across distributed systems.

    Example:
        ```python
        # Sample 10% of requests, but always sample errors and slow queries
        config = SamplingConfig(
            sample_rate=0.1,
            force_sample_on_error=True,
            force_sample_slow_queries_ms=100.0,
            deterministic=True,
        )

        # Check if a request should be sampled
        if config.should_sample(
            correlation_id="abc-123",
            is_error=False,
            duration_ms=50.0,
        ):
            emit_logs()
        ```
    """

    __slots__ = ("deterministic", "force_sample_on_error", "force_sample_slow_queries_ms", "sample_rate")

    DEFAULT_SAMPLE_RATE: ClassVar[float] = 1.0
    """Default sample rate (100% - sample everything)."""

    DEFAULT_SLOW_QUERY_THRESHOLD_MS: ClassVar[float] = 100.0
    """Default threshold in milliseconds for slow query detection."""

    HASH_MODULUS: ClassVar[int] = 10000
    """Modulus for deterministic hash-based sampling."""

    def __init__(
        self,
        *,
        sample_rate: float = 1.0,
        force_sample_on_error: bool = True,
        force_sample_slow_queries_ms: float | None = 100.0,
        deterministic: bool = True,
    ) -> None:
        """Initialize sampling configuration.

        Args:
            sample_rate: Probability of sampling (0.0 to 1.0). Values outside
                this range are clamped. Defaults to 1.0 (always sample).
            force_sample_on_error: If True, always sample when an error occurs.
                Defaults to True.
            force_sample_slow_queries_ms: If set, always sample queries that take
                longer than this threshold in milliseconds. Defaults to 100.0ms.
                Set to None to disable.
            deterministic: If True, use hash-based sampling that produces consistent
                results for the same correlation ID across distributed systems.
                If False, use random sampling. Defaults to True.
        """
        self.sample_rate = max(0.0, min(1.0, sample_rate))
        self.force_sample_on_error = force_sample_on_error
        self.force_sample_slow_queries_ms = force_sample_slow_queries_ms
        self.deterministic = deterministic

    def should_sample(
        self,
        correlation_id: str | None = None,
        *,
        is_error: bool = False,
        duration_ms: float | None = None,
        force: bool = False,
    ) -> bool:
        """Determine if this request should be sampled.

        Evaluates force-sample conditions first (errors, slow queries, explicit force),
        then falls back to rate-based sampling.

        Args:
            correlation_id: The correlation ID for deterministic sampling.
                If None and deterministic=True, falls back to random sampling.
            is_error: Whether this request resulted in an error.
            duration_ms: Query duration in milliseconds, if known.
            force: Explicit force-sample flag from application code.

        Returns:
            True if the request should be sampled, False otherwise.

        Example:
            ```python
            # Always sampled due to error
            config.should_sample(is_error=True)  # True

            # Always sampled due to slow query (>100ms default)
            config.should_sample(duration_ms=150.0)  # True

            # Rate-based sampling
            config.should_sample(
                correlation_id="abc-123"
            )  # depends on rate
            ```
        """
        # Force sample conditions take precedence
        if force:
            return True

        if is_error and self.force_sample_on_error:
            return True

        if (
            duration_ms is not None
            and self.force_sample_slow_queries_ms is not None
            and duration_ms >= self.force_sample_slow_queries_ms
        ):
            return True

        # Rate-based sampling
        if self.sample_rate >= 1.0:
            return True

        if self.sample_rate <= 0.0:
            return False

        # Deterministic or random sampling
        if self.deterministic and correlation_id:
            # Hash-based sampling for consistency across distributed systems
            hash_value = hash(correlation_id) % self.HASH_MODULUS
            threshold = int(self.sample_rate * self.HASH_MODULUS)
            return hash_value < threshold

        # Fall back to random sampling
        return random.random() < self.sample_rate  # noqa: S311

    def copy(self) -> "SamplingConfig":
        """Return a copy of the sampling configuration.

        Returns:
            A new SamplingConfig instance with the same values.
        """
        return SamplingConfig(
            sample_rate=self.sample_rate,
            force_sample_on_error=self.force_sample_on_error,
            force_sample_slow_queries_ms=self.force_sample_slow_queries_ms,
            deterministic=self.deterministic,
        )

    def __repr__(self) -> str:
        return (
            f"SamplingConfig(sample_rate={self.sample_rate!r}, "
            f"force_sample_on_error={self.force_sample_on_error!r}, "
            f"force_sample_slow_queries_ms={self.force_sample_slow_queries_ms!r}, "
            f"deterministic={self.deterministic!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SamplingConfig):
            return NotImplemented
        return (
            self.sample_rate == other.sample_rate
            and self.force_sample_on_error == other.force_sample_on_error
            and self.force_sample_slow_queries_ms == other.force_sample_slow_queries_ms
            and self.deterministic == other.deterministic
        )

    def __hash__(self) -> int:
        # SamplingConfig is mutable, so it should not be hashable
        msg = "SamplingConfig objects are mutable and unhashable"
        raise TypeError(msg)
