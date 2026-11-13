"""Weighted replica selection and passive single-flight circuit breaking."""

# ruff: noqa: S311

import random
import threading
import time
from collections.abc import Callable, Sequence
from math import isfinite
from typing import Any, Literal, Protocol, TypeAlias, final

from mypy_extensions import mypyc_attr

from sqlspec.exceptions import ImproperConfigurationError

__all__ = (
    "RandomSelector",
    "ReplicaHealthBreaker",
    "ReplicaSelector",
    "ReplicaSpec",
    "RoundRobinSelector",
    "SelectionStrategy",
    "build_selector",
)

SelectionStrategy: TypeAlias = Literal["round_robin", "random"]


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class ReplicaSpec:
    """Weighted replica configuration with an optional declared staleness bound."""

    __slots__ = ("config", "max_staleness_seconds", "name", "weight")

    def __init__(
        self, config: Any, *, weight: int = 1, name: str | None = None, max_staleness_seconds: float | None = None
    ) -> None:
        """Validate and retain a replica's configuration and selection metadata."""
        if not isinstance(weight, int) or isinstance(weight, bool) or weight < 1:
            msg = "Replica weight must be a positive integer"
            raise ImproperConfigurationError(msg)
        if max_staleness_seconds is not None and (not isfinite(max_staleness_seconds) or max_staleness_seconds < 0):
            msg = "Replica staleness must be finite and nonnegative"
            raise ImproperConfigurationError(msg)
        self.config = config
        self.weight = weight
        self.name = name
        self.max_staleness_seconds = max_staleness_seconds


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class ReplicaHealthBreaker:
    """Passive circuit breaker reserving at most one expired-open probe."""

    __slots__ = (
        "_clock",
        "_failure_counts",
        "_failure_threshold",
        "_half_open_in_flight",
        "_lock",
        "_opened_until",
        "_recovery_seconds",
    )

    def __init__(
        self,
        replica_count: int,
        *,
        failure_threshold: int = 3,
        recovery_seconds: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Configure consecutive failure and monotonic recovery thresholds."""
        if (
            isinstance(replica_count, bool)
            or replica_count < 1
            or isinstance(failure_threshold, bool)
            or failure_threshold < 1
        ):
            msg = "Replica count and failure threshold must be positive"
            raise ImproperConfigurationError(msg)
        if not isfinite(recovery_seconds) or recovery_seconds <= 0:
            msg = "Recovery duration must be finite and positive"
            raise ImproperConfigurationError(msg)
        self._clock = clock
        self._failure_threshold = failure_threshold
        self._recovery_seconds = recovery_seconds
        self._failure_counts = [0] * replica_count
        self._opened_until = [0.0] * replica_count
        self._half_open_in_flight = [False] * replica_count
        self._lock = threading.Lock()

    def is_available(self, index: int) -> bool:
        """Observe candidate eligibility without reserving a recovery probe."""
        with self._lock:
            return self._failure_counts[index] < self._failure_threshold or (
                self._clock() >= self._opened_until[index] and not self._half_open_in_flight[index]
            )

    def try_acquire_probe(self, index: int) -> bool:
        """Claim an expired-open probe immediately before native checkout."""
        with self._lock:
            if self._failure_counts[index] < self._failure_threshold:
                return True
            if self._clock() < self._opened_until[index] or self._half_open_in_flight[index]:
                return False
            self._half_open_in_flight[index] = True
            return True

    def release_probe(self, index: int) -> None:
        """Release a cancelled or unused recovery claim without recording health."""
        with self._lock:
            self._half_open_in_flight[index] = False

    def record_success(self, index: int) -> None:
        """Close a replica's circuit after successful checkout."""
        with self._lock:
            self._failure_counts[index] = 0
            self._opened_until[index] = 0.0
            self._half_open_in_flight[index] = False

    def record_failure(self, index: int) -> None:
        """Count a checkout failure and reopen a failed recovery probe."""
        with self._lock:
            self._failure_counts[index] += 1
            if self._failure_counts[index] >= self._failure_threshold:
                self._opened_until[index] = self._clock() + self._recovery_seconds
            self._half_open_in_flight[index] = False

    def reset(self) -> None:
        """Reset every replica's passive health state."""
        with self._lock:
            for index in range(len(self._failure_counts)):
                self._failure_counts[index] = 0
                self._opened_until[index] = 0.0
                self._half_open_in_flight[index] = False


class ReplicaSelector(Protocol):
    """Ordered replica candidates with observational health filtering."""

    @property
    def specs(self) -> tuple[ReplicaSpec, ...]:
        """Return replica descriptors in their original index order."""
        ...

    def select_order(
        self, breaker: ReplicaHealthBreaker | None = None, max_staleness_seconds: float | None = None
    ) -> list[int]:
        """Return eligible candidate indices without repetition or probe claims."""
        ...


def _eligible(
    specs: tuple[ReplicaSpec, ...], breaker: ReplicaHealthBreaker | None, max_staleness_seconds: float | None
) -> list[int]:
    return [
        index
        for index, spec in enumerate(specs)
        if (breaker is None or breaker.is_available(index))
        and (
            max_staleness_seconds is None
            or spec.max_staleness_seconds is None
            or spec.max_staleness_seconds <= max_staleness_seconds
        )
    ]


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class RoundRobinSelector:
    """Smooth weighted round-robin using storage proportional to replica count."""

    __slots__ = ("_current_weights", "_lock", "_specs", "_total_weight")

    def __init__(self, specs: Sequence[ReplicaSpec]) -> None:
        """Configure a nonempty, fixed replica sequence."""
        if not specs:
            msg = "At least one replica is required"
            raise ImproperConfigurationError(msg)
        self._specs = tuple(specs)
        self._current_weights = [0] * len(specs)
        self._total_weight = sum(spec.weight for spec in specs)
        self._lock = threading.Lock()

    @property
    def specs(self) -> tuple[ReplicaSpec, ...]:
        """Return configured replica descriptors."""
        return self._specs

    def select_order(
        self, breaker: ReplicaHealthBreaker | None = None, max_staleness_seconds: float | None = None
    ) -> list[int]:
        """Pick a weighted first choice followed by unique eligible failovers.

        Replicas without a declared staleness bound remain eligible.
        """
        eligible = _eligible(self._specs, breaker, max_staleness_seconds)
        if not eligible:
            return []
        with self._lock:
            winner = eligible[0]
            total = 0
            for index in eligible:
                weight = self._specs[index].weight
                self._current_weights[index] += weight
                total += weight
                if self._current_weights[index] > self._current_weights[winner]:
                    winner = index
            self._current_weights[winner] -= total
        return [winner, *(index for index in eligible if index != winner)]


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class RandomSelector:
    """Weighted random ordering without replacement."""

    __slots__ = ("_lock", "_rng", "_specs", "_weights")

    def __init__(self, specs: Sequence[ReplicaSpec], *, seed: int | None = None) -> None:
        """Configure replicas and an optional deterministic random seed."""
        if not specs:
            msg = "At least one replica is required"
            raise ImproperConfigurationError(msg)
        self._specs = tuple(specs)
        self._weights = [spec.weight for spec in specs]
        self._rng = random.Random(seed)
        self._lock = threading.Lock()

    @property
    def specs(self) -> tuple[ReplicaSpec, ...]:
        """Return configured replica descriptors."""
        return self._specs

    def select_order(
        self, breaker: ReplicaHealthBreaker | None = None, max_staleness_seconds: float | None = None
    ) -> list[int]:
        """Order each healthy, staleness-eligible index exactly once."""
        candidates = _eligible(self._specs, breaker, max_staleness_seconds)
        result: list[int] = []
        with self._lock:
            while candidates:
                index = self._rng.choices(candidates, weights=[self._weights[i] for i in candidates], k=1)[0]
                result.append(index)
                candidates.remove(index)
        return result


def build_selector(strategy: SelectionStrategy | ReplicaSelector, specs: Sequence[ReplicaSpec]) -> ReplicaSelector:
    """Build a named selector or retain a caller-supplied selector implementation."""
    if not specs:
        msg = "At least one replica is required"
        raise ImproperConfigurationError(msg)
    if not isinstance(strategy, str):
        return strategy
    if strategy == "round_robin":
        return RoundRobinSelector(specs)
    if strategy == "random":
        return RandomSelector(specs)
    msg = f"Unknown replica selection strategy {strategy!r}"
    raise ImproperConfigurationError(msg)
