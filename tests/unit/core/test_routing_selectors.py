"""Behavioral selection and passive health recovery contracts."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast

import pytest

from sqlspec.core.routing import (
    RandomSelector,
    ReplicaHealthBreaker,
    ReplicaSpec,
    RoundRobinSelector,
    SelectionStrategy,
    build_selector,
)
from sqlspec.exceptions import ImproperConfigurationError


def test_smooth_weighting_and_unique_failover_order() -> None:
    selector = RoundRobinSelector([ReplicaSpec("a", weight=4), ReplicaSpec("b", weight=2)])
    orders = [selector.select_order() for _ in range(60)]
    assert Counter(order[0] for order in orders) == {0: 40, 1: 20}
    assert all(sorted(order) == [0, 1] for order in orders)
    large = RoundRobinSelector([ReplicaSpec("a", weight=10**12), ReplicaSpec("b")])
    assert sorted(large.select_order()) == [0, 1]


def test_random_weighted_order_without_replacement() -> None:
    selector = RandomSelector([ReplicaSpec("a", weight=3), ReplicaSpec("b")], seed=42)
    orders = [selector.select_order() for _ in range(400)]
    assert 260 <= sum(order[0] == 0 for order in orders) <= 340
    assert all(sorted(order) == [0, 1] for order in orders)


def test_half_open_probe_is_single_flight_and_order_does_not_reserve() -> None:
    now = [100.0]
    breaker = ReplicaHealthBreaker(2, failure_threshold=2, recovery_seconds=5, clock=lambda: now[0])
    for index in range(2):
        breaker.record_failure(index)
        breaker.record_failure(index)
    selector = RoundRobinSelector([ReplicaSpec("a"), ReplicaSpec("b")])
    assert selector.select_order(breaker) == []
    now[0] = 105
    assert sorted(selector.select_order(breaker)) == [0, 1]
    with ThreadPoolExecutor(max_workers=8) as executor:
        claims = list(executor.map(breaker.try_acquire_probe, [0] * 8))
    assert claims.count(True) == 1
    assert breaker.try_acquire_probe(1)
    breaker.release_probe(1)
    assert breaker.try_acquire_probe(1)
    breaker.record_failure(0)
    assert not breaker.is_available(0)
    now[0] = 110
    assert breaker.try_acquire_probe(0)
    breaker.record_success(0)
    assert breaker.try_acquire_probe(0)
    assert breaker.try_acquire_probe(0)
    breaker.reset()
    assert sorted(selector.select_order(breaker)) == [0, 1]


@pytest.mark.parametrize("strategy", ["round_robin", "random"])
def test_selection_filters_stale_and_unhealthy_replicas(strategy: SelectionStrategy) -> None:
    selector = build_selector(
        strategy,
        [ReplicaSpec("a", max_staleness_seconds=1), ReplicaSpec("b", max_staleness_seconds=5), ReplicaSpec("c")],
    )
    breaker = ReplicaHealthBreaker(3, failure_threshold=1)
    breaker.record_failure(0)
    assert selector.select_order(breaker, max_staleness_seconds=2) == [2]


@pytest.mark.parametrize("weight", [0, -1, True, 1.5])
def test_invalid_weights_are_rejected(weight: int) -> None:
    with pytest.raises(ImproperConfigurationError):
        ReplicaSpec("a", weight=weight)


@pytest.mark.parametrize("constructor", [RoundRobinSelector, RandomSelector])
def test_empty_selector_is_rejected(constructor: type[RoundRobinSelector] | type[RandomSelector]) -> None:
    with pytest.raises(ImproperConfigurationError):
        constructor([])


@pytest.mark.parametrize(("replica_count", "failure_threshold", "recovery_seconds"), [(0, 1, 1), (1, 0, 1), (1, 1, 0)])
def test_invalid_health_configuration(replica_count: int, failure_threshold: int, recovery_seconds: float) -> None:
    with pytest.raises(ImproperConfigurationError):
        ReplicaHealthBreaker(replica_count, failure_threshold=failure_threshold, recovery_seconds=recovery_seconds)


def test_unknown_selector_is_rejected() -> None:
    with pytest.raises(ImproperConfigurationError):
        build_selector(cast(Any, "unknown"), [ReplicaSpec("a")])


def test_root_routing_exports() -> None:
    import sqlspec
    from sqlspec.exceptions import ReadOnlyTransactionError
    from sqlspec.utils.routing import RoutingContext, RoutingScope

    assert sqlspec.ReadOnlyTransactionError is ReadOnlyTransactionError
    assert sqlspec.RoutingContext is RoutingContext
    assert sqlspec.RoutingScope is RoutingScope
    assert sqlspec.ReplicaSpec is ReplicaSpec
