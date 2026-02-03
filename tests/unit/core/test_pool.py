# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for ObjectPool behavior."""

import pytest

from sqlspec.core._pool import ObjectPool

pytestmark = pytest.mark.xdist_group("core")


class _Sentinel:
    __slots__ = ("value",)

    def __init__(self, value: int) -> None:
        self.value = value


def test_object_pool_acquire_uses_factory_when_empty() -> None:
    calls = 0

    def factory() -> _Sentinel:
        nonlocal calls
        calls += 1
        return _Sentinel(calls)

    def resetter(obj: _Sentinel) -> None:
        obj.value = -1

    pool = ObjectPool(factory=factory, resetter=resetter, max_size=2)

    first = pool.acquire()
    second = pool.acquire()

    assert calls == 2
    assert first is not second
    assert first.value == 1
    assert second.value == 2


def test_object_pool_release_resets_and_respects_max_size() -> None:
    reset_calls: list[_Sentinel] = []

    def factory() -> _Sentinel:
        return _Sentinel(0)

    def resetter(obj: _Sentinel) -> None:
        obj.value = 0
        reset_calls.append(obj)

    pool = ObjectPool(factory=factory, resetter=resetter, max_size=1)

    first = pool.acquire()
    second = pool.acquire()

    first.value = 10
    second.value = 20

    pool.release(first)
    pool.release(second)

    assert reset_calls == [first, second]
    assert len(pool._pool) == 1
    assert pool._pool[0] is first

    reused = pool.acquire()
    assert reused is first
    assert reused.value == 0
