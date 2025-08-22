"""Tests for sqlspec.utils.singleton module.

Tests singleton pattern implementation using metaclass.
"""

import threading

import pytest

from sqlspec.utils.singleton import SingletonMeta

pytestmark = pytest.mark.xdist_group("utils")


class SingletonTestClass(metaclass=SingletonMeta):
    """Test singleton class."""

    def __init__(self, value: str = "default") -> None:
        self.value = value


class AnotherSingletonClass(metaclass=SingletonMeta):
    """Another test singleton class."""

    def __init__(self, data: int = 42) -> None:
        self.data = data


def test_singleton_single_instance() -> None:
    """Test singleton pattern creates only one instance."""
    instance1 = SingletonTestClass("test1")
    instance2 = SingletonTestClass("test2")

    assert instance1 is instance2
    assert instance1.value == "test1"
    assert instance2.value == "test1"


def test_singleton_different_classes() -> None:
    """Test different singleton classes have separate instances."""
    singleton1 = SingletonTestClass("test")
    singleton2 = AnotherSingletonClass(100)

    assert singleton1 is not singleton2  # type: ignore[comparison-overlap]  # pyright: ignore[reportUnnecessaryComparison]
    assert isinstance(singleton1, SingletonTestClass)
    assert isinstance(singleton2, AnotherSingletonClass)


def test_singleton_thread_safety() -> None:
    """Test singleton pattern is thread-safe."""
    instances = []

    def create_instance() -> None:
        instance = SingletonTestClass("thread_test")
        instances.append(instance)

    SingletonMeta._instances.clear()

    threads = [threading.Thread(target=create_instance) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert len({id(instance) for instance in instances}) == 1
    assert all(instance is instances[0] for instance in instances)


def test_singleton_with_args() -> None:
    """Test singleton pattern with constructor arguments."""

    if SingletonTestClass in SingletonMeta._instances:
        del SingletonMeta._instances[SingletonTestClass]

    instance1 = SingletonTestClass("first")
    instance2 = SingletonTestClass("second")

    assert instance1 is instance2
    assert instance1.value == "first"


def test_singleton_metaclass_edge_cases() -> None:
    """Test singleton metaclass with edge cases."""

    if SingletonTestClass in SingletonMeta._instances:
        del SingletonMeta._instances[SingletonTestClass]

    instance1 = SingletonTestClass("first")

    del SingletonMeta._instances[SingletonTestClass]

    instance2 = SingletonTestClass("second")

    assert instance1 is not instance2
    assert instance1.value == "first"
    assert instance2.value == "second"
