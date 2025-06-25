"""Tests for sqlspec.utils.singleton module."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

from sqlspec.utils.singleton import SingletonMeta


@pytest.fixture(autouse=True)
def clear_singleton_instances() -> None:
    """Clear singleton instances before each test to ensure clean state."""
    SingletonMeta._instances.clear()


def test_singleton_basic_instance_creation() -> None:
    """Test that singleton creates and returns the same instance."""

    class TestClass(metaclass=SingletonMeta):
        def __init__(self, value: int = 42) -> None:
            self.value = value

    instance1 = TestClass()
    instance2 = TestClass()

    assert instance1 is instance2
    assert id(instance1) == id(instance2)
    assert instance1.value == 42


def test_singleton_ignores_constructor_args_after_first_call() -> None:
    """Test that singleton ignores constructor arguments on subsequent calls."""

    class TestClass(metaclass=SingletonMeta):
        def __init__(self, value: int) -> None:
            self.value = value

    instance1 = TestClass(100)
    instance2 = TestClass(200)  # These args should be ignored

    assert instance1 is instance2
    assert instance1.value == 100  # Should keep original value
    assert instance2.value == 100  # Should be same as instance1


def test_singleton_different_classes_different_instances() -> None:
    """Test that different singleton classes have different instances."""

    class FirstClass(metaclass=SingletonMeta):
        def __init__(self) -> None:
            self.name = "first"

    class SecondClass(metaclass=SingletonMeta):
        def __init__(self) -> None:
            self.name = "second"

    first_instance = FirstClass()
    second_instance = SecondClass()

    assert first_instance is not second_instance  # type: ignore[comparison-overlap]
    assert first_instance.name == "first"
    assert second_instance.name == "second"


def test_singleton_inheritance_separate_instances() -> None:
    """Test that inherited singleton classes maintain separate instances."""

    class BaseClass(metaclass=SingletonMeta):
        def __init__(self, base_value: str = "base") -> None:
            self.base_value = base_value

    class DerivedClass(BaseClass):
        def __init__(self, derived_value: str = "derived") -> None:
            super().__init__()
            self.derived_value = derived_value

    base_instance1 = BaseClass("base1")
    base_instance2 = BaseClass("base2")
    derived_instance1 = DerivedClass("derived1")
    derived_instance2 = DerivedClass("derived2")

    # Base class instances should be the same
    assert base_instance1 is base_instance2
    assert base_instance1.base_value == "base1"

    # Derived class instances should be the same
    assert derived_instance1 is derived_instance2
    assert derived_instance1.derived_value == "derived1"

    # Base and derived should be different
    assert base_instance1 is not derived_instance1


def test_singleton_with_complex_initialization() -> None:
    """Test singleton behavior with complex initialization."""

    class ComplexClass(metaclass=SingletonMeta):
        def __init__(self, config: dict[str, Any]) -> None:
            self.config = config.copy()
            self.initialized_at = time.time()
            self.counter = 0

        def increment(self) -> None:
            self.counter += 1

    config1 = {"setting1": "value1", "setting2": 42}
    config2 = {"setting1": "different", "setting2": 99}

    instance1 = ComplexClass(config1)
    instance2 = ComplexClass(config2)  # Should be same instance, config ignored

    assert instance1 is instance2
    assert instance1.config == {"setting1": "value1", "setting2": 42}
    assert instance2.config == {"setting1": "value1", "setting2": 42}

    # Test that methods work on the singleton
    instance1.increment()
    assert instance2.counter == 1  # Should be same object


def test_singleton_thread_safety() -> None:
    """Test that singleton creation is thread-safe."""

    class ThreadTestClass(metaclass=SingletonMeta):
        def __init__(self) -> None:
            self.thread_id = threading.current_thread().ident
            # Add small delay to increase chance of race condition
            time.sleep(0.001)

    instances: list[ThreadTestClass] = []

    def create_instance() -> ThreadTestClass:
        return ThreadTestClass()

    # Create instances from multiple threads simultaneously
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(create_instance) for _ in range(20)]
        instances.extend(future.result() for future in as_completed(futures))

    # All instances should be the same object
    first_instance = instances[0]
    for instance in instances[1:]:
        assert instance is first_instance

    # Should only have one unique ID since all instances are the same
    assert len({id(instance) for instance in instances}) == 1


def test_singleton_multiple_inheritance_diamond_problem() -> None:
    """Test singleton behavior with multiple inheritance."""

    class SingletonA(metaclass=SingletonMeta):
        def __init__(self) -> None:
            self.value_a = "A"

    class SingletonB(metaclass=SingletonMeta):
        def __init__(self) -> None:
            self.value_b = "B"

    class DiamondClass(SingletonA, SingletonB):
        def __init__(self) -> None:
            # This is tricky - need to handle multiple inheritance correctly
            super().__init__()
            self.diamond_value = "diamond"

    instance1 = DiamondClass()
    instance2 = DiamondClass()

    assert instance1 is instance2
    assert hasattr(instance1, "diamond_value")
    assert instance1.diamond_value == "diamond"


def test_singleton_with_properties_and_methods() -> None:
    """Test singleton with properties and methods."""

    class PropertyClass(metaclass=SingletonMeta):
        def __init__(self, initial_value: int = 0) -> None:
            self._value = initial_value

        @property
        def value(self) -> int:
            return self._value

        @value.setter
        def value(self, new_value: int) -> None:
            self._value = new_value

        def add(self, amount: int) -> int:
            self._value += amount
            return self._value

    instance1 = PropertyClass(10)
    instance2 = PropertyClass(20)  # Should be same instance

    assert instance1 is instance2
    assert instance1.value == 10  # Original value preserved

    # Modify through one reference
    instance1.value = 50
    assert instance2.value == 50  # Both should see the change

    # Use method through different reference
    result = instance2.add(5)
    assert result == 55
    assert instance1.value == 55


def test_singleton_with_class_variables() -> None:
    """Test singleton behavior with class variables."""

    class ClassVarClass(metaclass=SingletonMeta):
        class_counter = 0

        def __init__(self) -> None:
            ClassVarClass.class_counter += 1
            self.instance_id = ClassVarClass.class_counter

    instance1 = ClassVarClass()
    instance2 = ClassVarClass()

    assert instance1 is instance2
    assert instance1.instance_id == 1  # Only initialized once
    assert instance2.instance_id == 1
    assert ClassVarClass.class_counter == 1  # Only incremented once


def test_singleton_preservation_across_module_reimport() -> None:
    """Test that singleton instances persist across reference changes."""

    class PersistentClass(metaclass=SingletonMeta):
        def __init__(self) -> None:
            self.persistent_data = "important_data"

    # Create instance
    original_instance = PersistentClass()
    original_id = id(original_instance)

    # Create new reference
    new_instance = PersistentClass()

    assert new_instance is original_instance
    assert id(new_instance) == original_id
    assert new_instance.persistent_data == "important_data"


def test_singleton_instances_dictionary_management() -> None:
    """Test that the instances dictionary is managed correctly."""

    class TestClass1(metaclass=SingletonMeta):
        pass

    class TestClass2(metaclass=SingletonMeta):
        pass

    # Initially empty (after setup_method)
    assert len(SingletonMeta._instances) == 0

    # Create instances
    instance1 = TestClass1()
    assert len(SingletonMeta._instances) == 1
    assert TestClass1 in SingletonMeta._instances

    instance2 = TestClass2()
    assert len(SingletonMeta._instances) == 2
    assert TestClass2 in SingletonMeta._instances

    # Verify stored instances
    assert SingletonMeta._instances[TestClass1] is instance1
    assert SingletonMeta._instances[TestClass2] is instance2


def test_singleton_with_exception_in_init() -> None:
    """Test singleton behavior when __init__ raises an exception."""

    class FailingClass(metaclass=SingletonMeta):
        def __init__(self, should_fail: bool = True) -> None:
            if should_fail:
                raise ValueError("Initialization failed")
            self.success = True

    # First call should fail and not create an instance
    with pytest.raises(ValueError, match="Initialization failed"):
        FailingClass()

    # Instance should not be stored if initialization failed
    assert FailingClass not in SingletonMeta._instances

    # Second call with different parameters should work
    instance = FailingClass(should_fail=False)
    assert hasattr(instance, "success")
    assert instance.success is True

    # Third call should return the successfully created instance
    instance2 = FailingClass(should_fail=True)  # should_fail ignored now
    assert instance2 is instance


@pytest.mark.parametrize(
    ("init_args", "init_kwargs"),
    [((), {}), ((1, 2, 3), {}), ((), {"a": 1, "b": 2}), ((1, 2), {"c": 3, "d": 4})],
    ids=["no_args", "positional_args", "keyword_args", "mixed_args"],
)
def test_singleton_with_various_argument_patterns(init_args: tuple[Any, ...], init_kwargs: dict[str, Any]) -> None:
    """Test singleton creation with various argument patterns."""

    class FlexibleClass(metaclass=SingletonMeta):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

    instance1 = FlexibleClass(*init_args, **init_kwargs)
    instance2 = FlexibleClass(999, different="args")  # Should be ignored

    assert instance1 is instance2
    assert instance1.args == init_args
    assert instance1.kwargs == init_kwargs
    assert instance2.args == init_args  # Not (999,)
    assert instance2.kwargs == init_kwargs  # Not {"different": "args"}
