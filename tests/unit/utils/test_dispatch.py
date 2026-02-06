from sqlspec.utils.dispatch import TypeDispatcher

# pyright: reportPrivateUsage=false


class Base:
    pass


class Child(Base):
    pass


class Unrelated:
    pass


def test_dispatcher_register_and_get_exact_match() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(Base, "base")

    assert dispatcher.get(Base()) == "base"


def test_dispatcher_mro_resolution() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(Base, "base")

    # Child should resolve to Base
    assert dispatcher.get(Child()) == "base"


def test_dispatcher_exact_priority() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(Base, "base")
    dispatcher.register(Child, "child")

    assert dispatcher.get(Base()) == "base"
    assert dispatcher.get(Child()) == "child"


def test_dispatcher_no_match() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(Base, "base")

    assert dispatcher.get(Unrelated()) is None


def test_dispatcher_caching() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(Base, "base")

    child = Child()
    # First call: resolution
    assert dispatcher.get(child) == "base"

    # Second call: cache hit
    assert dispatcher.get(child) == "base"

    # Verify it's cached (implementation detail, but good for regression)
    assert Child in dispatcher._cache


def test_dispatcher_primitive_types() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(int, "integer")
    dispatcher.register(str, "string")

    assert dispatcher.get(1) == "integer"
    assert dispatcher.get("hello") == "string"
    assert dispatcher.get(1.0) is None


def test_dispatcher_clear_cache() -> None:
    dispatcher = TypeDispatcher[str]()
    dispatcher.register(Base, "base")

    child = Child()
    dispatcher.get(child)
    assert Child in dispatcher._cache

    dispatcher.clear_cache()
    assert Child not in dispatcher._cache
