import contextlib
import timeit

from sqlspec.utils.dispatch import TypeDispatcher

__all__ = ("MyFilter", "StatementFilter", "bench_dispatcher", "bench_getattr", "bench_isinstance", "bench_try_except", )


class StatementFilter:
    _is_statement_filter = True


class MyFilter(StatementFilter):
    pass


def bench_isinstance() -> None:
    f = MyFilter()
    i = 1

    timeit.default_timer()
    for _ in range(1_000_000):
        isinstance(f, StatementFilter)
        isinstance(i, StatementFilter)
    timeit.default_timer()


def bench_dispatcher() -> None:
    dispatcher = TypeDispatcher[bool]()
    dispatcher.register(StatementFilter, True)

    f = MyFilter()
    i = 1

    # Warmup
    dispatcher.get(f)
    dispatcher.get(i)

    timeit.default_timer()
    for _ in range(1_000_000):
        dispatcher.get(f)
        dispatcher.get(i)
    timeit.default_timer()


def bench_getattr() -> None:
    f = MyFilter()
    i = 1

    timeit.default_timer()
    for _ in range(1_000_000):
        getattr(f, "_is_statement_filter", False)
        getattr(i, "_is_statement_filter", False)
    timeit.default_timer()


def bench_try_except() -> None:
    f = MyFilter()
    i = 1

    timeit.default_timer()
    for _ in range(1_000_000):
        with contextlib.suppress(AttributeError):
            _ = f._is_statement_filter

        with contextlib.suppress(AttributeError):
            _ = i._is_statement_filter
    timeit.default_timer()


if __name__ == "__main__":
    bench_isinstance()
    bench_dispatcher()
    bench_getattr()
    bench_try_except()
