import timeit
from abc import ABC
from sqlspec.utils.dispatch import TypeDispatcher

class StatementFilter(ABC):
    _is_statement_filter = True

class MyFilter(StatementFilter):
    pass

def bench_isinstance():
    f = MyFilter()
    i = 1
    
    start = timeit.default_timer()
    for _ in range(1_000_000):
        isinstance(f, StatementFilter)
        isinstance(i, StatementFilter)
    end = timeit.default_timer()
    print(f"isinstance: {end - start:.4f}s")

def bench_dispatcher():
    dispatcher = TypeDispatcher[bool]()
    dispatcher.register(StatementFilter, True)
    
    f = MyFilter()
    i = 1
    
    # Warmup
    dispatcher.get(f)
    dispatcher.get(i)
    
    start = timeit.default_timer()
    for _ in range(1_000_000):
        dispatcher.get(f)
        dispatcher.get(i)
    end = timeit.default_timer()
    print(f"dispatcher: {end - start:.4f}s")

def bench_getattr():
    f = MyFilter()
    i = 1
    
    start = timeit.default_timer()
    for _ in range(1_000_000):
        getattr(f, "_is_statement_filter", False)
        getattr(i, "_is_statement_filter", False)
    end = timeit.default_timer()
    print(f"getattr: {end - start:.4f}s")

def bench_try_except():
    f = MyFilter()
    i = 1
    
    start = timeit.default_timer()
    for _ in range(1_000_000):
        try:
            f._is_statement_filter
        except AttributeError:
            pass
        
        try:
            i._is_statement_filter
        except AttributeError:
            pass
    end = timeit.default_timer()
    print(f"try_except: {end - start:.4f}s")

if __name__ == "__main__":
    bench_isinstance()
    bench_dispatcher()
    bench_getattr()
    bench_try_except()