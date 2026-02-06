import hashlib
import time

SQL = "INSERT INTO notes (body) VALUES (?)"
PARAM_FINGERPRINT = "seq:(str,)"
HASH_DATA = (SQL, PARAM_FINGERPRINT, "qmark", "qmark", "sqlite", False)
ITERATIONS = 10000


def bench_make_cache_key() -> float:
    start = time.perf_counter()
    for _ in range(ITERATIONS):
        # Current logic in SQLProcessor._make_cache_key
        hash_str = hashlib.blake2b(repr(HASH_DATA).encode("utf-8"), digest_size=8).hexdigest()
        _ = f"sql_{hash_str}"
    return time.perf_counter() - start


def bench_tuple_key() -> float:
    start = time.perf_counter()
    for _ in range(ITERATIONS):
        # Alternative: use tuple directly as key
        _ = HASH_DATA
    return time.perf_counter() - start


if __name__ == "__main__":
    bench_make_cache_key()
    bench_tuple_key()
