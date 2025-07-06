import time
import unittest

from sqlspec.statement_new.cache import sql_cache
from sqlspec.statement_new.sql import SQL, SQLConfig


class TestSQLCaching(unittest.TestCase):
    def setUp(self) -> None:
        # Clear the cache before each test
        sql_cache.clear()

    def test_caching_with_same_sql_and_dialect(self) -> None:
        """Test that repeated calls with the same SQL and dialect hit the cache."""
        sql_string = "SELECT * FROM users WHERE id = ?"
        dialect = "sqlite"
        config = SQLConfig(dialect=dialect)

        # First call, should process and cache
        start_time = time.perf_counter()
        sql1 = SQL(sql_string, 1, config=config)
        _ = sql1._ensure_processed
        duration1 = time.perf_counter() - start_time

        # Second call, should be faster due to caching
        start_time = time.perf_counter()
        sql2 = SQL(sql_string, 1, config=config)
        _ = sql2._ensure_processed
        duration2 = time.perf_counter() - start_time

        self.assertLess(duration2, duration1, "Second call should be faster due to caching")
        self.assertIs(sql1._state, sql2._state, "Should be the same cached SQLState object")

    def test_no_caching_with_different_sql(self) -> None:
        """Test that different SQL statements are not cached together."""
        sql_string1 = "SELECT * FROM users WHERE id = ?"
        sql_string2 = "SELECT * FROM products WHERE id = ?"
        dialect = "sqlite"
        config = SQLConfig(dialect=dialect)

        sql1 = SQL(sql_string1, 1, config=config)
        _ = sql1._ensure_processed

        sql2 = SQL(sql_string2, 1, config=config)
        _ = sql2._ensure_processed

        self.assertIsNot(sql1._state, sql2._state, "Different SQL should have different SQLState objects")

    def test_no_caching_with_different_dialect(self) -> None:
        """Test that same SQL with different dialects are not cached together."""
        sql_string = "SELECT * FROM users WHERE id = ?"
        config1 = SQLConfig(dialect="sqlite")
        config2 = SQLConfig(dialect="postgres")

        sql1 = SQL(sql_string, 1, config=config1)
        _ = sql1._ensure_processed

        sql2 = SQL(sql_string, 1, config=config2)
        _ = sql2._ensure_processed

        self.assertIsNot(sql1._state, sql2._state, "Different dialects should have different SQLState objects")

    def test_cache_eviction(self) -> None:
        """Test that the cache evicts old items when it reaches max_size."""
        sql_cache.max_size = 2

        sql1 = SQL("SELECT 1", config=SQLConfig(dialect="sqlite"))
        key1 = sql1._state.cache_key()
        _ = sql1._ensure_processed

        sql2 = SQL("SELECT 2", config=SQLConfig(dialect="sqlite"))
        key2 = sql2._state.cache_key()
        _ = sql2._ensure_processed

        # At this point, cache should have sql1 and sql2
        self.assertIsNotNone(sql_cache.get(key1))
        self.assertIsNotNone(sql_cache.get(key2))

        # This should evict sql1
        sql3 = SQL("SELECT 3", config=SQLConfig(dialect="sqlite"))
        key3 = sql3._state.cache_key()
        _ = sql3._ensure_processed

        self.assertIsNone(sql_cache.get(key1), "Oldest item should be evicted")
        self.assertIsNotNone(sql_cache.get(key2))
        self.assertIsNotNone(sql_cache.get(key3))


if __name__ == "__main__":
    unittest.main()
