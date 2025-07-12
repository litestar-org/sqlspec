"""Test filter cache key implementations."""

from datetime import datetime

from sqlspec.statement.filters import (
    AnyCollectionFilter,
    BeforeAfterFilter,
    InCollectionFilter,
    LimitOffsetFilter,
    NotAnyCollectionFilter,
    NotInCollectionFilter,
    NotInSearchFilter,
    OnBeforeAfterFilter,
    OrderByFilter,
    SearchFilter,
)


class TestFilterCacheKeys:
    """Test cache key generation for all filter types."""

    def test_before_after_filter_cache_key(self) -> None:
        """Test BeforeAfterFilter cache key generation."""
        before_date = datetime(2023, 1, 1)
        after_date = datetime(2023, 12, 31)

        filter1 = BeforeAfterFilter("created_at", before=before_date, after=after_date)
        filter2 = BeforeAfterFilter("created_at", before=before_date, after=after_date)
        filter3 = BeforeAfterFilter("updated_at", before=before_date, after=after_date)
        filter4 = BeforeAfterFilter("created_at", before=before_date, after=None)

        # Same filters should have same cache key
        assert filter1.get_cache_key() == filter2.get_cache_key()
        # Different field name should have different cache key
        assert filter1.get_cache_key() != filter3.get_cache_key()
        # Different parameters should have different cache key
        assert filter1.get_cache_key() != filter4.get_cache_key()

    def test_on_before_after_filter_cache_key(self) -> None:
        """Test OnBeforeAfterFilter cache key generation."""
        date1 = datetime(2023, 1, 1)
        date2 = datetime(2023, 12, 31)

        filter1 = OnBeforeAfterFilter("created_at", on_or_before=date1, on_or_after=date2)
        filter2 = OnBeforeAfterFilter("created_at", on_or_before=date1, on_or_after=date2)
        filter3 = OnBeforeAfterFilter("created_at", on_or_before=date2, on_or_after=date1)

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()

    def test_in_collection_filter_cache_key(self) -> None:
        """Test InCollectionFilter cache key generation."""
        values1 = [1, 2, 3]
        values2 = [1, 2, 3]
        values3 = [3, 2, 1]  # Different order, but should be same cache key
        values4 = [1, 2, 4]

        filter1 = InCollectionFilter("id", values1)
        filter2 = InCollectionFilter("id", values2)
        filter3 = InCollectionFilter("id", values3)
        filter4 = InCollectionFilter("id", values4)
        filter5 = InCollectionFilter("id", None)

        # Same values should have same cache key
        assert filter1.get_cache_key() == filter2.get_cache_key()
        # Order matters for tuples, so different order = different key
        assert filter1.get_cache_key() != filter3.get_cache_key()
        # Different values should have different cache key
        assert filter1.get_cache_key() != filter4.get_cache_key()
        # None values should be handled
        assert filter5.get_cache_key() == ("InCollectionFilter", "id", None)

    def test_not_in_collection_filter_cache_key(self) -> None:
        """Test NotInCollectionFilter cache key generation."""
        filter1 = NotInCollectionFilter("status", ["active", "pending"])
        filter2 = NotInCollectionFilter("status", ["active", "pending"])
        filter3 = NotInCollectionFilter("status", ["pending", "active"])

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()

    def test_any_collection_filter_cache_key(self) -> None:
        """Test AnyCollectionFilter cache key generation."""
        filter1 = AnyCollectionFilter("tags", ["python", "sql"])
        filter2 = AnyCollectionFilter("tags", ["python", "sql"])
        filter3 = AnyCollectionFilter("categories", ["python", "sql"])

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()

    def test_not_any_collection_filter_cache_key(self) -> None:
        """Test NotAnyCollectionFilter cache key generation."""
        filter1 = NotAnyCollectionFilter("tags", ["deprecated", "legacy"])
        filter2 = NotAnyCollectionFilter("tags", ["deprecated", "legacy"])
        filter3 = NotAnyCollectionFilter("tags", None)

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()

    def test_limit_offset_filter_cache_key(self) -> None:
        """Test LimitOffsetFilter cache key generation."""
        filter1 = LimitOffsetFilter(limit=10, offset=0)
        filter2 = LimitOffsetFilter(limit=10, offset=0)
        filter3 = LimitOffsetFilter(limit=20, offset=0)
        filter4 = LimitOffsetFilter(limit=10, offset=10)

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()
        assert filter1.get_cache_key() != filter4.get_cache_key()

    def test_order_by_filter_cache_key(self) -> None:
        """Test OrderByFilter cache key generation."""
        filter1 = OrderByFilter("created_at", "asc")
        filter2 = OrderByFilter("created_at", "asc")
        filter3 = OrderByFilter("created_at", "desc")
        filter4 = OrderByFilter("updated_at", "asc")

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()
        assert filter1.get_cache_key() != filter4.get_cache_key()

    def test_search_filter_cache_key(self) -> None:
        """Test SearchFilter cache key generation."""
        filter1 = SearchFilter("name", "test", ignore_case=True)
        filter2 = SearchFilter("name", "test", ignore_case=True)
        filter3 = SearchFilter("name", "test", ignore_case=False)
        filter4 = SearchFilter({"name", "description"}, "test", ignore_case=True)
        filter5 = SearchFilter({"description", "name"}, "test", ignore_case=True)

        assert filter1.get_cache_key() == filter2.get_cache_key()
        assert filter1.get_cache_key() != filter3.get_cache_key()
        # Set fields should be sorted for consistent cache keys
        assert filter4.get_cache_key() == filter5.get_cache_key()

    def test_not_in_search_filter_cache_key(self) -> None:
        """Test NotInSearchFilter cache key generation."""
        filter1 = NotInSearchFilter("name", "test", ignore_case=True)
        filter2 = NotInSearchFilter("name", "test", ignore_case=True)
        filter3 = SearchFilter("name", "test", ignore_case=True)

        assert filter1.get_cache_key() == filter2.get_cache_key()
        # Different filter types should have different cache keys
        assert filter1.get_cache_key() != filter3.get_cache_key()

    def test_cache_key_types_are_hashable(self) -> None:
        """Test that all cache keys are hashable (can be used in dicts/sets)."""
        filters = [
            BeforeAfterFilter("date", before=datetime.now()),
            OnBeforeAfterFilter("date", on_or_after=datetime.now()),
            InCollectionFilter("id", [1, 2, 3]),
            NotInCollectionFilter("id", [4, 5, 6]),
            AnyCollectionFilter("tags", ["a", "b"]),
            NotAnyCollectionFilter("tags", ["c", "d"]),
            LimitOffsetFilter(10, 0),
            OrderByFilter("name", "asc"),
            SearchFilter("text", "query"),
            NotInSearchFilter("text", "exclude"),
        ]

        # All cache keys should be hashable
        cache_keys = {f.get_cache_key() for f in filters}
        assert len(cache_keys) == len(filters)
