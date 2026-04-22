"""Pagination containers excluded from mypyc compilation.

``msgspec.Struct`` uses a custom metaclass, and mypyc-compiled classes cannot
declare a metaclass. This module is kept uncompiled (see the mypyc ``exclude``
list in ``pyproject.toml``) so that :class:`OffsetPagination` retains runtime
``__annotations__`` and remains introspectable by Litestar's OpenAPI generator.

``msgspec`` is an optional dependency. When present we inherit from
``msgspec.Struct`` for the best runtime introspection and native encoding.
When absent we fall back to a plain generic container so ``sqlspec`` continues
to import without ``msgspec`` installed.
"""

from collections.abc import Sequence
from typing import Generic

from typing_extensions import TypeVar

__all__ = ("OffsetPagination",)

T = TypeVar("T")

try:
    import msgspec

    class OffsetPagination(msgspec.Struct, Generic[T]):
        """Container for data returned using limit/offset pagination.

        Args:
            items: List of data being sent as part of the response.
            limit: Maximal number of items to send.
            offset: Offset from the beginning of the query. Identical to an index.
            total: Total number of items.
        """

        items: Sequence[T]
        limit: int
        offset: int
        total: int

except ImportError:

    class OffsetPagination(Generic[T]):  # type: ignore[no-redef]
        """Container for data returned using limit/offset pagination (msgspec-free fallback)."""

        __slots__ = ("items", "limit", "offset", "total")

        items: Sequence[T]
        limit: int
        offset: int
        total: int

        def __init__(self, items: Sequence[T], limit: int, offset: int, total: int) -> None:
            self.items = items
            self.limit = limit
            self.offset = offset
            self.total = total
