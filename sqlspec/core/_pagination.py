"""Pagination containers excluded from mypyc compilation.

``msgspec.Struct`` uses a custom metaclass, and mypyc-compiled classes cannot
declare a metaclass. This module is kept uncompiled (see the mypyc ``exclude``
list in ``pyproject.toml``) so that :class:`OffsetPagination` retains runtime
``__annotations__`` and remains introspectable by Litestar's OpenAPI generator.
"""

from collections.abc import Sequence
from typing import Generic

import msgspec
from typing_extensions import TypeVar

__all__ = ("OffsetPagination",)

T = TypeVar("T")


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
