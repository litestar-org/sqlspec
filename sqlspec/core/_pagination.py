"""Pagination containers excluded from mypyc compilation.

mypyc strips class-level ``__annotations__`` from compiled modules, which
breaks Litestar's OpenAPI schema generation for generic containers. This
module is kept uncompiled (see the mypyc ``exclude`` list in ``pyproject.toml``)
so :class:`OffsetPagination` and :class:`CursorPagination` retain runtime ``__annotations__`` and remains
introspectable by Litestar (and any consumer calling
:func:`typing.get_type_hints`).

Implemented as a stdlib :func:`~dataclasses.dataclass` so it has no optional
runtime dependencies — ``msgspec`` is not required. Litestar's OpenAPI generator
natively recognizes dataclasses, and Litestar's default serialization (backed
by msgspec when installed) emits the expected ``{items, limit, offset, total}``
JSON shape.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic

from typing_extensions import TypeVar

__all__ = ("CursorPagination", "OffsetPagination")

T = TypeVar("T")


@dataclass
class OffsetPagination(Generic[T]):
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


@dataclass
class CursorPagination(Generic[T]):
    """Container for data returned using cursor (keyset) pagination.

    Args:
        items: List of data being sent as part of the response.
        limit: Maximal number of items to send.
        next_cursor: Token for the next page, or ``None`` on the last page.
        previous_cursor: Token for the previous page, or ``None`` on the first page.
        has_next: Whether a page exists after this one.
        has_previous: Whether a page exists before this one.
    """

    items: Sequence[T]
    limit: int
    next_cursor: "str | None"
    previous_cursor: "str | None"
    has_next: bool
    has_previous: bool
