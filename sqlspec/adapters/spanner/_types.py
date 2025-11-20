"""Type definitions for Spanner adapter."""

from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from google.cloud.spanner_v1.snapshot import Snapshot
    from google.cloud.spanner_v1.transaction import Transaction

    SpannerConnection = Union[Snapshot, Transaction]
else:
    SpannerConnection = Any
