"""Type definitions for Spanner adapter."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import SnapshotCheckout
    from google.cloud.spanner_v1.snapshot import Snapshot
    from google.cloud.spanner_v1.transaction import Transaction

    SpannerConnection = Snapshot | SnapshotCheckout | Transaction
else:
    SpannerConnection = Any
