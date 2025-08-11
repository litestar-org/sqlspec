"""Storage backend capability system.

This module provides a centralized way to track and query storage backend capabilities.
"""

from dataclasses import dataclass
from typing import ClassVar

from mypy_extensions import mypyc_attr

__all__ = ("HasStorageCapabilities", "StorageCapabilities")


@dataclass
class StorageCapabilities:
    """Tracks capabilities of a storage backend."""

    supports_read: bool = True
    supports_write: bool = True
    supports_delete: bool = True
    supports_list: bool = True
    supports_exists: bool = True
    supports_copy: bool = True
    supports_move: bool = True
    supports_metadata: bool = True

    supports_arrow: bool = False
    supports_streaming: bool = False
    supports_async: bool = False
    supports_batch_operations: bool = False
    supports_multipart_upload: bool = False
    supports_compression: bool = False

    supports_s3_select: bool = False
    supports_gcs_compose: bool = False
    supports_azure_snapshots: bool = False

    is_remote: bool = True
    is_cloud_native: bool = False
    has_low_latency: bool = False

    @classmethod
    def local_filesystem(cls) -> "StorageCapabilities":
        """Capabilities for local filesystem backend."""
        return cls(
            is_remote=False, has_low_latency=True, supports_arrow=True, supports_streaming=True, supports_async=True
        )

    @classmethod
    def s3_compatible(cls) -> "StorageCapabilities":
        """Capabilities for S3-compatible backends."""
        return cls(
            is_cloud_native=True,
            supports_multipart_upload=True,
            supports_s3_select=True,
            supports_arrow=True,
            supports_streaming=True,
            supports_async=True,
        )

    @classmethod
    def gcs(cls) -> "StorageCapabilities":
        """Capabilities for Google Cloud Storage."""
        return cls(
            is_cloud_native=True,
            supports_multipart_upload=True,
            supports_gcs_compose=True,
            supports_arrow=True,
            supports_streaming=True,
            supports_async=True,
        )

    @classmethod
    def azure_blob(cls) -> "StorageCapabilities":
        """Capabilities for Azure Blob Storage."""
        return cls(
            is_cloud_native=True,
            supports_multipart_upload=True,
            supports_azure_snapshots=True,
            supports_arrow=True,
            supports_streaming=True,
            supports_async=True,
        )


@mypyc_attr(allow_interpreted_subclasses=True)
class HasStorageCapabilities:
    """Mixin for storage backends that expose their capabilities."""

    __slots__ = ()

    capabilities: ClassVar[StorageCapabilities]

    @classmethod
    def has_capability(cls, capability: str) -> bool:
        """Check if backend has a specific capability."""
        return getattr(cls.capabilities, capability, False)

    @classmethod
    def get_capabilities(cls) -> StorageCapabilities:
        """Get all capabilities for this backend."""
        return cls.capabilities
