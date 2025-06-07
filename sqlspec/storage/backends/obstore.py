import logging
from typing import TYPE_CHECKING, Any, Union, cast

from sqlspec.exceptions import MissingDependencyError, StorageOperationFailedError, wrap_exceptions
from sqlspec.storage.protocol import ObjectStoreProtocol
from sqlspec.typing import OBSTORE_INSTALLED

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlspec.typing import ArrowRecordBatch, ArrowTable

__all__ = ("ObStoreBackend",)

logger = logging.getLogger(__name__)

# Constants for URI validation
S3_URI_PARTS_MIN_COUNT = 2
"""Minimum number of parts in a valid S3 URI (bucket/key)."""


class ObStoreBackend(ObjectStoreProtocol):
    """High-performance object storage using obstore-python.

    This backend implements the ObjectStoreProtocol using obstore-python,
    providing native support for S3, GCS, Azure, and local file storage
    with excellent performance characteristics.
    """

    @classmethod
    def from_config(cls, config: "dict[str, Any]") -> "ObStoreBackend":
        store_config = config.get("store_config", {})
        base_path = config.get("base_path", "")
        return cls(**store_config, base_path=base_path)

    def __init__(self, store: "Union[str, Any]", base_path: str = "") -> None:
        """Initialize with store URI or instance.

        Args:
            store: Either a URI string (e.g., "s3://bucket") or an obstore.Store instance
            base_path: Base path to prepend to all operations
        """
        if not OBSTORE_INSTALLED:
            msg = "obstore"
            raise MissingDependencyError(msg)
        try:
            import obstore as obs

            self.base_path = base_path.rstrip("/") if base_path else ""

            if isinstance(store, str):
                self.store = obs.store.from_url(store)
                self._store_uri = store
                self._store_config = {"uri": store}
            else:
                self.store = store
                self._store_uri = "unknown://"
                with wrap_exceptions(suppress=AttributeError):
                    self._store_uri = store.url
                self._store_config = {}
        except Exception as exc:
            msg = "Failed to initialize obstore client"
            raise StorageOperationFailedError(msg) from exc

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to base_path."""
        if self.base_path:
            return f"{self.base_path}/{path.lstrip('/')}"
        return path

    @property
    def backend_type(self) -> str:
        return "obstore"

    @property
    def base_uri(self) -> str:
        return self._store_uri

    # Core Operations (sync)
    def read_bytes(self, path: str) -> bytes:
        """Read bytes from an object."""
        try:
            resolved_path = self._resolve_path(path)
            return cast("bytes", self.store.get(resolved_path))
        except Exception as exc:
            msg = f"Failed to read bytes from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write bytes to an object."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.put(resolved_path, data)
        except Exception as exc:
            msg = f"Failed to write bytes to {path}"
            raise StorageOperationFailedError(msg) from exc

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read text from an object."""
        try:
            data = self.read_bytes(path)
            return data.decode(encoding)
        except Exception as exc:
            msg = f"Failed to read text from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_text(self, path: str, data: str, encoding: str = "utf-8") -> None:
        """Write text to an object."""
        try:
            self.write_bytes(path, data.encode(encoding))
        except Exception as exc:
            msg = f"Failed to write text to {path}"
            raise StorageOperationFailedError(msg) from exc

    # Object Operations
    def exists(self, path: str) -> bool:
        """Check if an object exists."""
        try:
            resolved_path = self._resolve_path(path)
            # obstore uses head() to check existence
            try:
                self.store.head(resolved_path)
                return True
            except Exception:
                return False
        except Exception as exc:
            msg = f"Failed to check existence of {path}"
            raise StorageOperationFailedError(msg) from exc

    def delete(self, path: str) -> None:
        """Delete an object."""
        try:
            resolved_path = self._resolve_path(path)
            self.store.delete(resolved_path)
        except Exception as exc:
            msg = f"Failed to delete {path}"
            raise StorageOperationFailedError(msg) from exc

    def copy(self, source: str, destination: str) -> None:
        """Copy an object."""
        try:
            source_path = self._resolve_path(source)
            dest_path = self._resolve_path(destination)
            # obstore has native copy support
            self.store.copy(source_path, dest_path)
        except Exception as exc:
            msg = f"Failed to copy {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    def move(self, source: str, destination: str) -> None:
        """Move an object."""
        try:
            # obstore doesn't have native move, so copy then delete
            self.copy(source, destination)
            self.delete(source)
        except Exception as exc:
            msg = f"Failed to move {source} to {destination}"
            raise StorageOperationFailedError(msg) from exc

    # Arrow Operations
    def read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Read an Arrow table from storage."""
        try:
            import pyarrow.parquet as pq

            data = self.read_bytes(path)
            return pq.read_table(data, **kwargs)
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to read Arrow table from {path}"
            raise StorageOperationFailedError(msg) from exc

    def write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Write an Arrow table to storage."""
        try:
            from io import BytesIO

            import pyarrow.parquet as pq

            buffer = BytesIO()
            pq.write_table(table, buffer, **kwargs)
            self.write_bytes(path, buffer.getvalue())
        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to write Arrow table to {path}"
            raise StorageOperationFailedError(msg) from exc

    # Listing Operations
    def list_objects(self, prefix: str = "", recursive: bool = True) -> list[str]:
        """List objects with optional prefix."""
        try:
            resolved_prefix = self._resolve_path(prefix) if prefix else self.base_path
            objects = []

            # Use obstore's list method
            for item in self.store.list(resolved_prefix):
                # Extract the path from the listing result
                if hasattr(item, "path"):
                    objects.append(item.path)
                elif hasattr(item, "key"):
                    objects.append(item.key)
                else:
                    objects.append(str(item))

            # Filter by recursive flag if needed
            if not recursive and self.base_path:
                # Only return immediate children
                base_depth = self.base_path.count("/")
                objects = [obj for obj in objects if obj.count("/") == base_depth + 1]

            return sorted(objects)
        except Exception as exc:
            msg = f"Failed to list objects with prefix {prefix}"
            raise StorageOperationFailedError(msg) from exc

    def glob(self, pattern: str) -> list[str]:
        """Find objects matching a glob pattern."""
        try:
            import fnmatch

            # List all objects and filter by pattern
            all_objects = self.list_objects(recursive=True)
            resolved_pattern = self._resolve_path(pattern)

            return [obj for obj in all_objects if fnmatch.fnmatch(obj, resolved_pattern)]
        except Exception as exc:
            msg = f"Failed to glob pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc

    # Path Operations
    def is_object(self, path: str) -> bool:
        """Check if path points to an object."""
        try:
            resolved_path = self._resolve_path(path)
            # An object exists and doesn't end with /
            return self.exists(path) and not resolved_path.endswith("/")
        except Exception as exc:
            msg = f"Failed to check if {path} is an object"
            raise StorageOperationFailedError(msg) from exc

    def is_path(self, path: str) -> bool:
        """Check if path points to a prefix (directory-like)."""
        try:
            resolved_path = self._resolve_path(path)
            # A path/prefix either ends with / or has objects under it
            if resolved_path.endswith("/"):
                return True

            # Check if there are any objects with this prefix
            objects = self.list_objects(prefix=path, recursive=False)
            return len(objects) > 0
        except Exception as exc:
            msg = f"Failed to check if {path} is a prefix"
            raise StorageOperationFailedError(msg) from exc

    def get_metadata(self, path: str) -> "dict[str, Any]":
        """Get object metadata."""
        try:
            resolved_path = self._resolve_path(path)
            # Use head() to get metadata
            metadata = self.store.head(resolved_path)

            # Convert metadata to dict
            if hasattr(metadata, "__dict__"):
                return vars(metadata)
            if isinstance(metadata, dict):
                return metadata  # pyright: ignore

        except Exception as exc:
            msg = f"Failed to get metadata for {path}"
            raise StorageOperationFailedError(msg) from exc
        else:
            return {
                "path": resolved_path,
                "exists": True,
            }

    def stream_arrow(self, pattern: str) -> "Iterator[ArrowRecordBatch]":
        """Stream Arrow record batches from matching objects."""
        try:
            import pyarrow.parquet as pq

            # Find all matching objects
            matching_objects = self.glob(pattern)

            # Stream each file as record batches
            for obj_path in matching_objects:
                try:
                    data = self.read_bytes(obj_path)
                    from io import BytesIO

                    # Create a BytesIO buffer for PyArrow
                    buffer = BytesIO(data)
                    parquet_file = pq.ParquetFile(buffer)

                    # Yield batches from this file
                    yield from parquet_file.iter_batches()
                except Exception as e:
                    # Log but continue with other files
                    logger.warning("Failed to read %s: %s", obj_path, e)
                    continue

        except ImportError:
            msg = "pyarrow"
            raise MissingDependencyError(msg)
        except Exception as exc:
            msg = f"Failed to stream Arrow data for pattern {pattern}"
            raise StorageOperationFailedError(msg) from exc
