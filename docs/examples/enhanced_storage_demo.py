"""Demonstration of enhanced storage registry patterns.

This example shows how the new storage registry provides Advanced Alchemy-inspired
patterns for flexible, lazy-loaded storage backend management.
"""

from sqlspec.storage import storage_registry
from sqlspec.storage.backends.fsspec import FSSpecBackend

__all__ = (
    "demo_advanced_usage",
    "demo_backend_type_scenarios",
    "demo_basic_registration",
    "demo_lazy_loading",
    "demo_scheme_mapping",
    "need_faster_access",
)


def demo_basic_registration() -> None:
    """Basic registration patterns."""

    # 1. Register with direct instance
    local_backend = FSSpecBackend("/tmp/sqlspec")
    storage_registry.register("temp-files", local_backend)

    # 2. Register with configuration (lazy loading)
    storage_registry.register(
        "s3-data",
        {
            "backend_type": "fsspec",
            "protocol": "s3",
            "bucket": "my-data-bucket",
            "fs_config": {"key": "ACCESS_KEY", "secret": "SECRET_KEY"},
        },
    )

    # 3. Register custom backend via import string
    storage_registry.register(
        "custom-storage", {"backend_type": "my_company.storage.CustomBackend", "custom_option": "value"}
    )


def demo_scheme_mapping() -> None:
    """URI scheme-based automatic resolution with backend type control."""

    # Register scheme mappings
    storage_registry.register_scheme("s3", "s3-data")
    storage_registry.register_scheme("gs", "gcs-data")

    # Auto-resolve with obstore → fsspec fallback (default behavior)
    storage_registry.resolve_from_uri("s3://my-bucket/file.parquet")
    # Tries obstore first, falls back to fsspec if obstore fails

    # Explicitly specify backend type
    storage_registry.resolve_from_uri("s3://my-bucket/file.parquet", backend_type="obstore")

    storage_registry.resolve_from_uri("s3://my-bucket/file.parquet", backend_type="fsspec")

    storage_registry.resolve_from_uri("file:///tmp/file.parquet", backend_type="local")


def demo_lazy_loading() -> None:
    """Lazy loading and configuration updates."""

    # Register configuration - backend not instantiated yet
    storage_registry.register(
        "analytics-s3",
        {"backend_type": "obstore", "store_config": {"bucket": "analytics-bucket", "region": "us-west-2"}},
    )

    # Backend is instantiated only when first accessed
    storage_registry.get("analytics-s3")  # Creates instance here

    # Subsequent calls return cached instance
    storage_registry.get("analytics-s3")  # Returns cached

    # Update configuration at runtime
    storage_registry.update_config(
        "analytics-s3",
        {
            "store_config": {"region": "us-east-1"}  # Updates region
        },
    )

    # Next access will use updated config
    storage_registry.get("analytics-s3")  # Re-instantiated


def demo_advanced_usage() -> None:
    """Advanced usage patterns with backend type control."""

    # Conditional registration based on environment
    import os

    if os.getenv("ENVIRONMENT") == "production":
        storage_registry.register(
            "data-store",
            {"backend_type": "obstore", "store_config": {"bucket": "prod-data-bucket", "region": "us-east-1"}},
        )
    else:
        storage_registry.register("data-store", {"backend_type": "local", "base_path": "/tmp/dev-data"})

    # Usage code remains the same regardless of backend
    storage_registry.get("data-store")

    # Runtime backend switching
    if need_faster_access():
        storage_registry.update_config(
            "data-store",
            {
                "store_config": {"region": "us-west-2"}  # Closer region
            },
        )

    # URI-based automatic resolution with backend type preference
    storage_registry.resolve_from_uri(
        "s3://my-bucket/file.parquet",
        backend_type="obstore",  # Force obstore (no fallback)
    )

    # Auto-resolution with obstore → fsspec fallback (default)
    storage_registry.resolve_from_uri("s3://my-bucket/file.parquet")

    # Fallback to default key if scheme fails
    try:
        storage_registry.resolve_from_uri(
            "unknown://scheme/file.parquet",
            default_key="data-store",  # Fallback to registered backend
        )
    except Exception:
        # Handle missing backends gracefully
        storage_registry.get("local-dev")


def demo_backend_type_scenarios() -> None:
    """Demonstrate different backend type selection scenarios."""

    # Scenario 1: High-performance workload - prefer obstore
    def high_performance_copy(source_uri: str, dest_uri: str) -> None:
        """Copy with obstore for best performance."""
        source_backend = storage_registry.resolve_from_uri(source_uri, backend_type="obstore")
        dest_backend = storage_registry.resolve_from_uri(dest_uri, backend_type="obstore")

        data = source_backend.read_bytes(source_uri)
        dest_backend.write_bytes(dest_uri, data)

    # Scenario 2: Compatibility workload - prefer fsspec
    def compatible_copy(source_uri: str, dest_uri: str) -> None:
        """Copy with fsspec for broader compatibility."""
        source_backend = storage_registry.resolve_from_uri(source_uri, backend_type="fsspec")
        dest_backend = storage_registry.resolve_from_uri(dest_uri, backend_type="fsspec")

        data = source_backend.read_bytes(source_uri)
        dest_backend.write_bytes(dest_uri, data)

    # Scenario 3: Auto-fallback for resilient operations
    def resilient_copy(source_uri: str, dest_uri: str) -> None:
        """Copy with automatic obstore → fsspec fallback."""
        # No backend_type specified = automatic fallback
        source_backend = storage_registry.resolve_from_uri(source_uri)
        dest_backend = storage_registry.resolve_from_uri(dest_uri)

        data = source_backend.read_bytes(source_uri)
        dest_backend.write_bytes(dest_uri, data)

    # Scenario 4: Mixed backend operations
    def mixed_backend_operation() -> None:
        """Use different backends for different purposes."""
        # Fast cloud access with obstore
        cloud_backend = storage_registry.resolve_from_uri("s3://data-lake/dataset.parquet", backend_type="obstore")

        # Local caching with local backend
        cache_backend = storage_registry.resolve_from_uri("file:///tmp/cache/dataset.parquet", backend_type="local")

        # Compatible remote access with fsspec
        remote_backend = storage_registry.resolve_from_uri("gs://backup-bucket/dataset.parquet", backend_type="fsspec")

        # Use appropriate backend for each operation
        data = cloud_backend.read_bytes("s3://data-lake/dataset.parquet")
        cache_backend.write_bytes("file:///tmp/cache/dataset.parquet", data)
        remote_backend.write_bytes("gs://backup-bucket/dataset.parquet", data)


def need_faster_access() -> bool:
    """Mock function for demo."""
    return False


if __name__ == "__main__":
    demo_basic_registration()
    demo_scheme_mapping()
    demo_lazy_loading()
    demo_advanced_usage()
    demo_backend_type_scenarios()

    print("Enhanced storage registry demo completed!")
    print(f"Registered keys: {storage_registry.list_keys()}")
    print(f"Scheme mappings: {storage_registry.list_schemes()}")
