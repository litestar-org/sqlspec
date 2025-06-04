# SQLSpec Data Import/Export and Enhanced Storage Abstraction Layer Plan

## 1. Introduction & Goals

This document outlines the architecture and implementation plan for enhancing SQLSpec with efficient data import/export capabilities and a flexible, key-based storage abstraction layer inspired by Advanced Alchemy's approach.

**Key Features:**

- `copy_from_path`: Execute database-native bulk load commands from local file paths (e.g., CSV, Parquet).
- `copy_from_uri`: Execute database-native bulk load commands from remote URIs, leveraging a new storage abstraction layer.
- `copy_from_arrow`: Load data directly from `pyarrow.Table` objects into database tables.
- `export_to_uri`: Export data (from a table or a query result) to a remote URI in specified formats (e.g., Parquet, CSV) using the storage layer.
- `copy_from_csv`: Convenience method to load data from CSV files/URIs.
- `export_to_csv`: Convenience method to export data to CSV files/URIs.
- **Enhanced Storage Abstraction Layer**: A unified interface supporting arbitrary backend keys (like "my_sales_bucket"), multiple backend choices for the same storage type (fsspec vs obstore), and driver-level configuration.

**Goals:**

- **Performance**: Prioritize the most efficient, database-specific methods for bulk data operations.
- **Developer Experience**: Provide intuitive APIs with named storage configurations and minimal per-call options.
- **Flexibility**: Support various data formats, storage systems, and backend choices without URI scheme limitations.
- **Modularity**: Clean separation between data operations and storage interactions.
- **Extensibility**: Design the storage layer to easily accommodate new backends and configurations.

## 2. Enhanced Storage Abstraction Layer (Key-Based)

### 2.1. Core Concept: Key-Based Backend Registration

Instead of registering backends by URI scheme (limiting and inflexible), we register pre-configured backend **instances** with arbitrary **keys**. This allows:

- **Named Configurations**: `"my_sales_bucket"`, `"staging_data"`, `"production_exports"`
- **Multiple Backend Choices**: Both `fsspec` and `obstore` can handle S3/GCS, choose based on needs
- **Environment Flexibility**: Same key can use different backends/configurations across environments
- **Better DX**: Minimal options needed per API call

**Example Usage:**

```python
# Instead of:
driver.copy_from_uri("sales", "s3://bucket/data.parquet", 
                    storage_options={"aws_access_key_id": "...", "backend": "obstore"})

# Use:
driver.copy_from_storage("sales", "my_sales_bucket", "data.parquet")
```

### 2.2. `sqlspec.storage.protocol.StorageBackendProtocol`

Enhanced protocol with both URI-based and key-based operations:

```python
from typing import Protocol, Any, Literal, runtime_checkable

@runtime_checkable
class StorageBackendProtocol(Protocol):
    """Protocol for storage backends supporting both URI and path-based operations."""
    
    # Core operations
    def read_bytes(self, path: str, **kwargs: Any) -> bytes: ...
    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None: ...
    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str: ...
    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None: ...
    
    # Arrow support (optional)
    def read_arrow(self, path: str, **kwargs: Any) -> "pyarrow.Table":
        """Read an Arrow table from the given path."""
        raise NotImplementedError("Arrow reading not supported by this backend")
    
    def write_arrow(self, path: str, table: "pyarrow.Table", **kwargs: Any) -> None:
        """Write an Arrow table to the given path."""
        raise NotImplementedError("Arrow writing not supported by this backend")
    
    # File operations
    def exists(self, path: str, **kwargs: Any) -> bool: ...
    def delete(self, path: str, **kwargs: Any) -> None: ...
    def list_objects(self, prefix: str = "", **kwargs: Any) -> list[str]: ...
    
    # Security
    def get_signed_url(
        self,
        path: str,
        operation: Literal["read", "write"] = "read",
        expires_in: int = 3600,
        **kwargs: Any,
    ) -> str:
        """Generate a pre-signed URL for the given path and operation."""
        raise NotImplementedError("Signed URL generation not supported by this backend")
    
    # Backend metadata
    @property
    def backend_type(self) -> str:
        """Return backend type identifier (e.g., 'obstore', 'fsspec', 'local')."""
        ...
    
    @property
    def base_uri(self) -> str:
        """Return the base URI this backend is configured for (e.g., 's3://bucket')."""
        ...
```

### 2.3. `sqlspec.storage.registry.StorageRegistry`

Enhanced registry supporting key-based backend management:

```python
from typing import Any, Mapping, Optional
from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.exceptions import SQLSpecError

class BackendNotRegisteredError(SQLSpecError):
    """Raised when a requested storage backend key is not registered."""

class StorageRegistry:
    """Registry for managing pre-configured storage backend instances by key."""
    
    def __init__(self) -> None:
        self._backends: dict[str, StorageBackendProtocol] = {}
    
    def register_backend(self, key: str, backend_instance: StorageBackendProtocol) -> None:
        """Register a pre-configured backend instance with a unique key."""
        if not isinstance(key, str) or not key:
            msg = "Backend key must be a non-empty string."
            raise TypeError(msg)
        if not isinstance(backend_instance, StorageBackendProtocol):
            msg = "Backend instance must implement StorageBackendProtocol."
            raise TypeError(msg)
        if key in self._backends:
            msg = f"Backend key '{key}' is already registered."
            raise ValueError(msg)
        self._backends[key] = backend_instance
    
    def get_backend(self, key: str) -> StorageBackendProtocol:
        """Retrieve a registered backend instance by its key."""
        try:
            return self._backends[key]
        except KeyError:
            msg = f"No storage backend registered for key: '{key}'"
            raise BackendNotRegisteredError(msg) from None
    
    def unregister_backend(self, key: str) -> None:
        """Unregister a backend instance by its key."""
        if key not in self._backends:
            msg = f"Backend key '{key}' not found for unregistration."
            raise BackendNotRegisteredError(msg)
        del self._backends[key]
    
    def is_registered(self, key: str) -> bool:
        """Check if a backend key is registered."""
        return key in self._backends
    
    def list_registered_keys(self) -> list[str]:
        """List all registered backend keys."""
        return list(self._backends.keys())
    
    def register_from_config(self, key: str, config: Mapping[str, Any]) -> None:
        """Register a backend from configuration dictionary."""
        backend_type = config.get("backend_type")
        if not backend_type:
            msg = "Configuration must specify 'backend_type'"
            raise ValueError(msg)
        
        # Factory method to create backend from config
        backend_instance = self._create_backend_from_config(backend_type, config)
        self.register_backend(key, backend_instance)
    
    def _create_backend_from_config(self, backend_type: str, config: Mapping[str, Any]) -> StorageBackendProtocol:
        """Create a backend instance from configuration."""
        if backend_type == "obstore":
            from sqlspec.storage.backends.obstore import ObstoreBackend
            return ObstoreBackend.from_config(config)
        elif backend_type == "fsspec":
            from sqlspec.storage.backends.fsspec import FsspecBackend
            return FsspecBackend.from_config(config)
        elif backend_type == "local":
            from sqlspec.storage.backends.file import LocalFileBackend
            return LocalFileBackend.from_config(config)
        else:
            msg = f"Unknown backend type: {backend_type}"
            raise ValueError(msg)

# Global singleton registry
storage_registry = StorageRegistry()
```

### 2.4. Backend Implementations

#### 2.4.1. `ObstoreBackend`

```python
# sqlspec/storage/backends/obstore.py
from typing import Any, Mapping
from sqlspec.storage.protocol import StorageBackendProtocol

class ObstoreBackend:
    """Storage backend using obstore for S3, GCS, Azure, etc."""
    
    def __init__(self, store_config: Mapping[str, Any], base_path: str = ""):
        """Initialize with obstore configuration."""
        self._store_config = store_config
        self._base_path = base_path.rstrip("/")
        self._store = self._create_store()
    
    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "ObstoreBackend":
        """Create from configuration dictionary."""
        store_config = config.get("store_config", {})
        base_path = config.get("base_path", "")
        return cls(store_config, base_path)
    
    def _create_store(self):
        """Create obstore instance from configuration."""
        import obstore
        return obstore.ObjectStore(**self._store_config)
    
    def _full_path(self, path: str) -> str:
        """Combine base path with relative path."""
        if self._base_path:
            return f"{self._base_path}/{path.lstrip('/')}"
        return path
    
    @property
    def backend_type(self) -> str:
        return "obstore"
    
    @property
    def base_uri(self) -> str:
        # Extract from store config
        return f"{self._store_config.get('scheme', 'unknown')}://{self._store_config.get('bucket', 'unknown')}"
    
    # Implement all StorageBackendProtocol methods...
```

#### 2.4.2. `FsspecBackend`

```python
# sqlspec/storage/backends/fsspec.py
from typing import Any, Mapping
from sqlspec.storage.protocol import StorageBackendProtocol

class FsspecBackend:
    """Storage backend using fsspec for various filesystems."""
    
    def __init__(self, protocol: str, fs_config: Mapping[str, Any], base_path: str = ""):
        """Initialize with fsspec filesystem."""
        self._protocol = protocol
        self._fs_config = fs_config
        self._base_path = base_path.rstrip("/")
        self._fs = self._create_filesystem()
    
    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "FsspecBackend":
        """Create from configuration dictionary."""
        protocol = config["protocol"]
        fs_config = config.get("fs_config", {})
        base_path = config.get("base_path", "")
        return cls(protocol, fs_config, base_path)
    
    def _create_filesystem(self):
        """Create fsspec filesystem from configuration."""
        import fsspec
        return fsspec.filesystem(self._protocol, **self._fs_config)
    
    @property
    def backend_type(self) -> str:
        return "fsspec"
    
    @property
    def base_uri(self) -> str:
        bucket = self._fs_config.get("bucket", self._fs_config.get("bucket_name", "unknown"))
        return f"{self._protocol}://{bucket}"
    
    # Implement all StorageBackendProtocol methods...
```

#### 2.4.3. `LocalFileBackend`

```python
# sqlspec/storage/backends/file.py
from pathlib import Path
from typing import Any, Mapping
from sqlspec.storage.protocol import StorageBackendProtocol

class LocalFileBackend:
    """Storage backend for local filesystem operations."""
    
    def __init__(self, base_path: str = ""):
        """Initialize with base directory path."""
        self._base_path = Path(base_path) if base_path else Path.cwd()
        self._base_path.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "LocalFileBackend":
        """Create from configuration dictionary."""
        base_path = config.get("base_path", "")
        return cls(base_path)
    
    @property
    def backend_type(self) -> str:
        return "local"
    
    @property
    def base_uri(self) -> str:
        return f"file://{self._base_path.absolute()}"
    
    # Implement all StorageBackendProtocol methods...
```

## 3. Enhanced Configuration Integration

### 3.1. Driver-Level Storage Configuration

Update `SQLConfig` to include storage settings:

```python
# sqlspec/config.py (additions)
@dataclass
class StorageConfig:
    """Configuration for storage backends."""
    default_storage_key: Optional[str] = None
    backends: dict[str, dict[str, Any]] = field(default_factory=dict)
    auto_register: bool = True

@dataclass 
class SQLConfig:
    # ... existing fields ...
    storage: StorageConfig = field(default_factory=StorageConfig)
```

### 3.2. Driver Integration

Drivers automatically configure storage backends on initialization:

```python
# Example in driver __init__
def __init__(self, config: SQLConfig):
    super().__init__(config)
    self._configure_storage_backends()

def _configure_storage_backends(self) -> None:
    """Configure storage backends from driver config."""
    if not self.config.storage.auto_register:
        return
    
    for key, backend_config in self.config.storage.backends.items():
        try:
            storage_registry.register_from_config(key, backend_config)
        except Exception as e:
            logger.warning(f"Failed to register storage backend '{key}': {e}")
```

## 4. Enhanced Copy/Export Mixins

### 4.1. Updated Method Signatures

```python
# sqlspec/statement/mixins/_copy_sync.py
class SyncCopyOperationsMixin:
    """Enhanced sync copy operations with key-based storage."""
    
    def copy_from_storage(
        self,
        table_name: str,
        storage_key: str,
        file_path: str,
        *,
        strategy: str = "append",
        format: Optional[str] = None,
        copy_options: Optional[dict[str, Any]] = None,
    ) -> "ExecuteResult":
        """Copy data from a storage backend to a database table.
        
        Args:
            table_name: Target table name
            storage_key: Registered storage backend key (e.g., "my_sales_bucket")
            file_path: Path within the storage backend (e.g., "2024/sales.parquet")
            strategy: Load strategy for Arrow tables ("append", "truncate", "replace")
            format: File format hint ("csv", "parquet", "json", etc.)
            copy_options: Database-specific copy options
        """
        backend = storage_registry.get_backend(storage_key)
        
        # Auto-detect format if not provided
        if format is None:
            format = self._detect_format(file_path)
        
        # Use database-specific optimized path if available
        if self._can_copy_directly(backend, format):
            return self._copy_direct_from_storage(table_name, backend, file_path, format, copy_options)
        
        # Fallback to download and local copy
        return self._copy_via_download(table_name, backend, file_path, format, strategy, copy_options)
    
    def export_to_storage(
        self,
        source: Union[str, "SQL"],
        storage_key: str,
        file_path: str,
        *,
        format: str = "parquet",
        export_options: Optional[dict[str, Any]] = None,
    ) -> "ExecuteResult":
        """Export data to a storage backend.
        
        Args:
            source: Table name or SQL query object
            storage_key: Registered storage backend key
            file_path: Target path within the storage backend
            format: Export format ("parquet", "csv", "json", etc.)
            export_options: Database-specific export options
        """
        backend = storage_registry.get_backend(storage_key)
        
        # Use database-specific optimized export if available
        if self._can_export_directly(backend, format):
            return self._export_direct_to_storage(source, backend, file_path, format, export_options)
        
        # Fallback to query and upload
        return self._export_via_upload(source, backend, file_path, format, export_options)
    
    # Backward compatibility methods
    def copy_from_uri(self, table_name: str, uri: str, **kwargs) -> "ExecuteResult":
        """Legacy URI-based copy (for backward compatibility)."""
        # Parse URI and try to find matching registered backend or create temporary one
        # Implementation details...
    
    # ... other methods (copy_from_path, copy_from_arrow, etc.)
```

## 5. Configuration Examples

### 5.1. Application Configuration

```python
# Application setup
from sqlspec.config import SQLConfig, StorageConfig
from sqlspec.storage.registry import storage_registry

# Define storage backends
storage_config = StorageConfig(
    default_storage_key="primary_data",
    backends={
        "primary_data": {
            "backend_type": "obstore",
            "store_config": {
                "scheme": "s3",
                "bucket": "my-production-bucket",
                "region": "us-west-2",
            },
            "base_path": "data",
        },
        "staging_data": {
            "backend_type": "fsspec",
            "protocol": "s3",
            "fs_config": {
                "bucket": "my-staging-bucket",
                "key": "AKIA...",
                "secret": "...",
            },
            "base_path": "staging",
        },
        "local_dev": {
            "backend_type": "local",
            "base_path": "/tmp/sqlspec_data",
        },
    }
)

# Create SQL config with storage
sql_config = SQLConfig(storage=storage_config)

# Initialize driver (auto-registers storage backends)
driver = DuckDBDriver(sql_config)
```

### 5.2. Usage Examples

```python
# Simple usage with registered backends
driver.copy_from_storage("sales", "primary_data", "2024/Q1/sales.parquet")
driver.export_to_storage("SELECT * FROM analytics", "primary_data", "reports/monthly.csv")

# With options
driver.copy_from_storage(
    "orders", 
    "primary_data", 
    "orders.csv",
    strategy="replace",
    copy_options={"header": True, "delimiter": ","}
)

# Environment-specific usage (same key, different backends)
# In production: "data_store" -> S3 via obstore
# In development: "data_store" -> local filesystem
driver.copy_from_storage("users", "data_store", "users.parquet")
```

## 6. Database-Specific Optimizations

### 6.1. DuckDB Integration

```python
# DuckDB can often read directly from S3/GCS
def _can_copy_directly(self, backend: StorageBackendProtocol, format: str) -> bool:
    """Check if DuckDB can read directly from this backend."""
    if backend.backend_type in ("obstore", "fsspec"):
        base_uri = backend.base_uri
        if base_uri.startswith(("s3://", "gs://", "https://")) and format in ("parquet", "csv", "json"):
            return True
    return False

def _copy_direct_from_storage(self, table_name: str, backend: StorageBackendProtocol, 
                            file_path: str, format: str, copy_options: dict) -> "ExecuteResult":
    """Use DuckDB's native S3/GCS reading capabilities."""
    full_uri = f"{backend.base_uri}/{file_path}"
    copy_sql = f"COPY {table_name} FROM '{full_uri}' (FORMAT '{format}', {self._format_copy_options(copy_options)})"
    return self.execute_sql(copy_sql)
```

### 6.2. BigQuery Integration

```python
# BigQuery requires GCS staging for non-GCS sources
def _copy_direct_from_storage(self, table_name: str, backend: StorageBackendProtocol, 
                            file_path: str, format: str, copy_options: dict) -> "ExecuteResult":
    """Optimize BigQuery loads using GCS staging when needed."""
    if backend.base_uri.startswith("gs://"):
        # Direct GCS load
        return self._load_from_gcs(table_name, backend, file_path, format, copy_options)
    else:
        # Stage to GCS first
        temp_gcs_backend = storage_registry.get_backend("temp_gcs_staging")
        temp_path = f"temp/{uuid4()}/{file_path}"
        
        # Copy to GCS staging
        data = backend.read_bytes(file_path)
        temp_gcs_backend.write_bytes(temp_path, data)
        
        # Load from GCS
        result = self._load_from_gcs(table_name, temp_gcs_backend, temp_path, format, copy_options)
        
        # Cleanup
        temp_gcs_backend.delete(temp_path)
        
        return result
```

## 7. Advanced Features

### 7.1. Signed URL Support

```python
# Generate signed URLs for secure access
def get_signed_download_url(self, storage_key: str, file_path: str, expires_in: int = 3600) -> str:
    """Generate a signed URL for downloading a file."""
    backend = storage_registry.get_backend(storage_key)
    return backend.get_signed_url(file_path, operation="read", expires_in=expires_in)

def get_signed_upload_url(self, storage_key: str, file_path: str, expires_in: int = 3600) -> str:
    """Generate a signed URL for uploading a file."""
    backend = storage_registry.get_backend(storage_key)
    return backend.get_signed_url(file_path, operation="write", expires_in=expires_in)
```

### 7.2. Batch Operations

```python
def copy_multiple_from_storage(
    self,
    operations: list[dict[str, Any]],
    *,
    strategy: str = "append",
    parallel: bool = True,
) -> list["ExecuteResult"]:
    """Perform multiple copy operations efficiently."""
    # Implementation for batch operations
    pass
```

## 8. Testing Strategy

### 8.1. Unit Tests

- **StorageRegistry**: Test registration, retrieval, and configuration
- **Backend Implementations**: Mock underlying libraries (obstore, fsspec)
- **Mixin Methods**: Mock storage backends and database operations

### 8.2. Integration Tests with MinIO

```python
# tests/integration/test_storage/test_minio_integration.py
import pytest
from pytest_databases import minio

@pytest.fixture
def minio_storage_config(minio):
    """Create storage config using pytest-databases MinIO."""
    return {
        "backend_type": "fsspec",
        "protocol": "s3",
        "fs_config": {
            "endpoint_url": minio.get_connection_url(),
            "key": minio.access_key,
            "secret": minio.secret_key,
        },
        "base_path": "test-data",
    }

def test_copy_from_minio_storage(duckdb_driver, minio_storage_config):
    """Test copying data from MinIO using storage key."""
    # Register MinIO backend
    storage_registry.register_from_config("test_minio", minio_storage_config)
    
    # Upload test data to MinIO
    backend = storage_registry.get_backend("test_minio")
    test_data = "id,name\n1,Alice\n2,Bob\n"
    backend.write_text("test.csv", test_data)
    
    # Test copy operation
    result = duckdb_driver.copy_from_storage("users", "test_minio", "test.csv")
    assert result.success
    
    # Verify data was loaded
    rows = duckdb_driver.execute_sql("SELECT COUNT(*) FROM users").fetchone()
    assert rows[0] == 2
```

## 9. Migration Path

### 9.1. Backward Compatibility

- Keep existing URI-based methods as deprecated but functional
- Provide migration guide from URI-based to key-based approach
- Auto-registration of common backends for smoother transition

### 9.2. Configuration Migration

```python
# Helper to migrate from URI-based to key-based config
def migrate_uri_config_to_key_config(old_config: dict) -> dict:
    """Convert URI-based storage configs to key-based configs."""
    # Implementation to help users migrate
    pass
```

## 10. Documentation

### 10.1. Quick Start Guide

```python
# docs/usage/storage-quickstart.md

# 1. Configure storage backends
storage_config = StorageConfig(
    backends={
        "my_bucket": {
            "backend_type": "obstore",
            "store_config": {"scheme": "s3", "bucket": "my-data"},
        }
    }
)

# 2. Use in copy operations
driver.copy_from_storage("table", "my_bucket", "data.parquet")
driver.export_to_storage("SELECT * FROM table", "my_bucket", "export.csv")
```

### 10.2. Advanced Configuration

- Multiple backend types for the same storage
- Environment-specific configurations
- Performance optimization guides
- Security best practices

## 11. Future Considerations

- **Caching Layer**: Cache frequently accessed small files
- **Streaming Support**: Stream large files without full download
- **Compression**: Automatic compression/decompression
- **Monitoring**: Storage operation metrics and logging
- **Multi-Backend Operations**: Copy between different storage backends

This enhanced plan provides a more flexible, user-friendly approach to storage backend management while maintaining high performance and extensive customization options.
