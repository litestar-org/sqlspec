"""Unit tests for StorageRegistry.

This module tests the StorageRegistry including:
- URI-first access pattern with automatic backend detection
- ObStore preferred, FSSpec fallback architecture
- Scheme-based routing with dependency detection
- Named aliases for commonly used configurations
- Backend instance caching and retrieval
- Error handling for missing dependencies
- Configuration management
"""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.exceptions import ImproperConfigurationError, MissingDependencyError
from sqlspec.storage.registry import StorageRegistry

if TYPE_CHECKING:
    pass


# Test Fixtures
@pytest.fixture
def registry() -> StorageRegistry:
    """Create a fresh storage registry."""
    return StorageRegistry()


@pytest.fixture
def mock_obstore_backend() -> MagicMock:
    """Create a mock ObStore backend."""
    backend = MagicMock()
    backend.protocol = "s3"
    backend.backend_type = "obstore"
    return backend


@pytest.fixture
def mock_fsspec_backend() -> MagicMock:
    """Create a mock FSSpec backend."""
    backend = MagicMock()
    backend.protocol = "s3"
    backend.backend_type = "fsspec"
    return backend


# URI-First Access Tests
@pytest.mark.parametrize(
    "uri,expected_backend",
    [
        ("s3://bucket/path", "obstore"),
        ("gs://bucket/path", "obstore"),
        ("az://container/path", "obstore"),
        ("file:///local/path", "obstore"),
    ],
    ids=["s3", "gcs", "azure", "local_file"],
)
def test_uri_access_with_obstore(registry: StorageRegistry, uri: str, expected_backend: str) -> None:
    """Test URI-first access prefers ObStore when available."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_backend = MagicMock()
            mock_backend.backend_type = expected_backend
            mock_obstore_class.return_value = mock_backend

            backend = registry.get(uri)

            mock_obstore_class.assert_called_once_with(uri)
            assert backend == mock_backend


@pytest.mark.parametrize(
    "uri,expected_protocol",
    [
        ("s3://bucket/path", "s3"),
        ("gs://bucket/path", "gs"),
        ("az://container/path", "az"),
        ("http://example.com/file", "http"),
        ("ftp://server/file", "ftp"),
    ],
    ids=["s3", "gcs", "azure", "http", "ftp"],
)
def test_uri_access_fallback_to_fsspec(registry: StorageRegistry, uri: str, expected_protocol: str) -> None:
    """Test fallback to FSSpec when ObStore is not available."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", False):
        with patch("sqlspec.storage.registry.FSSPEC_INSTALLED", True):
            with patch("sqlspec.storage.backends.fsspec.FSSpecBackend") as mock_fsspec_class:
                mock_backend = MagicMock()
                mock_backend.protocol = expected_protocol
                mock_backend.backend_type = "fsspec"
                mock_fsspec_class.return_value = mock_backend

                backend = registry.get(uri)

                mock_fsspec_class.assert_called_once_with(uri)
                assert backend == mock_backend


def test_uri_access_no_dependencies(registry: StorageRegistry) -> None:
    """Test error when no backend dependencies are available."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", False):
        with patch("sqlspec.storage.registry.FSSPEC_INSTALLED", False):
            with pytest.raises(MissingDependencyError, match="No storage backend available"):
                registry.get("s3://bucket/path")


# Named Alias Tests
def test_register_alias_simple(registry: StorageRegistry) -> None:
    """Test registering a simple named alias."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        registry.register_alias("test-s3", uri="s3://test-bucket")

        assert "test-s3" in registry._aliases
        alias_config = registry._aliases["test-s3"]
        assert alias_config["uri"] == "s3://test-bucket"


def test_register_alias_with_config(registry: StorageRegistry) -> None:
    """Test registering an alias with configuration."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        registry.register_alias(
            "prod-s3",
            uri="s3://prod-bucket/data",
            base_path="sqlspec",
            region="us-east-1",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )

        assert "prod-s3" in registry._aliases
        alias_config = registry._aliases["prod-s3"]
        assert alias_config["uri"] == "s3://prod-bucket/data"
        assert alias_config["base_path"] == "sqlspec"
        assert alias_config["region"] == "us-east-1"
        assert alias_config["aws_access_key_id"] == "key"
        assert alias_config["aws_secret_access_key"] == "secret"


def test_get_with_alias(registry: StorageRegistry, mock_obstore_backend: MagicMock) -> None:
    """Test getting backend using a registered alias."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.return_value = mock_obstore_backend

            # Register alias
            registry.register_alias("test-alias", uri="s3://test-bucket", region="us-west-2")

            # Get backend using alias
            backend = registry.get("test-alias")

            mock_obstore_class.assert_called_once_with("s3://test-bucket", region="us-west-2")
            assert backend == mock_obstore_backend


def test_alias_not_found(registry: StorageRegistry) -> None:
    """Test error when alias is not found."""
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias: 'unknown-alias'"):
        registry.get("unknown-alias")


# Backend Instance Caching Tests
def test_backend_instance_caching(registry: StorageRegistry, mock_obstore_backend: MagicMock) -> None:
    """Test that backend instances are cached and reused."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.return_value = mock_obstore_backend

            # First access
            backend1 = registry.get("s3://bucket/path")
            # Second access with same URI
            backend2 = registry.get("s3://bucket/path")

            # Should only create one instance
            mock_obstore_class.assert_called_once()
            assert backend1 is backend2


def test_backend_instance_caching_with_kwargs(registry: StorageRegistry) -> None:
    """Test that backends with different kwargs get different instances."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            backend1 = MagicMock()
            backend2 = MagicMock()
            mock_obstore_class.side_effect = [backend1, backend2]

            # Different configurations should get different instances
            result1 = registry.get("s3://bucket", region="us-east-1")
            result2 = registry.get("s3://bucket", region="us-west-2")

            assert mock_obstore_class.call_count == 2
            assert result1 is not result2


# Scheme Detection Tests
@pytest.mark.parametrize(
    "uri,expected_scheme",
    [
        ("s3://bucket/path", "s3"),
        ("gs://bucket/path", "gs"),
        ("az://container/path", "az"),
        ("http://example.com", "http"),
        ("file:///local/path", "file"),
        ("/absolute/local/path", "file"),
        ("relative/path", "file"),
        ("C:\\Windows\\path", "file"),
    ],
    ids=["s3", "gcs", "azure", "http", "file_uri", "absolute_path", "relative_path", "windows_path"],
)
def test_scheme_detection(registry: StorageRegistry, uri: str, expected_scheme: str) -> None:
    """Test scheme detection from various URI formats."""
    scheme = registry._get_scheme(uri)
    assert scheme == expected_scheme


# ObStore Preferred Tests
@pytest.mark.parametrize(
    "scheme,obstore_available,fsspec_available,expected_backend",
    [
        ("s3", True, True, "obstore"),
        ("s3", False, True, "fsspec"),
        ("gs", True, True, "obstore"),
        ("az", True, True, "obstore"),
        ("http", True, True, "fsspec"),  # HTTP not supported by ObStore
        ("ftp", True, True, "fsspec"),  # FTP not supported by ObStore
        ("file", True, True, "obstore"),
    ],
    ids=["s3_both", "s3_fsspec_only", "gcs_both", "azure_both", "http_fsspec", "ftp_fsspec", "file_both"],
)
def test_backend_preference(
    registry: StorageRegistry, scheme: str, obstore_available: bool, fsspec_available: bool, expected_backend: str
) -> None:
    """Test that ObStore is preferred when available for supported schemes."""
    uri = f"{scheme}://test-resource"

    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", obstore_available):
        with patch("sqlspec.storage.registry.FSSPEC_INSTALLED", fsspec_available):
            if expected_backend == "obstore":
                with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
                    mock_backend = MagicMock()
                    mock_backend.backend_type = "obstore"
                    mock_obstore_class.return_value = mock_backend

                    result = registry.get(uri)

                    mock_obstore_class.assert_called_once()
                    assert result.backend_type == "obstore"  # type: ignore[attr-defined]
            else:
                with patch("sqlspec.storage.backends.fsspec.FSSpecBackend") as mock_fsspec_class:
                    mock_backend = MagicMock()
                    mock_backend.backend_type = "fsspec"
                    mock_fsspec_class.return_value = mock_backend

                    result = registry.get(uri)

                    mock_fsspec_class.assert_called_once()
                    assert result.backend_type == "fsspec"  # type: ignore[attr-defined]


# Clear Registry Tests
def test_clear_instances(registry: StorageRegistry, mock_obstore_backend: MagicMock) -> None:
    """Test clearing cached backend instances."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.return_value = mock_obstore_backend

            # Create cached instance
            registry.get("s3://bucket")
            assert len(registry._instances) == 1

            # Clear instances
            registry.clear_instances()
            assert len(registry._instances) == 0


def test_clear_aliases(registry: StorageRegistry) -> None:
    """Test clearing registered aliases."""
    registry.register_alias("test1", uri="s3://bucket1")
    registry.register_alias("test2", uri="s3://bucket2")
    assert len(registry._aliases) == 2

    registry.clear_aliases()
    assert len(registry._aliases) == 0


def test_clear_all(registry: StorageRegistry, mock_obstore_backend: MagicMock) -> None:
    """Test clearing both instances and aliases."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.return_value = mock_obstore_backend

            # Create instance and alias
            registry.register_alias("test", uri="s3://bucket")
            registry.get("s3://bucket")

            assert len(registry._aliases) == 1
            assert len(registry._instances) == 1

            registry.clear()
            assert len(registry._aliases) == 0
            assert len(registry._instances) == 0


# Error Handling Tests
def test_invalid_uri_format(registry: StorageRegistry) -> None:
    """Test handling of invalid URI formats."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.side_effect = Exception("Invalid URI")

            with pytest.raises(Exception, match="Invalid URI"):
                registry.get("://invalid")


def test_backend_initialization_error(registry: StorageRegistry) -> None:
    """Test handling of backend initialization errors."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.side_effect = Exception("Connection failed")

            with pytest.raises(Exception, match="Connection failed"):
                registry.get("s3://bucket")


# Configuration Override Tests
def test_kwargs_override_alias_config(registry: StorageRegistry, mock_obstore_backend: MagicMock) -> None:
    """Test that kwargs override alias configuration."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_obstore_class.return_value = mock_obstore_backend

            # Register alias with region
            registry.register_alias("test", uri="s3://bucket", region="us-east-1")

            # Get with different region
            registry.get("test", region="us-west-2")

            # Should use overridden region
            mock_obstore_class.assert_called_once_with("s3://bucket", region="us-west-2")


# Global Registry Tests
def test_global_registry_import() -> None:
    """Test that global registry can be imported."""
    from sqlspec.storage.registry import storage_registry

    assert isinstance(storage_registry, StorageRegistry)


def test_global_registry_usage() -> None:
    """Test using the global registry instance."""
    from sqlspec.storage.registry import storage_registry

    # Clear to ensure clean state
    storage_registry.clear()

    # Register an alias
    storage_registry.register_alias("test-global", uri="s3://global-bucket")

    assert "test-global" in storage_registry._aliases


# Edge Cases
def test_empty_uri(registry: StorageRegistry) -> None:
    """Test handling of empty URI."""
    with pytest.raises(ImproperConfigurationError, match="Unknown storage alias: ''"):
        registry.get("")


def test_none_uri(registry: StorageRegistry) -> None:
    """Test handling of None URI."""
    with pytest.raises(AttributeError):
        registry.get(None)  # type: ignore


@pytest.mark.parametrize(
    "path,expected_scheme",
    [(".", "file"), ("..", "file"), ("~", "file"), ("~/data", "file")],
    ids=["current_dir", "parent_dir", "home_dir", "home_subdir"],
)
def test_special_path_handling(registry: StorageRegistry, path: str, expected_scheme: str) -> None:
    """Test handling of special path formats."""
    scheme = registry._get_scheme(path)
    assert scheme == expected_scheme


# Backend Type Verification Tests
def test_backend_protocol_compliance(registry: StorageRegistry) -> None:
    """Test that returned backends comply with ObjectStoreProtocol."""
    with patch("sqlspec.storage.registry.OBSTORE_INSTALLED", True):
        with patch("sqlspec.storage.backends.obstore.ObStoreBackend") as mock_obstore_class:
            mock_backend = MagicMock()
            # Set required protocol attributes
            mock_backend.protocol = "s3"
            mock_backend.backend_type = "obstore"
            mock_backend.read_bytes = MagicMock()
            mock_backend.write_bytes = MagicMock()
            mock_backend.exists = MagicMock()
            mock_backend.delete = MagicMock()
            mock_backend.list_objects = MagicMock()

            mock_obstore_class.return_value = mock_backend

            backend = registry.get("s3://bucket")

            # Verify it has required methods
            assert hasattr(backend, "read_bytes")
            assert hasattr(backend, "write_bytes")
            assert hasattr(backend, "exists")
            assert hasattr(backend, "delete")
            assert hasattr(backend, "list_objects")
