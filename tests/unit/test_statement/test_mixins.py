"""Unit tests for driver mixins.

Tests the mixin classes that provide additional functionality for database drivers,
including SQL translation and unified storage operations.
"""

from unittest.mock import MagicMock

from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, SyncStorageMixin


class TestSQLTranslatorMixin:
    """Test SQLTranslatorMixin functionality."""

    def test_sql_translator_mixin_import(self) -> None:
        """Test that SQLTranslatorMixin can be imported."""
        assert SQLTranslatorMixin is not None
        # SQLTranslatorMixin is tested more thoroughly in test_sql_translator_mixin.py


class TestStorageMixins:
    """Test unified storage mixins."""

    def test_sync_storage_mixin_import(self) -> None:
        """Test that SyncStorageMixin can be imported."""
        assert SyncStorageMixin is not None

    def test_async_storage_mixin_import(self) -> None:
        """Test that AsyncStorageMixin can be imported."""
        assert AsyncStorageMixin is not None

    def test_storage_mixin_base_methods(self) -> None:
        """Test storage mixin base functionality."""

        # Create a mock driver class that includes the mixin
        class MockDriver(SyncStorageMixin):
            def __init__(self) -> None:
                self.config = MagicMock()
                self._connection = MagicMock()

        driver = MockDriver()

        # Test URI detection
        assert driver._is_uri("s3://bucket/key")
        assert driver._is_uri("file:///path/to/file")
        assert driver._is_uri("/absolute/path")
        assert not driver._is_uri("relative/path")
        assert not driver._is_uri("just_a_file.txt")


class TestMixinIntegration:
    """Test integration between different mixins."""

    def test_multiple_mixin_inheritance(self) -> None:
        """Test that a driver can inherit from multiple mixins."""

        class MockDriver(SQLTranslatorMixin, SyncStorageMixin):
            def __init__(self) -> None:
                self.dialect = "sqlite"
                self.config = MagicMock()
                self._connection = MagicMock()

        driver = MockDriver()
        assert hasattr(driver, "convert_to_dialect")  # From SQLTranslatorMixin
        assert hasattr(driver, "_is_uri")  # From SyncStorageMixin
        assert driver.dialect == "sqlite"
