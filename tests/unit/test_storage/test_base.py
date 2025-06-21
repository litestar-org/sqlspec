"""Unit tests for ObjectStoreBase.

This module tests the ObjectStoreBase abstract base class including:
- Abstract method enforcement
- Protocol compliance verification
- Subclass implementation requirements
- Type hints and signatures
"""

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from sqlspec.storage.backends.base import ObjectStoreBase

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from sqlspec.typing import ArrowRecordBatch, ArrowTable


# Test Implementation
class ConcreteStore(ObjectStoreBase):
    """Concrete implementation for testing."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def read_bytes(self, path: str, **kwargs: Any) -> bytes:
        """Read bytes implementation."""
        return b"test data"

    def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Write bytes implementation."""
        pass

    def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Read text implementation."""
        return "test text"

    def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Write text implementation."""
        pass

    def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """List objects implementation."""
        return ["file1.txt", "file2.txt"]

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Exists implementation."""
        return True

    def delete(self, path: str, **kwargs: Any) -> None:
        """Delete implementation."""
        pass

    def copy(self, source: str, destination: str, **kwargs: Any) -> None:
        """Copy implementation."""
        pass

    def move(self, source: str, destination: str, **kwargs: Any) -> None:
        """Move implementation."""
        pass

    def glob(self, pattern: str, **kwargs: Any) -> list[str]:
        """Glob implementation."""
        return ["matched1.txt", "matched2.txt"]

    def is_object(self, path: str, **kwargs: Any) -> bool:
        """Is object implementation."""
        return True

    def is_path(self, path: str, **kwargs: Any) -> bool:
        """Is path implementation."""
        return False

    def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Get metadata implementation."""
        return {"size": 1024, "modified": "2024-01-01"}

    # Async methods
    async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
        """Async read bytes implementation."""
        return b"async test data"

    async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
        """Async write bytes implementation."""
        pass

    async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
        """Async read text implementation."""
        return "async test text"

    async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
        """Async write text implementation."""
        pass

    async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
        """Async list objects implementation."""
        return ["async_file1.txt", "async_file2.txt"]

    async def exists_async(self, path: str, **kwargs: Any) -> bool:
        """Async exists implementation."""
        return True

    async def delete_async(self, path: str, **kwargs: Any) -> None:
        """Async delete implementation."""
        pass

    # Arrow methods
    def read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Read arrow implementation."""
        return MagicMock()

    def write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Write arrow implementation."""
        pass

    def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
        """Stream arrow implementation."""
        yield MagicMock()

    async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
        """Async stream arrow implementation."""
        yield MagicMock()

    async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async copy implementation."""
        pass

    async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
        """Async move implementation."""
        pass

    async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
        """Async get metadata implementation."""
        return {"size": 1024, "modified": "2024-01-01"}

    async def read_arrow_async(self, path: str, **kwargs: Any) -> "ArrowTable":
        """Async read arrow implementation."""
        return MagicMock()

    async def write_arrow_async(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
        """Async write arrow implementation."""
        pass


# Abstract Method Tests
def test_abstract_base_cannot_be_instantiated() -> None:
    """Test that ObjectStoreBase cannot be instantiated directly."""
    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        ObjectStoreBase()  # type: ignore


def test_incomplete_implementation_fails() -> None:
    """Test that incomplete implementations fail to instantiate."""

    class IncompleteStore(ObjectStoreBase):
        """Incomplete implementation missing required methods."""

        def read_bytes(self, path: str, **kwargs: Any) -> bytes:
            return b""

        # Missing other required methods

    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        IncompleteStore()  # type: ignore[abstract]


# Implementation Tests
def test_concrete_implementation_works() -> None:
    """Test that a complete implementation can be instantiated."""
    store = ConcreteStore()  # type: ignore[abstract]
    assert isinstance(store, ObjectStoreBase)


def test_concrete_implementation_methods() -> None:
    """Test that concrete implementation methods work correctly."""
    store = ConcreteStore()  # type: ignore[abstract]

    # Test sync methods
    assert store.read_bytes("test.txt") == b"test data"
    assert store.read_text("test.txt") == "test text"
    assert store.list_objects() == ["file1.txt", "file2.txt"]
    assert store.exists("test.txt") is True
    assert store.glob("*.txt") == ["matched1.txt", "matched2.txt"]
    assert store.is_object("test.txt") is True
    assert store.is_path("dir/") is False
    assert store.get_metadata("test.txt") == {"size": 1024, "modified": "2024-01-01"}

    # Test methods that return None
    store.write_bytes("test.txt", b"data")
    store.write_text("test.txt", "text")
    store.delete("test.txt")
    store.copy("src.txt", "dst.txt")
    store.move("old.txt", "new.txt")


@pytest.mark.asyncio
async def test_concrete_async_methods() -> None:
    """Test that concrete async implementation methods work correctly."""
    store = ConcreteStore()  # type: ignore[abstract]

    # Test async methods
    assert await store.read_bytes_async("test.txt") == b"async test data"
    assert await store.read_text_async("test.txt") == "async test text"
    assert await store.list_objects_async() == ["async_file1.txt", "async_file2.txt"]
    assert await store.exists_async("test.txt") is True

    # Test async methods that return None
    await store.write_bytes_async("test.txt", b"data")
    await store.write_text_async("test.txt", "text")
    await store.delete_async("test.txt")


def test_concrete_arrow_methods() -> None:
    """Test that concrete arrow implementation methods work correctly."""
    store = ConcreteStore()  # type: ignore[abstract]

    # Test arrow methods
    table = store.read_arrow("data.parquet")
    assert table is not None

    store.write_arrow("data.parquet", MagicMock())

    # Test streaming
    batches = list(store.stream_arrow("*.parquet"))
    assert len(batches) == 1


@pytest.mark.asyncio
async def test_concrete_arrow_async_streaming() -> None:
    """Test async arrow streaming."""
    store = ConcreteStore()  # type: ignore[abstract]

    batches = [batch async for batch in store.stream_arrow_async("*.parquet")]

    assert len(batches) == 1


# Method Signature Tests
def test_required_method_signatures() -> None:
    """Test that required methods have correct signatures."""
    required_methods = ["read_bytes", "write_bytes", "read_text", "write_text", "list_objects", "exists"]

    for method_name in required_methods:
        assert hasattr(ObjectStoreBase, method_name)
        method = getattr(ObjectStoreBase, method_name)
        assert callable(method)


def test_method_kwargs_support() -> None:
    """Test that methods support **kwargs for extensibility."""
    store = ConcreteStore()  # type: ignore[abstract]

    # All methods should accept arbitrary kwargs
    store.read_bytes("test.txt", custom_option=True)
    store.write_bytes("test.txt", b"data", compression="gzip")
    store.read_text("test.txt", encoding="utf-8", cache=True)
    store.write_text("test.txt", "text", encoding="utf-8", overwrite=True)
    store.list_objects(prefix="", recursive=True, max_results=100)
    store.exists("test.txt", check_cache=True)
    store.delete("test.txt", force=True)
    store.copy("src.txt", "dst.txt", preserve_metadata=True)
    store.move("old.txt", "new.txt", atomic=True)
    store.glob("*.txt", case_sensitive=False)
    store.is_object("test.txt", follow_links=True)
    store.is_path("dir/", resolve=True)
    store.get_metadata("test.txt", include_custom=True)


# Inheritance Tests
def test_multiple_inheritance_allowed() -> None:
    """Test that ObjectStoreBase can be used with multiple inheritance."""

    class Mixin:
        """Test mixin class."""

        def mixin_method(self) -> str:
            return "mixin"

    class MultiStore(ConcreteStore, Mixin):
        """Store with multiple inheritance."""

        pass

    store = MultiStore()  # type: ignore[abstract]
    assert isinstance(store, ObjectStoreBase)
    assert isinstance(store, Mixin)
    assert store.mixin_method() == "mixin"
    assert store.read_bytes("test.txt") == b"test data"


def test_super_calls_work() -> None:
    """Test that super() calls work in subclasses."""

    class ExtendedStore(ConcreteStore):
        """Extended store that calls super()."""

        def read_bytes(self, path: str, **kwargs: Any) -> bytes:
            """Extended read bytes."""
            result = super().read_bytes(path, **kwargs)
            return b"extended: " + result

    store = ExtendedStore()  # type: ignore[abstract]
    assert store.read_bytes("test.txt") == b"extended: test data"


# Type Annotation Tests
def test_type_annotations_present() -> None:
    """Test that methods have proper type annotations."""
    import inspect

    # Check a few key methods
    read_bytes_sig = inspect.signature(ObjectStoreBase.read_bytes)
    # With __future__ annotations, these become strings
    assert read_bytes_sig.return_annotation == "bytes"

    exists_sig = inspect.signature(ObjectStoreBase.exists)
    assert exists_sig.return_annotation == "bool"

    list_objects_sig = inspect.signature(ObjectStoreBase.list_objects)
    assert list_objects_sig.return_annotation == "list[str]"


# Edge Cases
def test_empty_implementation_with_notimplemented() -> None:
    """Test implementation that raises NotImplementedError."""

    class NotImplementedStore(ObjectStoreBase):
        """Store that raises NotImplementedError for all methods."""

        def read_bytes(self, path: str, **kwargs: Any) -> bytes:
            raise NotImplementedError("read_bytes not implemented")

        def write_bytes(self, path: str, data: bytes, **kwargs: Any) -> None:
            raise NotImplementedError("write_bytes not implemented")

        def read_text(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
            raise NotImplementedError("read_text not implemented")

        def write_text(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
            raise NotImplementedError("write_text not implemented")

        def list_objects(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
            raise NotImplementedError("list_objects not implemented")

        def exists(self, path: str, **kwargs: Any) -> bool:
            raise NotImplementedError("exists not implemented")

        def delete(self, path: str, **kwargs: Any) -> None:
            raise NotImplementedError("delete not implemented")

        def copy(self, source: str, destination: str, **kwargs: Any) -> None:
            raise NotImplementedError("copy not implemented")

        def move(self, source: str, destination: str, **kwargs: Any) -> None:
            raise NotImplementedError("move not implemented")

        def glob(self, pattern: str, **kwargs: Any) -> list[str]:
            raise NotImplementedError("glob not implemented")

        def is_object(self, path: str, **kwargs: Any) -> bool:
            raise NotImplementedError("is_object not implemented")

        def is_path(self, path: str, **kwargs: Any) -> bool:
            raise NotImplementedError("is_path not implemented")

        def get_metadata(self, path: str, **kwargs: Any) -> dict[str, Any]:
            raise NotImplementedError("get_metadata not implemented")

        # Async methods
        async def read_bytes_async(self, path: str, **kwargs: Any) -> bytes:
            raise NotImplementedError("read_bytes_async not implemented")

        async def write_bytes_async(self, path: str, data: bytes, **kwargs: Any) -> None:
            raise NotImplementedError("write_bytes_async not implemented")

        async def read_text_async(self, path: str, encoding: str = "utf-8", **kwargs: Any) -> str:
            raise NotImplementedError("read_text_async not implemented")

        async def write_text_async(self, path: str, data: str, encoding: str = "utf-8", **kwargs: Any) -> None:
            raise NotImplementedError("write_text_async not implemented")

        async def list_objects_async(self, prefix: str = "", recursive: bool = True, **kwargs: Any) -> list[str]:
            raise NotImplementedError("list_objects_async not implemented")

        async def exists_async(self, path: str, **kwargs: Any) -> bool:
            raise NotImplementedError("exists_async not implemented")

        async def delete_async(self, path: str, **kwargs: Any) -> None:
            raise NotImplementedError("delete_async not implemented")

        async def copy_async(self, source: str, destination: str, **kwargs: Any) -> None:
            raise NotImplementedError("copy_async not implemented")

        async def move_async(self, source: str, destination: str, **kwargs: Any) -> None:
            raise NotImplementedError("move_async not implemented")

        async def get_metadata_async(self, path: str, **kwargs: Any) -> dict[str, Any]:
            raise NotImplementedError("get_metadata_async not implemented")

        # Arrow methods
        def read_arrow(self, path: str, **kwargs: Any) -> "ArrowTable":
            raise NotImplementedError("read_arrow not implemented")

        def write_arrow(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
            raise NotImplementedError("write_arrow not implemented")

        def stream_arrow(self, pattern: str, **kwargs: Any) -> "Iterator[ArrowRecordBatch]":
            raise NotImplementedError("stream_arrow not implemented")

        async def stream_arrow_async(self, pattern: str, **kwargs: Any) -> "AsyncIterator[ArrowRecordBatch]":
            raise NotImplementedError("stream_arrow_async not implemented")
            # Make it an async generator to satisfy the return type
            yield  # type: ignore[unreachable]

        async def read_arrow_async(self, path: str, **kwargs: Any) -> "ArrowTable":
            raise NotImplementedError("read_arrow_async not implemented")

        async def write_arrow_async(self, path: str, table: "ArrowTable", **kwargs: Any) -> None:
            raise NotImplementedError("write_arrow_async not implemented")

    store = NotImplementedStore()  # type: ignore[abstract]

    with pytest.raises(NotImplementedError, match="read_bytes not implemented"):
        store.read_bytes("test.txt")

    with pytest.raises(NotImplementedError, match="exists not implemented"):
        store.exists("test.txt")
