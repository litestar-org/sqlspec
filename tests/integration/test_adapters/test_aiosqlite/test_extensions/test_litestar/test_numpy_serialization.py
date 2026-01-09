"""Integration tests for NumPy array serialization in Litestar plugin.

Tests automatic NumPy array encoding/decoding in HTTP request/response cycles.
"""

import tempfile

import pytest

from sqlspec.base import SQLSpec
from sqlspec.typing import LITESTAR_INSTALLED, NUMPY_INSTALLED

if not LITESTAR_INSTALLED or not NUMPY_INSTALLED:
    pytest.skip("Litestar or NumPy not installed", allow_module_level=True)

import numpy as np
from litestar import Litestar, get, post
from litestar.datastructures import State
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin

pytestmark = [pytest.mark.integration, pytest.mark.aiosqlite, pytest.mark.xdist_group("sqlite")]


def test_litestar_numpy_encoder_registered() -> None:
    """Test that NumPy encoder is automatically registered when NumPy installed."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        app = Litestar(route_handlers=[], plugins=[SQLSpecPlugin(sql)])

        assert app.type_encoders is not None
        assert np.ndarray in app.type_encoders


def test_litestar_numpy_decoder_registered() -> None:
    """Test that NumPy decoder is automatically registered when NumPy installed."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        app = Litestar(route_handlers=[], plugins=[SQLSpecPlugin(sql)])

        assert app.type_decoders is not None
        assert len(app.type_decoders) > 0


def test_litestar_numpy_response_encoding() -> None:
    """Test that NumPy arrays in responses are automatically encoded to JSON."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @get("/vector")
        def get_vector(state: State) -> dict[str, int | list[float]]:
            embedding = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
            return {"id": 1, "embedding": embedding, "dimensions": len(embedding)}  # type: ignore[dict-item]

        app = Litestar(route_handlers=[get_vector], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            response = client.get("/vector")

            assert response.status_code == HTTP_200_OK

            data = response.json()
            assert data["id"] == 1
            assert data["embedding"] == [0.1, 0.2, 0.3, 0.4, 0.5]
            assert data["dimensions"] == 5


def test_litestar_numpy_request_decoding() -> None:
    """Test that type decoders are registered (decoding happens at type boundaries)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @post("/vector")
        def create_vector(data: dict[str, int | list[float]], state: State) -> dict[str, bool]:
            embedding_list = data.get("embedding", [])
            assert isinstance(embedding_list, list)
            assert len(embedding_list) == 3

            return {"success": True}

        app = Litestar(route_handlers=[create_vector], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            response = client.post("/vector", json={"id": 1, "embedding": [1.0, 2.0, 3.0]})

            assert response.status_code == HTTP_201_CREATED
            assert response.json() == {"success": True}


def test_litestar_numpy_round_trip() -> None:
    """Test full round-trip of NumPy array through request/response cycle."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @post("/echo")
        def echo_vector(data: dict[str, int | list[float]], state: State) -> dict[str, int | list[float]]:
            embedding_list = data.get("embedding", [])
            embedding_array = np.array(embedding_list)

            return {
                "id": data["id"],
                "embedding": embedding_array,  # type: ignore[dict-item]
                "dimensions": len(embedding_array),
            }

        app = Litestar(route_handlers=[echo_vector], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            original_embedding = [0.5, 1.5, 2.5, 3.5]
            response = client.post("/echo", json={"id": 42, "embedding": original_embedding})

            assert response.status_code == HTTP_201_CREATED

            data = response.json()
            assert data["id"] == 42
            assert data["embedding"] == original_embedding
            assert data["dimensions"] == 4


def test_litestar_numpy_multidimensional_arrays() -> None:
    """Test encoding of multi-dimensional NumPy arrays."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @get("/matrix")
        def get_matrix(state: State) -> dict[str, list[list[int]]]:
            matrix = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            return {"matrix": matrix}  # type: ignore[dict-item]

        app = Litestar(route_handlers=[get_matrix], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            response = client.get("/matrix")

            assert response.status_code == HTTP_200_OK

            data = response.json()
            assert data["matrix"] == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]


def test_litestar_numpy_empty_array() -> None:
    """Test encoding of empty NumPy arrays."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @get("/empty")
        def get_empty(state: State) -> dict[str, list[int]]:
            empty_arr = np.array([])
            return {"data": empty_arr}  # type: ignore[dict-item]

        app = Litestar(route_handlers=[get_empty], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            response = client.get("/empty")

            assert response.status_code == HTTP_200_OK

            data = response.json()
            assert data["data"] == []


def test_litestar_numpy_various_dtypes() -> None:
    """Test encoding of NumPy arrays with various dtypes."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @get("/dtypes")
        def get_dtypes(state: State) -> dict[str, list[float]]:
            float32_arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
            float64_arr = np.array([4.0, 5.0, 6.0], dtype=np.float64)
            int64_arr = np.array([7, 8, 9], dtype=np.int64)

            return {
                "float32": float32_arr,  # type: ignore[dict-item]
                "float64": float64_arr,  # type: ignore[dict-item]
                "int64": int64_arr,  # type: ignore[dict-item]
            }

        app = Litestar(route_handlers=[get_dtypes], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            response = client.get("/dtypes")

            assert response.status_code == HTTP_200_OK

            data = response.json()
            assert data["float32"] == [1.0, 2.0, 3.0]
            assert data["float64"] == [4.0, 5.0, 6.0]
            assert data["int64"] == [7, 8, 9]


def test_litestar_numpy_large_embedding_vector() -> None:
    """Test encoding of large embedding vectors (common in ML workflows)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(
            connection_config={"database": tmp.name}, extension_config={"litestar": {"commit_mode": "manual"}}
        )
        sql.add_config(config)

        @get("/embedding")
        def get_large_embedding(state: State) -> dict[str, int | list[float]]:
            embedding_768 = np.random.rand(768).astype(np.float32)  # noqa: NPY002
            return {"id": 1, "embedding": embedding_768, "dimensions": len(embedding_768)}  # type: ignore[dict-item]

        app = Litestar(route_handlers=[get_large_embedding], plugins=[SQLSpecPlugin(sql)])

        with TestClient(app) as client:
            response = client.get("/embedding")

            assert response.status_code == HTTP_200_OK

            data = response.json()
            assert data["id"] == 1
            assert len(data["embedding"]) == 768
            assert data["dimensions"] == 768
            assert all(isinstance(x, float) for x in data["embedding"])
