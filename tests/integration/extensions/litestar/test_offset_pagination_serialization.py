"""Integration tests for OffsetPagination serialization in Litestar extension."""

import tempfile

import pytest
from litestar import Litestar, get
from litestar.testing import TestClient

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.core.filters import OffsetPagination
from sqlspec.extensions.litestar import SQLSpecPlugin
from sqlspec.typing import LITESTAR_INSTALLED

pytestmark = pytest.mark.xdist_group("sqlite")

if not LITESTAR_INSTALLED:
    pytest.skip("Litestar not installed", allow_module_level=True)


def test_litestar_offset_pagination_serialization() -> None:
    """OffsetPagination should serialize with SQLSpec's Litestar encoder."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        sql = SQLSpec()
        config = AiosqliteConfig(connection_config={"database": tmp.name})
        sql.add_config(config)

        @get("/pagination")
        def get_pagination() -> OffsetPagination[dict[str, int]]:
            return OffsetPagination([{"id": 1}], limit=10, offset=0, total=1)

        app = Litestar(route_handlers=[get_pagination], plugins=[SQLSpecPlugin(sqlspec=sql)])

        with TestClient(app=app) as client:
            response = client.get("/pagination")
            assert response.status_code == 200
            assert response.json() == {"items": [{"id": 1}], "limit": 10, "offset": 0, "total": 1}
