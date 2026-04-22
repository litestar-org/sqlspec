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


def test_litestar_offset_pagination_openapi_schema() -> None:
    """OffsetPagination[T] must register T as an OpenAPI component (regression for #419)."""
    import msgspec

    class Item(msgspec.Struct):
        name: str

    @get("/items")
    async def list_items() -> OffsetPagination[Item]:
        return OffsetPagination(items=[Item(name="a")], limit=10, offset=0, total=1)

    sql = SQLSpec()
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    sql.add_config(config)
    app = Litestar(route_handlers=[list_items], plugins=[SQLSpecPlugin(sqlspec=sql)])

    schema = app.openapi_schema
    component_names = set(schema.components.schemas.keys())
    assert any("Item" in name for name in component_names), f"Item not in OpenAPI components: {component_names}"

    response = schema.paths["/items"].get.responses["200"]
    media = response.content["application/json"]
    assert media.schema is not None, "response media schema is None"

    schema_dict = media.schema.to_schema() if hasattr(media.schema, "to_schema") else media.schema
    assert schema_dict, f"OpenAPI response schema empty for OffsetPagination[Item]: {schema_dict!r}"
