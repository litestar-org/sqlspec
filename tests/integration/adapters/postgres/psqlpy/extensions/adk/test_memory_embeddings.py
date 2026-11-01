"""Native vector preservation during batched psqlpy memory insertion."""

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psqlpy.adk import PsqlpyADKMemoryStore

if TYPE_CHECKING:
    from sqlspec.extensions.adk import StoredMemory

pytestmark = pytest.mark.xdist_group("pgvector")


@pytest.mark.parametrize("with_owner", [False, True])
async def test_bulk_memory_insertion_preserves_embeddings(
    pgvector_config_psqlpy: PsqlpyConfig, with_owner: bool
) -> None:
    """Vectors and nulls remain aligned with their rows, including owner-scoped batches."""
    config = PsqlpyConfig(
        connection_config=dict(pgvector_config_psqlpy.connection_config),
        extension_config={
            "adk": {
                "memory_table": f"memory_vectors_{uuid4().hex}",
                "vector_dimensions": 2,
                "vector_index_type": "none",
                "owner_id_column": "tenant_id VARCHAR(128)" if with_owner else None,
            }
        },
    )
    store = PsqlpyADKMemoryStore(config)
    now = datetime.now(UTC)
    entries: list[StoredMemory] = [
        {
            "id": f"memory-{index}",
            "session_id": "session",
            "app_name": "app",
            "user_id": "user",
            "scope": "user",
            "event_id": f"event-{index}",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "memory"},
            "content_text": "memory",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": embedding,
        }
        for index, embedding in enumerate(((1.0, 0.0), None, (0.0, 1.0)))
    ]
    try:
        await store.create_tables()
        assert await store.insert_memory_entries(entries, owner_id="tenant") == 3
        assert await store.insert_memory_entries(entries, owner_id="tenant") == 0
        results = await store.search_entries("", "app", "user", embedding=(1.0, 0.0))
        assert [row["id"] for row in results] == ["memory-0", "memory-2"]
        assert [row["embedding"] for row in results] == [[1.0, 0.0], [0.0, 1.0]]
    finally:
        await store.drop_tables()
        await config.close_pool()
