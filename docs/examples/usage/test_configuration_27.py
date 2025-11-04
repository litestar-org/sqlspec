"""Test configuration example: Best practice - Clean up resources."""

import tempfile

import pytest


@pytest.mark.asyncio
async def test_cleanup_resources_best_practice() -> None:
    """Test resource cleanup best practice."""
    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        spec = SQLSpec()
        db = spec.add_config(
            AiosqliteConfig(pool_config={"database": tmp.name})
        )

        # Use the connection
        async with spec.provide_session(db) as session:
            await session.execute("CREATE TABLE test (id INTEGER)")

        # Clean up resources - important for async adapters
        await spec.close_all_pools()

        # Verify pools are closed
        assert db.pool_instance is None or not hasattr(db.pool_instance, "_pool")

