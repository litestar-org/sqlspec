"""Test configuration example: Best practice - Clean up resources."""

import pytest

__all__ = ("test_cleanup_resources_best_practice",)


@pytest.mark.asyncio
async def test_cleanup_resources_best_practice() -> None:
    """Test resource cleanup best practice."""
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        db_manager = SQLSpec()
        db = db_manager.add_config(AiosqliteConfig(pool_config={"database": tmp.name}))

        # Use the connection
        async with db_manager.provide_session(db) as session:
            await session.execute("CREATE TABLE test (id INTEGER)")

        # Clean up resources - important for async adapters
        await db_manager.close_all_pools()

        # Verify pools are closed
        assert db.pool_instance is None or not hasattr(db.pool_instance, "_pool")
