"""Test configuration example: Best practice - Clean up resources."""

__all__ = ("test_cleanup_resources_best_practice",)


import pytest


@pytest.mark.asyncio
async def test_cleanup_resources_best_practice() -> None:
    """Test resource cleanup best practice."""
    # start-example
    import tempfile

    from sqlspec import SQLSpec
    from sqlspec.adapters.aiosqlite import AiosqliteConfig

    with tempfile.NamedTemporaryFile(suffix=".db", delete=True) as tmp:
        db_manager = SQLSpec()
        db = db_manager.add_config(AiosqliteConfig(connection_config={"database": tmp.name}))

        # Use the connection
        async with db_manager.provide_session(db) as session:
            await session.execute("CREATE TABLE test (id INTEGER)")

        # Clean up resources - important for async adapters
        await db_manager.close_all_pools()

        # Verify pools are closed
        # end-example
        assert db.connection_instance is None or not hasattr(db.connection_instance, "_pool")
