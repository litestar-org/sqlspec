"""Own asyncpg release work until shielded native reset has acknowledged completion."""

import asyncio
from collections.abc import Awaitable

__all__ = ("await_owned_release",)


async def await_owned_release(operation: Awaitable[None]) -> None:
    """Join a native release parent before allowing connection invalidation.

    The SDK release must receive a timeout from its adapter. Cancellation of
    this caller cannot cancel that parent while its SDK-shielded child still
    runs. Cooperative reset callbacks acknowledge the SDK timeout; callbacks
    suppressing cancellation require acknowledgement before holder reuse.
    """
    task = asyncio.ensure_future(operation)
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if not task.cancelled():
                cancellation = exc
        except BaseException:
            break
    task.result()
    if cancellation is not None:
        raise cancellation
