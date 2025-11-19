# start-example
import asyncio
from contextvars import ContextVar

db_session: ContextVar = ContextVar('db_session', default=None)

async def get_session():
    session = db_session.get()
    if session is None:
        session = await spec.provide_session(config).__aenter__()
        db_session.set(session)
    return session

async def cleanup_session():
    session = db_session.get()
    if session:
        await session.__aexit__(None, None, None)
        db_session.set(None)
# end-example

def test_stub():
    assert True
