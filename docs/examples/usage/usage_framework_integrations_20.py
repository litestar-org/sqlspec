# start-example
from contextvars import ContextVar

__all__ = ("cleanup_session", "get_session", "test_stub")


db_session: ContextVar = ContextVar("db_session", default=None)


async def get_session():
    session = db_session.get()
    if session is None:
        session = await spec.provide_session(config).__aenter__()
        db_session.set(session)
    return session


async def cleanup_session() -> None:
    session = db_session.get()
    if session:
        await session.__aexit__(None, None, None)
        db_session.set(None)


# end-example


def test_stub() -> None:
    assert True
