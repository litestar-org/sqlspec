"""Litestar ADK Integration Example with AsyncPG.

This example demonstrates how to integrate Google ADK session storage
with a Litestar web application using PostgreSQL (AsyncPG).

Features:
    - SQLSpecSessionService as a dependency
    - RESTful API endpoints for session management
    - Automatic table creation on startup
    - Health check endpoint

Requirements:
    - PostgreSQL running locally (default port 5432)
    - pip install sqlspec[asyncpg,adk,litestar] google-genai litestar[standard]

Usage:
    python docs/examples/adk_litestar_asyncpg.py

    Then test with:
        curl http://localhost:8000/health
        curl -X POST http://localhost:8000/sessions -H "Content-Type: application/json" \
             -d '{"app_name":"chatbot","user_id":"alice","state":{"theme":"dark"}}'
        curl http://localhost:8000/sessions/chatbot/alice
"""

from datetime import datetime, timezone
from typing import Any

from google.adk.events.event import Event
from google.genai import types
from litestar import Litestar, get, post
from litestar.datastructures import State
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from msgspec import Struct

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore
from sqlspec.extensions.adk import SQLSpecSessionService

__all__ = ("app", "main")


class CreateSessionRequest(Struct):
    """Request model for creating a session."""

    app_name: str
    user_id: str
    state: dict[str, Any] = {}


class AddEventRequest(Struct):
    """Request model for adding an event to a session."""

    author: str
    text: str


class SessionResponse(Struct):
    """Response model for session data."""

    id: str
    app_name: str
    user_id: str
    state: dict[str, Any]
    event_count: int
    last_update_time: str


async def get_adk_service(state: State) -> SQLSpecSessionService:
    """Dependency injection provider for ADK service.

    Args:
        state: Litestar application state.

    Returns:
        SQLSpecSessionService instance.
    """
    return state.adk_service


@get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint.

    Returns:
        Status information.
    """
    return {"status": "healthy", "service": "ADK Session API", "timestamp": datetime.now(timezone.utc).isoformat()}


@post("/sessions", status_code=HTTP_201_CREATED)
async def create_session(
    data: CreateSessionRequest, adk_service: SQLSpecSessionService
) -> dict[str, Any]:
    """Create a new ADK session.

    Args:
        data: Session creation request.
        adk_service: ADK session service (injected).

    Returns:
        Created session information.
    """
    session = await adk_service.create_session(app_name=data.app_name, user_id=data.user_id, state=data.state)

    return {
        "id": session.id,
        "app_name": session.app_name,
        "user_id": session.user_id,
        "state": session.state,
        "created_at": datetime.fromtimestamp(session.last_update_time, tz=timezone.utc).isoformat(),
    }


@get("/sessions/{app_name:str}/{user_id:str}")
async def list_sessions(app_name: str, user_id: str, adk_service: SQLSpecSessionService) -> dict[str, Any]:
    """List all sessions for a user in an app.

    Args:
        app_name: Application name.
        user_id: User identifier.
        adk_service: ADK session service (injected).

    Returns:
        List of sessions.
    """
    response = await adk_service.list_sessions(app_name=app_name, user_id=user_id)

    sessions = [
        SessionResponse(
            id=s.id,
            app_name=s.app_name,
            user_id=s.user_id,
            state=s.state,
            event_count=len(s.events),
            last_update_time=datetime.fromtimestamp(s.last_update_time, tz=timezone.utc).isoformat(),
        )
        for s in response.sessions
    ]

    return {"sessions": [s.__dict__ for s in sessions], "count": len(sessions)}


@get("/sessions/{app_name:str}/{user_id:str}/{session_id:str}")
async def get_session(
    app_name: str, user_id: str, session_id: str, adk_service: SQLSpecSessionService
) -> dict[str, Any]:
    """Retrieve a specific session with its events.

    Args:
        app_name: Application name.
        user_id: User identifier.
        session_id: Session identifier.
        adk_service: ADK session service (injected).

    Returns:
        Session with full event history.
    """
    session = await adk_service.get_session(app_name=app_name, user_id=user_id, session_id=session_id)

    if not session:
        return {"error": "Session not found"}, HTTP_200_OK

    events = [
        {
            "id": e.id,
            "author": e.author,
            "timestamp": datetime.fromtimestamp(e.timestamp, tz=timezone.utc).isoformat(),
            "content": e.content.parts[0].text if e.content and e.content.parts else None,
        }
        for e in session.events
    ]

    return {
        "id": session.id,
        "app_name": session.app_name,
        "user_id": session.user_id,
        "state": session.state,
        "events": events,
        "event_count": len(events),
    }


@post("/sessions/{app_name:str}/{user_id:str}/{session_id:str}/events", status_code=HTTP_201_CREATED)
async def add_event(
    app_name: str, user_id: str, session_id: str, data: AddEventRequest, adk_service: SQLSpecSessionService
) -> dict[str, str]:
    """Add an event to a session.

    Args:
        app_name: Application name.
        user_id: User identifier.
        session_id: Session identifier.
        data: Event data.
        adk_service: ADK session service (injected).

    Returns:
        Event creation confirmation.
    """
    session = await adk_service.get_session(app_name=app_name, user_id=user_id, session_id=session_id)

    if not session:
        return {"error": "Session not found"}

    event = Event(
        id=f"evt_{datetime.now(timezone.utc).timestamp()}",
        invocation_id=f"inv_{len(session.events) + 1}",
        author=data.author,
        branch="main",
        actions=[],
        timestamp=datetime.now(timezone.utc).timestamp(),
        content=types.Content(parts=[types.Part(text=data.text)]),
        partial=False,
        turn_complete=True,
    )

    await adk_service.append_event(session, event)

    return {"event_id": event.id, "session_id": session_id, "message": "Event added successfully"}


async def startup_hook(app: Litestar) -> None:
    """Initialize ADK service and create tables on application startup.

    Args:
        app: Litestar application instance.
    """
    config = AsyncpgConfig(pool_config={"dsn": "postgresql://postgres:postgres@localhost:5432/sqlspec_dev"})

    store = AsyncpgADKStore(config)
    await store.create_tables()

    service = SQLSpecSessionService(store)
    app.state.adk_service = service

    print("✅ ADK tables initialized in PostgreSQL")
    print("🚀 ADK Session API ready")


app = Litestar(
    route_handlers=[health_check, create_session, list_sessions, get_session, add_event],
    on_startup=[startup_hook],
    dependencies={"adk_service": get_adk_service},
    debug=True,
)


def main() -> None:
    """Run the Litestar application."""
    import uvicorn

    print("=== Litestar ADK Integration Example ===")
    print("Starting server on http://localhost:8000")
    print("\nAvailable endpoints:")
    print("  GET  /health")
    print("  POST /sessions")
    print("  GET  /sessions/{app_name}/{user_id}")
    print("  GET  /sessions/{app_name}/{user_id}/{session_id}")
    print("  POST /sessions/{app_name}/{user_id}/{session_id}/events")
    print("\nPress Ctrl+C to stop\n")

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
