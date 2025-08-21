"""Example showing how to use SQLSpec session backend with Litestar."""

from litestar import Litestar, get, post
from litestar.config.session import SessionConfig
from litestar.datastructures import State

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpec, SQLSpecSessionBackend

# Configure SQLSpec with SQLite database
sqlite_config = SqliteConfig(
    pool_config={"database": "sessions.db"},
    migration_config={"script_location": "migrations", "version_table_name": "sqlspec_migrations"},
)

# Create SQLSpec plugin
sqlspec_plugin = SQLSpec(sqlite_config)

# Create session backend using SQLSpec
session_backend = SQLSpecSessionBackend(
    config=sqlite_config,
    table_name="user_sessions",
    session_lifetime=3600,  # 1 hour
)

# Configure session middleware
session_config = SessionConfig(
    backend=session_backend,
    cookie_https_only=False,  # Set to True in production
    cookie_secure=False,  # Set to True in production with HTTPS
    cookie_domain="localhost",
    cookie_path="/",
    cookie_max_age=3600,
    cookie_same_site="lax",
    cookie_http_only=True,
    session_cookie_name="sqlspec_session",
)


@get("/")
async def index() -> dict[str, str]:
    """Homepage route."""
    return {"message": "SQLSpec Session Example"}


@get("/login")
async def login_form() -> str:
    """Simple login form."""
    return """
    <html>
        <body>
            <h2>Login</h2>
            <form method="post" action="/login">
                <input type="text" name="username" placeholder="Username" required>
                <input type="password" name="password" placeholder="Password" required>
                <button type="submit">Login</button>
            </form>
        </body>
    </html>
    """


@post("/login")
async def login(data: dict[str, str], request) -> dict[str, str]:
    """Handle login and create session."""
    username = data.get("username")
    password = data.get("password")

    # Simple authentication (use proper auth in production)
    if username == "admin" and password == "secret":
        # Store user data in session
        request.set_session(
            {"user_id": 1, "username": username, "login_time": "2024-01-01T12:00:00Z", "roles": ["admin", "user"]}
        )
        return {"message": f"Welcome, {username}!"}

    return {"error": "Invalid credentials"}


@get("/profile")
async def profile(request) -> dict[str, str]:
    """User profile route - requires session."""
    session_data = request.session

    if not session_data or "user_id" not in session_data:
        return {"error": "Not logged in"}

    return {
        "user_id": session_data["user_id"],
        "username": session_data["username"],
        "login_time": session_data["login_time"],
        "roles": session_data["roles"],
    }


@post("/logout")
async def logout(request) -> dict[str, str]:
    """Logout and clear session."""
    request.clear_session()
    return {"message": "Logged out successfully"}


@get("/admin/sessions")
async def admin_sessions(request, state: State) -> dict[str, any]:
    """Admin route to view all active sessions."""
    session_data = request.session

    if not session_data or "admin" not in session_data.get("roles", []):
        return {"error": "Admin access required"}

    # Get session backend from state
    backend = session_backend
    session_ids = await backend.get_all_session_ids()

    return {
        "active_sessions": len(session_ids),
        "session_ids": session_ids[:10],  # Limit to first 10 for display
    }


@post("/admin/cleanup")
async def cleanup_sessions(request, state: State) -> dict[str, str]:
    """Admin route to clean up expired sessions."""
    session_data = request.session

    if not session_data or "admin" not in session_data.get("roles", []):
        return {"error": "Admin access required"}

    # Clean up expired sessions
    backend = session_backend
    await backend.delete_expired_sessions()

    return {"message": "Expired sessions cleaned up"}


# Create Litestar application
app = Litestar(
    route_handlers=[index, login_form, login, profile, logout, admin_sessions, cleanup_sessions],
    plugins=[sqlspec_plugin],
    session_config=session_config,
    debug=True,
)


if __name__ == "__main__":
    import uvicorn

    print("Starting SQLSpec Session Example...")
    print("Visit http://localhost:8000 to view the application")
    print("Login with username 'admin' and password 'secret'")

    uvicorn.run(app, host="0.0.0.0", port=8000)
