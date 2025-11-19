# start-example
__all__ = ("create_user", "test_stub" )


spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://..."},
        extension_config={
            "litestar": {"commit_mode": "autocommit"}  # Auto-commit on 2xx
        },
    )
)


@post("/users")
async def create_user(data: dict, db_session: AsyncDriverAdapterBase) -> dict:
    # Transaction begins automatically
    result = await db_session.execute(
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id", data["name"], data["email"]
    )
    # Commits automatically on success
    return result.one()


# end-example


def test_stub() -> None:
    assert True
