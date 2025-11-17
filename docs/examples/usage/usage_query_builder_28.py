def test_example_28():
    # start-example
    from pydantic import BaseModel
    from sqlspec import sql

    class User(BaseModel):
        id: int
        name: str
        email: str

    query = sql.select("id", "name", "email").from_("users")
    result = session.execute(query)
    users: list[User] = result.all(schema_type=User)
    # end-example
