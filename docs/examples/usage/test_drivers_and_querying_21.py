# Example from docs/usage/drivers_and_querying.rst - code-block 21
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str

# Mapping results to typed User instances
# result = session.execute("SELECT id, name, email FROM users")
# users: list[User] = result.all(schema_type=User)
# user: User = result.one(schema_type=User)

# Placeholder only

