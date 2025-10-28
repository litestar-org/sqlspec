from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

db_manager = SQLSpec()
db = db_manager.add_config(SqliteConfig(pool_config={"database": "mydb.db"}))

# Transaction committed on successful exit
with db_manager.provide_session(db) as session:
    session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    session.execute("INSERT INTO orders (user_name) VALUES (?)", "Alice")
    # Both committed together

# Transaction rolled back on exception
try:
    with db_manager.provide_session(db) as session:
        session.execute("INSERT INTO users (name) VALUES (?)", "Bob")
        raise ValueError("Something went wrong!")
except ValueError:
    pass  # Transaction was rolled back automatically


