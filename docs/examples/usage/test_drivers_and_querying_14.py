# Example from docs/usage/drivers_and_querying.rst - code-block 14
# Batch insert example
# session.execute_many(
#     "INSERT INTO users (name, email) VALUES (?, ?)",
#     [
#         ("Alice", "alice@example.com"),
#         ("Bob", "bob@example.com"),
#         ("Charlie", "charlie@example.com"),
#     ]
# )

# Batch update example
# session.execute_many(
#     "UPDATE users SET status = ? WHERE id = ?",
#     [
#         ("active", 1),
#         ("inactive", 2),
#     ]
# )

# Examples require a session object to run.

