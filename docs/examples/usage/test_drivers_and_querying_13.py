# Example from docs/usage/drivers_and_querying.rst - code-block 13
# SELECT query example
# result = session.execute("SELECT * FROM users WHERE id = ?", 1)

# INSERT query example
# result = session.execute(
#     "INSERT INTO users (name, email) VALUES (?, ?)",
#     "Alice",
#     "alice@example.com"
# )

# UPDATE example
# result = session.execute(
#     "UPDATE users SET email = ? WHERE id = ?",
#     "newemail@example.com",
#     1
# )
# print(f"Updated {result.rows_affected} rows")

# DELETE example
# result = session.execute("DELETE FROM users WHERE id = ?", 1)

# These lines are examples and require a session object to run.

