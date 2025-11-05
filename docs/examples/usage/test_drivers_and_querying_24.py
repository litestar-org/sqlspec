# Example from docs/usage/drivers_and_querying.rst - code-block 24
# Positional parameters examples
# session.execute("SELECT * FROM users WHERE id = ?", 1)
# session.execute("SELECT * FROM users WHERE id = $1 AND status = $2", 1, "active")
# session.execute("SELECT * FROM users WHERE id = %s", 1)

# Placeholder only

