# Example from docs/usage/drivers_and_querying.rst - code-block 22
# Manual transaction control example
# with spec.provide_session(config) as session:
#     try:
#         session.begin()
#         session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
#         session.execute("INSERT INTO logs (action) VALUES (?)", "user_created")
#         session.commit()
#     except Exception:
#         session.rollback()
#         raise

# Placeholder only

