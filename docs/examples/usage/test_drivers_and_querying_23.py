# Example from docs/usage/drivers_and_querying.rst - code-block 23
# Context manager transactions
# async with spec.provide_session(config) as session:
#     async with session.begin():
#         await session.execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?", 1)
#         await session.execute("UPDATE accounts SET balance = balance + 100 WHERE id = ?", 2)
#         # Auto-commits on success, auto-rollbacks on exception

# Placeholder only

