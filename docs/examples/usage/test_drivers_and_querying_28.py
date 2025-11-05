# Example from docs/usage/drivers_and_querying.rst - code-block 28
# Performance tips examples
# config = AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/db", "min_size": 10, "max_size": 20})
# session.execute_many("INSERT INTO users (name) VALUES (?)", [(name,) for name in large_list])
# count = session.select_value("SELECT COUNT(*) FROM users")

# Placeholder only

