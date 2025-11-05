# Example from docs/usage/drivers_and_querying.rst - code-block 27
# Script execution example
# session.execute("""
#     CREATE TABLE users (
#         id INTEGER PRIMARY KEY,
#         name TEXT NOT NULL
#     );
#     CREATE TABLE posts (
#         id INTEGER PRIMARY KEY,
#         user_id INTEGER,
#         title TEXT,
#         FOREIGN KEY (user_id) REFERENCES users(id)
#     );
#     CREATE INDEX idx_posts_user_id ON posts(user_id);
# """)

# Placeholder only

