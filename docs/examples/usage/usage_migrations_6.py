# start-example
# migrations/0002_add_user_roles.py
"""Add user roles table

Revision ID: 0002_add_user_roles
Created at: 2025-10-18 12:00:00
"""

__all__ = ("downgrade", "test_upgrade_and_downgrade_strings", "upgrade")


def upgrade() -> str:
    """Apply migration."""
    return """
    CREATE TABLE user_roles (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        role VARCHAR(50) NOT NULL
    );
    """


def downgrade() -> str:
    """Revert migration."""
    return """
    DROP TABLE user_roles;
    """


# end-example


def test_upgrade_and_downgrade_strings() -> None:
    up_sql = upgrade()
    down_sql = downgrade()
    assert "CREATE TABLE user_roles" in up_sql
    assert "DROP TABLE user_roles" in down_sql
