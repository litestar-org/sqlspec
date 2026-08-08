-- name: by_owner
-- dialect: oracle
SELECT
    username AS schema_name,
    user_id,
    created,
    common,
    oracle_maintained,
    inherited,
    default_collation
FROM all_users
WHERE username = COALESCE(:schema_name, username)
ORDER BY username;
