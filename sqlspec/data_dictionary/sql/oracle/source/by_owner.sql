-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    name,
    type,
    line,
    text,
    origin_con_id
FROM all_source
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR name = :object_name)
ORDER BY owner, name, type, line;
