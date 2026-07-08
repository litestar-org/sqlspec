-- name: list
-- dialect: cockroachdb
SELECT
    catalog_name::text AS catalog_name,
    schema_name::text AS schema_name,
    schema_owner::text AS owner_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg\_%'
  AND schema_name <> 'information_schema'
ORDER BY schema_name;
