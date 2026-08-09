-- name: list
-- dialect: mysql
SELECT
    component_id,
    component_group_id,
    component_urn
FROM mysql.component
ORDER BY component_id;
