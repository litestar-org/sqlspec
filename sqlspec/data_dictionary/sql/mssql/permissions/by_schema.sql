-- name: by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    o.name AS object_name,
    o.type_desc AS object_type,
    grantee.name AS grantee_name,
    grantor.name AS grantor_name,
    perm.permission_name,
    perm.state_desc,
    perm.class_desc,
    perm.major_id,
    perm.minor_id
FROM sys.database_permissions AS perm
LEFT JOIN sys.objects AS o ON perm.major_id = o.object_id
LEFT JOIN sys.schemas AS s ON o.schema_id = s.schema_id
LEFT JOIN sys.database_principals AS grantee ON perm.grantee_principal_id = grantee.principal_id
LEFT JOIN sys.database_principals AS grantor ON perm.grantor_principal_id = grantor.principal_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, o.name, grantee.name, perm.permission_name;
