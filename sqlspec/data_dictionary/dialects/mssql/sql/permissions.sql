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

-- name: role_members
-- dialect: mssql
SELECT
    role_principal.name AS role_name,
    member_principal.name AS member_name,
    member_principal.type_desc AS member_type
FROM sys.database_role_members AS drm
INNER JOIN sys.database_principals AS role_principal ON drm.role_principal_id = role_principal.principal_id
INNER JOIN sys.database_principals AS member_principal ON drm.member_principal_id = member_principal.principal_id
ORDER BY role_principal.name, member_principal.name;
