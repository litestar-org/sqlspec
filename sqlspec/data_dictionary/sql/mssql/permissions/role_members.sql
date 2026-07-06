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
