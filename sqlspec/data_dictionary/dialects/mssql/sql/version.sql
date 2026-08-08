-- name: version
-- dialect: mssql
SELECT
    @@VERSION AS version_string,
    CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion')) AS product_version,
    CONVERT(NVARCHAR(256), SERVERPROPERTY('Edition')) AS edition,
    CONVERT(INT, SERVERPROPERTY('EngineEdition')) AS engine_edition;
