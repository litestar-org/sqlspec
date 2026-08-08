-- name: list
-- dialect: duckdb
SELECT
    extension_name,
    loaded,
    installed,
    install_path,
    description,
    aliases,
    extension_version,
    install_mode,
    installed_from
FROM duckdb_extensions()
ORDER BY extension_name;
