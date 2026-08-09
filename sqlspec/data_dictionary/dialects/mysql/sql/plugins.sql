-- name: list
-- dialect: mysql
SELECT
    plugin_name,
    plugin_version,
    plugin_status,
    plugin_type,
    plugin_type_version,
    plugin_library,
    plugin_library_version,
    plugin_author,
    plugin_description,
    plugin_license,
    load_option
FROM information_schema.plugins
ORDER BY plugin_type, plugin_name;
