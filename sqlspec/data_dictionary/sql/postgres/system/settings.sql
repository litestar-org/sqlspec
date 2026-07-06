-- name: settings
-- dialect: postgres
SELECT
    name::text AS setting_name,
    setting::text AS setting_value,
    unit::text AS unit,
    category::text AS category,
    context::text AS context,
    vartype::text AS value_type,
    source::text AS source,
    boot_val::text AS boot_value,
    reset_val::text AS reset_value,
    pending_restart
FROM pg_catalog.pg_settings
ORDER BY name;
