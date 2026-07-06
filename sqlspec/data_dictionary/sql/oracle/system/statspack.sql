-- name: statspack
-- dialect: oracle
SELECT
    :acknowledge_license AS license_acknowledged,
    :include_diagnostics AS diagnostics_enabled,
    'STATS$SNAPSHOT' AS required_view,
    'Statspack access requires explicit opt-in and privileges.' AS warning
FROM dual
WHERE :include_diagnostics = 1
  AND :acknowledge_license = 1;
