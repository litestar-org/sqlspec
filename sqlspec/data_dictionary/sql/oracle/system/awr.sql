-- name: awr
-- dialect: oracle
SELECT
    :acknowledge_license AS license_acknowledged,
    :include_diagnostics AS diagnostics_enabled,
    'DBA_HIST_SNAPSHOT' AS required_view,
    'AWR/ASH/ADDM and DBA_HIST access requires explicit opt-in, privileges, and license acknowledgement.' AS warning
FROM dual
WHERE :include_diagnostics = 1
  AND :acknowledge_license = 1;
