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

-- name: no_diagnostics
-- dialect: oracle
SELECT
    0 AS diagnostics_enabled,
    'disabled' AS diagnostics_mode,
    'Oracle diagnostics, AWR/ASH/ADDM, Statspack, and DBA_HIST queries are disabled by default.' AS warning
FROM dual;

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
