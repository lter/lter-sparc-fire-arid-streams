-- Manual import of USGS daily discharge CSV for site 09510200 (Sycamore Creek USGS site instead of NEON)
-- Source file (local): /home/srearl/Desktop/usgs_09510200.csv
-- Source file (archive): https://drive.google.com/file/d/12Pa9VFbEXuLbd5v-mL4Iev8mSBgWHRZm/view?usp=sharing
-- Target DB (local):   /home/srearl/Desktop/wildfire_discharge.duckdb
-- Target table:        usgs_discharge_daily
-- Columns inserted:    daily_id, time_series_id, parameter_code, time, value,
--                      monitoring_location_id, statistic_id, unit_of_measure,
--                      approval_status, last_modified, qualifier, usgs_site,
--                      retrieved_at
-- Notes:
--  - Replaces legacy syca rows with this USGS site data.
--  - Dedups on (monitoring_location_id, time, parameter_code, statistic_id).
--
-- run as: duckdb ~/Desktop/wildfire_discharge.duckdb ".read usgs_manual_import_09510200.sql"

-- Stage CSV as varchar to keep parsing explicit and robust.
CREATE OR REPLACE TEMP VIEW stg_09510200 AS
SELECT *
FROM read_csv_auto(
  '/home/srearl/Desktop/usgs_09510200.csv',
  header=TRUE,
  all_varchar=TRUE,
  normalize_names=FALSE
);

-- Prepare typed records for insert.
CREATE OR REPLACE TEMP VIEW stg_09510200_clean AS
SELECT
  NULLIF(TRIM(id), '') AS daily_id,
  NULLIF(TRIM(time_series_id), '') AS time_series_id,
  NULLIF(TRIM(parameter_code), '') AS parameter_code,
  TRY_CAST(time AS DATE) AS time,
  TRY_CAST(NULLIF(TRIM(value), '') AS DOUBLE) AS value,
  NULLIF(TRIM(monitoring_location_id), '') AS monitoring_location_id,
  NULLIF(TRIM(statistic_id), '') AS statistic_id,
  NULLIF(TRIM(unit_of_measure), '') AS unit_of_measure,
  NULLIF(TRIM(approval_status), '') AS approval_status,
  TRY_CAST(NULLIF(TRIM(last_modified), '') AS TIMESTAMP) AS last_modified,
  NULLIF(TRIM(qualifier), '') AS qualifier,
  'USGS-09510200' AS usgs_site,
  CURRENT_TIMESTAMP AS retrieved_at
FROM stg_09510200
WHERE TRY_CAST(time AS DATE) IS NOT NULL;

-- Remove legacy syca rows so this site is represented only by the USGS file.
DELETE FROM usgs_discharge_daily
WHERE usgs_site = 'syca';

-- Build candidates that are not already present.
CREATE OR REPLACE TEMP VIEW candidates_09510200 AS
SELECT s.*
FROM stg_09510200_clean s
WHERE NOT EXISTS (
  SELECT 1
  FROM usgs_discharge_daily d
  WHERE d.monitoring_location_id = s.monitoring_location_id
    AND d.time = s.time
    AND d.parameter_code = s.parameter_code
    AND d.statistic_id = s.statistic_id
);

SELECT COUNT(*) AS candidate_rows_09510200
FROM candidates_09510200;

INSERT INTO usgs_discharge_daily (
  daily_id,
  time_series_id,
  parameter_code,
  time,
  value,
  monitoring_location_id,
  statistic_id,
  unit_of_measure,
  approval_status,
  last_modified,
  qualifier,
  usgs_site,
  retrieved_at
)
SELECT
  daily_id,
  time_series_id,
  parameter_code,
  time,
  value,
  monitoring_location_id,
  statistic_id,
  unit_of_measure,
  approval_status,
  last_modified,
  qualifier,
  usgs_site,
  retrieved_at
FROM candidates_09510200;

-- Post-insert checks for replacement site.
SELECT COUNT(*) AS total_rows_09510200
FROM usgs_discharge_daily
WHERE usgs_site='USGS-09510200'
  AND parameter_code='00060'
  AND statistic_id='00003';

SELECT COUNT(*) AS remaining_syca_rows
FROM usgs_discharge_daily
WHERE usgs_site='syca';
