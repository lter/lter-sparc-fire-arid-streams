-- Manual import of NWIS DV tab file for USGS site 09406000
-- Source file (local):   /home/srearl/Desktop/usgs_09406000.tab
-- Source file (archive): https://drive.google.com/file/d/1Eqq9yN-RfjD_tuUgVSjgIdKVgYEl961j/view?usp=drive_link
-- Target DB (local):     /home/srearl/Desktop/wildfire_discharge.duckdb
-- Target DB (archive):   https://drive.google.com/file/d/1EzwKi9M9chglsh7MyM58im7RYIbGDtBl/view?usp=drive_link
-- data source:           https://waterdata.usgs.gov/monitoring-location/USGS-09406000/#dataTypeId=continuous-00065-0&period=P7D&showFieldMeasurements=true
-- Target table:          usgs_discharge_daily
-- Columns inserted: parameter_code, time, value, monitoring_location_id, statistic_id, unit_of_measure, approval_status, usgs_site
-- Notes:
--  - Preserves leading zeros in site_no by reading as text.
--  - Skips header comments and the type-token row.
--  - Maps blank value to NULL, blank _cd to NULL.
--  - Dedups on (monitoring_location_id, time, parameter_code, statistic_id).

-- Note: running via `duckdb /home/srearl/Desktop/wildfire_discharge.duckdb -init this.sql` opens the DB; no ATTACH needed.

-- Stage: read TSV with tab delimiter, header row after 25 comment lines, skip type-token row after header
-- If your file layout differs, adjust skip_rows accordingly.
CREATE OR REPLACE TEMP VIEW stg AS
SELECT *
FROM read_csv_auto(
  '/home/srearl/Desktop/usgs_09406000.tab',
  delim='\t',
  header=TRUE,
  comment='#',
  all_varchar=TRUE,
  normalize_names=FALSE
);

-- Warning: show any unexpected parameter/stat series columns beyond the discharge mean pair
-- This is informational; script continues regardless.
SELECT column_name AS unexpected_series_column
FROM information_schema.columns
WHERE table_name = 'stg'
  AND regexp_matches(column_name, '^[0-9]+_\d{5}_\d{5}(_cd)?$')
  AND column_name NOT IN ('143271_00060_00003','143271_00060_00003_cd');

-- Cleaned projection for insert (minimal columns only)
CREATE OR REPLACE TEMP VIEW stg_clean AS
SELECT
  '00060' AS parameter_code,
  TRY_CAST(datetime AS DATE) AS time,
  TRY_CAST(NULLIF(TRIM("143271_00060_00003"), '') AS DOUBLE) AS value,
  'USGS-' || site_no AS monitoring_location_id,
  '00003' AS statistic_id,
  'ft^3/s' AS unit_of_measure,
  NULLIF(TRIM("143271_00060_00003_cd"), '') AS approval_status,
  'USGS-' || site_no AS usgs_site
FROM stg
WHERE TRY_CAST(datetime AS DATE) IS NOT NULL;

-- Preview: build candidate rows (not yet in target) and count
CREATE OR REPLACE TEMP VIEW candidates AS
SELECT s.*
FROM stg_clean s
WHERE NOT EXISTS (
  SELECT 1
  FROM usgs_discharge_daily d
  WHERE d.monitoring_location_id = s.monitoring_location_id
    AND d.time = s.time
    AND d.parameter_code = s.parameter_code
    AND d.statistic_id = s.statistic_id
);

SELECT COUNT(*) AS candidate_rows
FROM candidates;

-- Insert only new rows using anti-join on natural key
INSERT INTO usgs_discharge_daily (
  parameter_code,
  time,
  value,
  monitoring_location_id,
  statistic_id,
  unit_of_measure,
  approval_status,
  usgs_site
)
SELECT
  parameter_code,
  time,
  value,
  monitoring_location_id,
  statistic_id,
  unit_of_measure,
  approval_status,
  usgs_site
FROM candidates;

-- Post-insert: verify total rows for site/parameter/stat
SELECT COUNT(*) AS total_rows_for_site
FROM usgs_discharge_daily
WHERE usgs_site='USGS-09406000'
  AND parameter_code='00060'
  AND statistic_id='00003';

-- Site USGS-09510200 import is maintained in a dedicated script:
-- lter-sparc-fire-arid-streams/hydrologic_variability/usgs_manual_import_09510200.sql