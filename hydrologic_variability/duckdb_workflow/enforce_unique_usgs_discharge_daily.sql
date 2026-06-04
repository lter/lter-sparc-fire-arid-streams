/*
Base Uniqueness Maintenance (DuckDB v1.4.3)

Purpose:
- Recreate the base table `usgs_discharge_daily` with a `UNIQUE (usgs_site,
time)` constraint.
- DuckDB v1.4.3 does not support `ALTER TABLE ADD CONSTRAINT`, so a
copy-and-swap is required.

Sequence (Maintenance Window Recommended):
1) Diagnostics: Create `usgs_discharge_daily_duplicates` and ensure it returns 0 rows.
2) Identify dependents of `usgs_discharge_daily` (e.g., views that reference
it). Drop or disable them temporarily.
  - You can search view definitions: `SELECT table_name, view_definition FROM
  information_schema.views WHERE view_definition ILIKE
  '%usgs_discharge_daily%';`
3) Create `usgs_discharge_daily_unique` with the same schema plus `UNIQUE (usgs_site, time)`.
4) Populate `usgs_discharge_daily_unique` from `usgs_discharge_daily`.
5) Swap tables inside a transaction:
  - `ALTER TABLE usgs_discharge_daily RENAME TO usgs_discharge_daily_backup;`
  - `ALTER TABLE usgs_discharge_daily_unique RENAME TO usgs_discharge_daily;`
6) Recreate or re-enable dependent objects dropped in step 2.
7) Verify:
  - Constraint: `SELECT constraint_name, constraint_type FROM
  information_schema.table_constraints WHERE table_name =
  'usgs_discharge_daily';`
  - Sample rows/coverage as needed.
8) Optional cleanup: `DROP TABLE IF EXISTS usgs_discharge_daily_backup;`

Notes:
- If dependencies prevent rename, drop those dependents first (this script drops
the duplicates view before the swap).
- Keep a backup until verification is complete.
*/

-- 0) Diagnostics: duplicates view
CREATE OR REPLACE VIEW usgs_discharge_daily_duplicates AS
SELECT
  usgs_discharge_daily.usgs_site AS usgs_site,
  usgs_discharge_daily.time AS time,
  COUNT(*) AS row_count
FROM usgs_discharge_daily
GROUP BY usgs_discharge_daily.usgs_site, usgs_discharge_daily.time
HAVING COUNT(*) > 1;

-- Optional: inspect offenders before proceeding
-- SELECT * FROM usgs_discharge_daily_duplicates LIMIT 50;

-- 1) Stop if duplicates exist (manual check recommended)
-- Proceed only when the duplicates view returns 0 rows.

-- 2) Create a new table with the desired UNIQUE constraint and identical schema
DROP TABLE IF EXISTS usgs_discharge_daily_unique;
CREATE TABLE usgs_discharge_daily_unique (
  daily_id               VARCHAR,
  time_series_id         VARCHAR,
  parameter_code         VARCHAR,
  time                   DATE,
  value                  DOUBLE,
  monitoring_location_id VARCHAR,
  statistic_id           VARCHAR,
  unit_of_measure        VARCHAR,
  approval_status        VARCHAR,
  last_modified          TIMESTAMP,
  qualifier              VARCHAR,
  usgs_site              VARCHAR,
  retrieved_at           TIMESTAMP,
  UNIQUE (usgs_site, time)
);

-- 3) Populate new table from the original
INSERT INTO usgs_discharge_daily_unique
SELECT
  usgs_discharge_daily.daily_id,
  usgs_discharge_daily.time_series_id,
  usgs_discharge_daily.parameter_code,
  usgs_discharge_daily.time,
  usgs_discharge_daily.value,
  usgs_discharge_daily.monitoring_location_id,
  usgs_discharge_daily.statistic_id,
  usgs_discharge_daily.unit_of_measure,
  usgs_discharge_daily.approval_status,
  usgs_discharge_daily.last_modified,
  usgs_discharge_daily.qualifier,
  usgs_discharge_daily.usgs_site,
  usgs_discharge_daily.retrieved_at
FROM usgs_discharge_daily;

-- 4) Swap tables: backup original, replace with unique-constrained table
-- Ensure no dependent objects block the rename
DROP VIEW IF EXISTS usgs_discharge_daily_duplicates;
BEGIN TRANSACTION;
ALTER TABLE usgs_discharge_daily RENAME TO usgs_discharge_daily_backup;
ALTER TABLE usgs_discharge_daily_unique RENAME TO usgs_discharge_daily;
COMMIT;

-- 5) Optional: drop backup after verification
-- DROP TABLE IF EXISTS usgs_discharge_daily_backup;
