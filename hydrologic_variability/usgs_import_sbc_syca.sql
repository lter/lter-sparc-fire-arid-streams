-- Import sbc.csv and syca.csv into usgs_discharge_daily using DuckDB
-- Mappings:
--   sbc.csv: usgs_site -> usgs_discharge_daily.usgs_site
--            Date      -> usgs_discharge_daily.time (DATE)
--            Flow      -> usgs_discharge_daily.value (DOUBLE)
--   syca.csv: usgs_site -> usgs_discharge_daily.usgs_site
--             date      -> usgs_discharge_daily.time (DATE)
--             mean      -> usgs_discharge_daily.value (DOUBLE)
-- Other target columns are left NULL.
-- Data sources:
-- 1. sbc.csv derived from exiting data in wildfire database sensu:
-- \COPY (
-- SELECT *
-- FROM firearea.discharge
-- WHERE usgs_site IN (
--   'sbc_lter_mis',
--   'sbc_lter_rat',
--   'sbc_lter_bel',
--   'sbc_lter_gav',
--   'sbc_lter_ref',
--   'sbc_lter_hon'
-- )
-- ) to '/tmp/sbc.csv' with csv header ;
-- 2. syca.csv dervied from a modified version of the workflow to query NEON
-- discharge data
-- ([here](https://github.com/lter/lter-sparc-fire-arid-streams/blob/78c8881ff3435894bdf8ade6c1b264a5337b88d6/wildfire_database/wildfire_database_non_usgs.qmd#L498))
-- that wrote to file instead of to the datbase. The database already houses
-- the earliest dates but repulling from NEON gave us the most up-do-date data
-- available.


-- Insert sbc.csv rows with file-level DISTINCT and anti-join dedup by (usgs_site, time)
WITH sbc AS (
    SELECT DISTINCT
        usgs_site,
        CAST("Date" AS DATE) AS time,
        CAST(NULLIF(TRIM(CAST("Flow" AS VARCHAR)), '') AS DOUBLE) AS value
    FROM read_csv_auto('/tmp/sbc.csv', header=TRUE)
), candidates AS (
    SELECT s.*
    FROM sbc s
    WHERE NOT EXISTS (
        SELECT 1
        FROM usgs_discharge_daily d
        WHERE d.usgs_site = s.usgs_site
          AND d.time = s.time
    )
)
INSERT INTO usgs_discharge_daily (usgs_site, time, value)
SELECT usgs_site, time, value FROM candidates;

-- Insert syca.csv rows with file-level DISTINCT and anti-join dedup by (usgs_site, time)
WITH syca AS (
    SELECT DISTINCT
        usgs_site,
        CAST("date" AS DATE) AS time,
        CAST(NULLIF(TRIM(CAST("mean" AS VARCHAR)), '') AS DOUBLE) AS value
    FROM read_csv_auto('/tmp/syca.csv', header=TRUE)
), candidates AS (
    SELECT s.*
    FROM syca s
    WHERE NOT EXISTS (
        SELECT 1
        FROM usgs_discharge_daily d
        WHERE d.usgs_site = s.usgs_site
          AND d.time = s.time
    )
)
INSERT INTO usgs_discharge_daily (usgs_site, time, value)
SELECT usgs_site, time, value FROM candidates;

-- Optional sanity checks
SELECT usgs_site, COUNT(*) AS n
FROM usgs_discharge_daily
WHERE usgs_site IN ('sbc_lter_bel', 'syca')
GROUP BY usgs_site;

-- Check for duplicates before index creation
SELECT usgs_site, time, COUNT(*) AS c
FROM usgs_discharge_daily
GROUP BY usgs_site, time
HAVING COUNT(*) > 1
ORDER BY c DESC, usgs_site, time;

-- Create unique index to enforce future uniqueness on (usgs_site, time)
CREATE UNIQUE INDEX IF NOT EXISTS idx_usgs_discharge_daily_site_time
ON usgs_discharge_daily (usgs_site, time);
