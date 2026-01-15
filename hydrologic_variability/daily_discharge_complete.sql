/*
Daily Discharge Backfill Workflow (DuckDB v1.4.3)

- Purpose: Build core-only table daily_discharge_complete with columns
(usgs_site, time, value), backfilling missing daily dates per site using RANGE
and avoiding table aliasing throughout.
- Base constraint: DuckDB v1.4.3 does not support ALTER TABLE ADD CONSTRAINT. A
diagnostics view (usgs_discharge_daily_duplicates) is provided to check for base
duplicates. If base-level uniqueness is required, use a maintenance window and
the helper script sql/enforce_unique_usgs_discharge_daily.sql to recreate the
base table with UNIQUE(usgs_site, time); dependent objects may need temporary
dropping and recreation.
- Idempotency: Uses DROP TABLE IF EXISTS for reproducible rebuilds. The derived
table enforces UNIQUE(usgs_site, time).
- Execution:
  duckdb /home/srearl/desktop/wildfire_discharge.duckdb -c ".read
  /home/srearl/desktop/sql/daily_discharge_complete.sql"
- verification:
  - count rows: select count(*) from daily_discharge_complete;
  - sample coverage: select usgs_site, min(time), max(time), count(*) from
  daily_discharge_complete group by usgs_site order by usgs_site limit 5;
  - derived constraint: select constraint_name, constraint_type from
  information_schema.table_constraints where table_schema = 'main' and
  table_name = 'daily_discharge_complete';
  - base duplicates: select * from usgs_discharge_daily_duplicates limit 20;
- notes: physically orders output by (usgs_site, time) for scan locality. all
inserted backfill rows set value = null.
*/

-- idempotent build of core-only daily_discharge_complete with backfilled
-- missing days per usgs_site
-- duckdb v1.4.3

-- 0) diagnostics: list any duplicate (usgs_site, time) pairs in base table
create or replace view usgs_discharge_daily_duplicates as
select
  usgs_discharge_daily.usgs_site as usgs_site,
  usgs_discharge_daily.time as time,
  count(*) as row_count
from usgs_discharge_daily
group by usgs_discharge_daily.usgs_site, usgs_discharge_daily.time
having count(*) > 1;

-- 1) enforce uniqueness on base table (run only if no duplicates exist) review
-- usgs_discharge_daily_duplicates; if it is empty, apply the constraint note:
-- duckdb v1.4.3 does not support alter table add constraint. base-table
-- uniqueness is enforced via a separate migration script that recreates the
-- table with a unique (usgs_site, time) constraint.

-- 2) drop derived table if re-running
drop table if exists daily_discharge_complete;

-- 3) create core-only derived table with uniqueness on (usgs_site, time)
create table daily_discharge_complete (
  usgs_site varchar,
  time date,
  value double,
  unique (usgs_site, time)
);

-- 4) load existing core rows from base table
insert into daily_discharge_complete
select
  usgs_discharge_daily.usgs_site,
  usgs_discharge_daily.time,
  usgs_discharge_daily.value
from usgs_discharge_daily
order by usgs_discharge_daily.usgs_site, usgs_discharge_daily.time;

-- 5) generate missing (usgs_site, time) pairs and insert with null value
with per_site_bounds as (
  select
    usgs_discharge_daily.usgs_site as usgs_site,
    min(usgs_discharge_daily.time) as min_time,
    max(usgs_discharge_daily.time) as max_time
  from usgs_discharge_daily
  group by usgs_discharge_daily.usgs_site
),
per_site_calendar as (
  select
    per_site_bounds.usgs_site as usgs_site,
    unnest(range(per_site_bounds.min_time, per_site_bounds.max_time, interval 1 day))::date as time
  from per_site_bounds
),
missing_pairs as (
  select
    per_site_calendar.usgs_site as usgs_site,
    per_site_calendar.time as time
  from per_site_calendar
  left join usgs_discharge_daily
    on per_site_calendar.usgs_site = usgs_discharge_daily.usgs_site
   and per_site_calendar.time = usgs_discharge_daily.time
  where usgs_discharge_daily.usgs_site is null
)
insert into daily_discharge_complete
select
  missing_pairs.usgs_site as usgs_site,
  missing_pairs.time as time,
  null as value
from missing_pairs
order by missing_pairs.usgs_site, missing_pairs.time;