

## overview

Queries to generate standardized base layers of aggregated discharge and
fire statistics, and exports of ecoregions and the overlap among
catchments.

Queries generate either a table, view, materialized view, or export.
Views should be reconstructed as needed based on database updates.

Syntax to run the queries with render is included but it is expected
that these queries will be run infrequently and selectively.

## table: discharge (aggregated discharge)

A view of discharge that reflects the combined records of data from USGS
and otherwise sites.

``` sql
DROP TABLE IF EXISTS firearea.discharge ;

CREATE TABLE firearea.discharge AS
SELECT
  discharge_daily.usgs_site,
  discharge_daily."Date",
  discharge_daily."Flow",
  discharge_daily."Flow_cd"
FROM firearea.discharge_daily
WHERE
  discharge_daily."Flow" >= 0
  AND discharge_daily.usgs_site IN (
    SELECT DISTINCT(usgs_site)
    FROM firearea.ecoregion_catchments
  )
UNION
SELECT
  non_usgs_discharge.usgs_site,
  non_usgs_discharge.date AS "Date",
  non_usgs_discharge.mean AS "Flow",
  NULL AS "Flow_cd"
FROM firearea.non_usgs_discharge
WHERE
  non_usgs_discharge.mean >= 0
;

-- Step 1: add quartile column
ALTER TABLE firearea.discharge
  ADD COLUMN quartile INTEGER ;

CREATE OR REPLACE PROCEDURE firearea.update_discharge_quartiles()
LANGUAGE plpgsql
AS $$
BEGIN
-- Step 2: Calculate percentiles
WITH percentiles AS (
    SELECT
        usgs_site,
        percentile_disc(0.25) WITHIN GROUP (ORDER BY "Flow") AS p25,
        percentile_disc(0.50) WITHIN GROUP (ORDER BY "Flow") AS p50,
        percentile_disc(0.75) WITHIN GROUP (ORDER BY "Flow") AS p75
    FROM
        firearea.discharge
    GROUP BY
        usgs_site
),
-- Step 3: Assign quartiles
quartiles AS (
    SELECT
        d.usgs_site,
        d."Flow",
        CASE
            WHEN d."Flow" <= p.p25 THEN 1
            WHEN d."Flow" <= p.p50 THEN 2
            WHEN d."Flow" <= p.p75 THEN 3
            ELSE 4
        END AS quartile
    FROM
        firearea.discharge d
    JOIN
        percentiles p ON d.usgs_site = p.usgs_site
)
-- Step 4: Update the table
UPDATE firearea.discharge
SET quartile = q.quartile
FROM quartiles q
WHERE
    firearea.discharge.usgs_site = q.usgs_site
    AND firearea.discharge."Flow" = q."Flow"
  ;
-- If everything runs successfully, commit automatically
EXCEPTION
  WHEN OTHERS THEN
    -- Rollback the transaction in case of failure
    RAISE NOTICE 'Transaction failed. Rolling back changes.';
    ROLLBACK;
END $$;


CALL firearea.update_discharge_quartiles();
```

## view: ranges (individual fires)

A view of the interval (`daterange`) between sites \* fires. This view
allows us to identify the timing of a separate event (water chemistry
sample, discharge value, etc.) relative to a fire within the catchment.
Intervals reflect the temporal period between a fire and the most recent
fire (pre) and the next occurring fire (post) within that catchment.

A note about quality: the construction of this table, and if and how it
affects subsequent queries in terms of inclusivity has not been
evaluated thoroughly. That is, for example, how a water-chemistry sample
is categorized (pre, post) if it were to fall on the day of the fire has
not been tested.

``` sql
DROP VIEW IF EXISTS firearea.ranges ;

CREATE VIEW firearea.ranges AS 
WITH RECURSIVE
pre_fire_cte AS (
  SELECT 
    pre_fire.usgs_site,
    pre_fire.date,
    UNNEST(pre_fire.event) AS event,
    pre_fire.pre
  FROM (
    SELECT
      pre_array.usgs_site,
      pre_array.ig_date AS date,
      pre_array.event_id AS event,
      DATERANGE(
        LAG(pre_array.ig_date, 1) OVER (
          PARTITION BY pre_array.usgs_site
          ORDER BY pre_array.ig_date
        ),
        pre_array.ig_date
      ) AS pre
    FROM (
      SELECT
        usgs_site,
        ig_date,
        ARRAY_AGG(event_id) AS event_id
      FROM firearea.fires_catchments 
      GROUP BY
        usgs_site,
        ig_date
      ORDER BY
        ig_date
    ) AS pre_array
    ORDER BY
      pre_array.usgs_site,
      pre_array.ig_date
  ) AS pre_fire
  WHERE pre_fire.pre != 'empty'
),
post_fire_cte AS (
  SELECT 
    post_fire.usgs_site,
    post_fire.date,
    UNNEST(post_fire.event) AS event,
    post_fire.post
  FROM (
    SELECT
      post_array.usgs_site,
      post_array.ig_date AS date,
      post_array.event_id AS event,
      DATERANGE(
        post_array.ig_date,
        LEAD(post_array.ig_date, 1) OVER (
          PARTITION BY post_array.usgs_site
          ORDER BY post_array.ig_date
        )
      ) AS post
    FROM (
      SELECT
        usgs_site,
        ig_date,
        ARRAY_AGG(event_id) AS event_id
      FROM firearea.fires_catchments 
      GROUP BY
        usgs_site,
        ig_date
      ORDER BY
        ig_date
    ) AS post_array
    ORDER BY
      post_array.usgs_site,
      post_array.ig_date
  ) AS post_fire
  WHERE post_fire.post != 'empty'
)
SELECT
  pre_fire_cte.usgs_site,
  pre_fire_cte.date,
  pre_fire_cte.event,
  pre_fire_cte.pre,
  post_fire_cte.post
FROM pre_fire_cte
JOIN post_fire_cte ON (
  pre_fire_cte.usgs_site = post_fire_cte.usgs_site
  AND pre_fire_cte.date  = post_fire_cte.date
  AND pre_fire_cte.event = post_fire_cte.event
)
;
```

## m.view: ranges_agg (aggregated fires)

Whereas the \[## individual fires\] ranges calculates range statistics
based on all site and all fires, \[## summer fires\] addresses some
pre-processing and calculates range statistics separately on summer and
non-summer fires. Pre-processing includes filtering fires smaller than a
certain percent of the catchment burned (0.00). Summer fires are defined
as fires occurring DOY 110 through 250. For summer fires, the interval
between the previous fire and latest fire are from the earliest fire in
the summer period and latest fire in the summer period.

A view of the interval (`daterange`) between sites \* fires. This view
allows us to identify the timing of a separate event (water chemistry
sample, discharge value, etc.) relative to a fire within the catchment.
Intervals reflect the temporal period between a fire and the most recent
fire (pre) and the next occurring fire (post) within that catchment.

A note about quality: the construction of this table, and if and how it
affects subsequent queries in terms of inclusivity has not been
evaluated thoroughly. That is, for example, how a water-chemistry sample
is categorized (pre, post) if it were to fall on the day of the fire has
not been tested.

output columns:

| Column              | Description                                           |
|---------------------|-------------------------------------------------------|
| usgs_site           | Catchment/site identifier                             |
| year                | Fire year                                             |
| start_date          | Start date of fire period                             |
| end_date            | End date of fire period                               |
| previous_end_date   | End date of previous fire period                      |
| next_start_date     | Start date of next fire period                        |
| days_since          | Days since previous fire                              |
| days_until          | Days until next fire                                  |
| events              | Array of event IDs in this fire period                |
| cum_fire_area       | Cumulative burned area (km²) for the fire period      |
| catch_area          | Catchment area (km²)                                  |
| cum_per_cent_burned | Cumulative percent of catchment burned                |
| all_fire_area       | Total burned area (km²) in catchment through end_date |
| all_per_cent_burned | Percent of total catchment burned through end_date    |
| latitude            | Catchment centroid latitude                           |
| longitude           | Catchment centroid longitude                          |

``` sql
-- REFRESH MATERIALIZED VIEW firearea.ranges_agg;

DROP VIEW IF EXISTS firearea.ranges_agg CASCADE ;
DROP MATERIALIZED VIEW IF EXISTS firearea.ranges_agg CASCADE ;

CREATE MATERIALIZED VIEW firearea.ranges_agg AS 
-- merges usgs and non-usgs catchments
WITH combined_catchments AS (
SELECT
  usgs_site,
  geometry
FROM firearea.catchments
WHERE
usgs_site IN (
    SELECT DISTINCT(usgs_site)
    FROM firearea.ecoregion_catchments
)
UNION 
SELECT
  usgs_site,
  geometry
FROM firearea.non_usgs_catchments
),
-- calculates areal stats and filters small fires
fires_filtered AS (
  SELECT
    areas_stats.*
  FROM (
    SELECT
      areas.usgs_site,
      areas.event_id,
      mtbs_fire_perimeters.ig_date,
      ROUND((mtbs_fire_perimeters.burnbndac / 247.11)::numeric, 2) AS fire_area,
      areas.catch_area,
      areas.fire_catch_area,
      ROUND(((areas.fire_catch_area / areas.catch_area) * 100)::numeric, 2) AS per_cent_burned
    FROM
    (
      SELECT
        fires_catchments.usgs_site,
        fires_catchments.event_id,
        fires_catchments.ig_date,
        ROUND((ST_Area(fires_catchments.geometry, TRUE) / 1000000)::numeric, 2) AS fire_catch_area,
        ROUND((ST_Area(combined_catchments.geometry, TRUE) / 1000000)::numeric, 2) AS catch_area
      FROM
        firearea.fires_catchments
      JOIN
        combined_catchments ON (combined_catchments.usgs_site = fires_catchments.usgs_site)
    ) AS areas
    JOIN
      firearea.mtbs_fire_perimeters ON (mtbs_fire_perimeters.event_id = areas.event_id)
  ) AS areas_stats
-- filtering by > 0.00 drops the count from 12782 to 11207
-- WHERE areas_stats.per_cent_burned > 0.00
),
-- aggregates events and cumulative burned area for all summer fires and
-- non-summer fires with more than one event per day per catchment;
-- summer fires are unioned to non-summer fires
grouped_fires AS (
  SELECT 
    usgs_site, 
    EXTRACT(YEAR FROM ig_date) AS year,
    MIN(ig_date) AS start_date,
    MAX(ig_date) AS end_date,
    ARRAY_AGG(DISTINCT event_id) AS events,
    SUM(fire_catch_area) AS cum_fire_area
  FROM 
    fires_filtered
  WHERE
    EXTRACT(DOY FROM ig_date) BETWEEN 110 AND 250
  GROUP BY 
    fires_filtered.usgs_site,
    year
  UNION
  -- account for occurrences of multiple fires per day at a site
  SELECT
    usgs_site,
    EXTRACT(YEAR FROM ig_date) AS year,
    ig_date AS start_date,
    ig_date AS end_date,
    ARRAY_AGG(DISTINCT event_id) AS events,
    SUM(fire_catch_area) AS cum_fire_area 
  FROM 
    fires_filtered
  WHERE 
    EXTRACT(DOY FROM ig_date) < 110
    OR EXTRACT(DOY FROM ig_date) > 250
  GROUP BY
    usgs_site,
    ig_date
),
-- identifies date since last and date until next fire
grouped_fires_ranges AS (
  SELECT
    grouped_fires.usgs_site,
    grouped_fires.year,
    start_date,
    end_date,
    LAG(grouped_fires.end_date, 1) OVER (
      PARTITION BY grouped_fires.usgs_site
      ORDER BY grouped_fires.end_date
    ) AS previous_end_date,
    LEAD(grouped_fires.start_date, 1) OVER (
      PARTITION BY grouped_fires.usgs_site
      ORDER BY grouped_fires.start_date
    ) AS next_start_date,
    events,
    cum_fire_area
  FROM grouped_fires
  ORDER BY
    grouped_fires.usgs_site,
    grouped_fires.start_date
),
-- calculates days since last and days until next fire
grouped_fires_days AS (
  SELECT
    usgs_site,
    year,
    start_date,
    end_date,
    previous_end_date,
    next_start_date,
    CASE
        WHEN previous_end_date IS NOT NULL
        THEN start_date - previous_end_date
        ELSE NULL
    END AS days_since,
    CASE
        WHEN next_start_date IS NOT NULL
        THEN next_start_date - end_date
        ELSE NULL
    END AS days_until,
    events,
    cum_fire_area
  FROM grouped_fires_ranges
)
-- calculates cumulative burned area (and percent) per tuple
-- calculates cumulative burned area (and percent) by site through end_date
SELECT
  grouped_fires_days.usgs_site,
  grouped_fires_days.year,
  grouped_fires_days.start_date,
  grouped_fires_days.end_date,
  grouped_fires_days.previous_end_date,
  grouped_fires_days.next_start_date,
  grouped_fires_days.days_since,
  grouped_fires_days.days_until,
  grouped_fires_days.events,
  grouped_fires_days.cum_fire_area,
  areas.catch_area,
  ROUND(((grouped_fires_days.cum_fire_area / areas.catch_area) * 100)::numeric, 2) AS cum_per_cent_burned,
  SUM(grouped_fires_days.cum_fire_area) OVER (
    PARTITION BY grouped_fires_days.usgs_site 
    ORDER BY grouped_fires_days.end_date 
    ROWS UNBOUNDED PRECEDING
  ) AS all_fire_area,
  ROUND((SUM(grouped_fires_days.cum_fire_area) OVER (
    PARTITION BY grouped_fires_days.usgs_site 
    ORDER BY grouped_fires_days.end_date 
    ROWS UNBOUNDED PRECEDING
  ) / areas.catch_area * 100)::numeric, 2) AS all_per_cent_burned,
  areas.latitude,
  areas.longitude
FROM
  grouped_fires_days
-- adds catchment centroid
JOIN
(
  SELECT
    combined_catchments.usgs_site,
    ROUND((ST_Area(combined_catchments.geometry, TRUE) / 1000000)::numeric, 2) AS catch_area,
    ST_Y(ST_Centroid(ST_Transform(geometry, 4326))) AS latitude,
    ST_X(ST_Centroid(ST_Transform(geometry, 4326))) AS longitude
  FROM
  combined_catchments
) AS areas ON (areas.usgs_site = grouped_fires_days.usgs_site)
;

-- add indexes for better performance
CREATE INDEX idx_ranges_agg_usgs_site ON firearea.ranges_agg (usgs_site);
CREATE INDEX idx_ranges_agg_dates ON firearea.ranges_agg (start_date, end_date);
```

## view: dd_area_stats (discharge & areal stats)

A view of statistics around discharge, and catchment and fire areas.

``` sql
DROP VIEW IF EXISTS firearea.dd_area_stats ;

CREATE VIEW firearea.dd_area_stats AS 
WITH combined_catchments AS (
SELECT
  usgs_site,
  geometry
FROM firearea.catchments
UNION 
SELECT
  usgs_site,
  geometry
FROM firearea.non_usgs_catchments
)
SELECT
  dd_stats.*,
  area_stats.fire_area,
  area_stats.catch_area,
  area_stats.fire_catch_area,
  area_stats.per_cent_burned
FROM (
  SELECT
    fire_catchment_dates.usgs_site,
    fire_catchment_dates.event_id,
    fire_catchment_dates.ignition_date,
    discharge_daily_statistics.min_date_q,
    discharge_daily_statistics.max_date_q,
    fire_catchment_dates.ignition_date - discharge_daily_statistics.min_date_q AS days_q_pre_fire,
    discharge_daily_statistics.max_date_q - fire_catchment_dates.ignition_date AS days_q_post_fire,
    discharge_daily_statistics.records_q
  FROM
  (
    SELECT
      usgs_site,
      min("Date") AS min_date_q,
      max("Date") AS max_date_q,
      count("Flow_cd") AS records_q
    FROM firearea.discharge
    GROUP BY
    usgs_site
  ) AS discharge_daily_statistics
  JOIN
  (
    SELECT
      fires_catchments.usgs_site,
      fires_catchments.event_id,
      mtbs_fire_perimeters.ig_date AS ignition_date
    FROM
    firearea.fires_catchments
    JOIN
    firearea.mtbs_fire_perimeters ON (mtbs_fire_perimeters.event_id = fires_catchments.event_id)
  ) AS fire_catchment_dates
  ON (fire_catchment_dates.usgs_site = discharge_daily_statistics.usgs_site)
) AS dd_stats
JOIN
(
  SELECT
    areas.usgs_site,
    areas.event_id,
    ROUND((burnbndac / 247.11)::numeric, 2) AS fire_area,
    areas.catch_area,
    areas.fire_catch_area,
    ROUND(((areas.fire_catch_area / areas.catch_area) * 100)::numeric, 2) AS per_cent_burned
  FROM
  (
    SELECT
      fires_catchments.usgs_site,
      fires_catchments.event_id,
      fires_catchments.ig_date,
      ROUND((ST_Area(fires_catchments.geometry, TRUE) / 1000000)::numeric, 2) AS fire_catch_area,
      ROUND((ST_Area(combined_catchments.geometry, TRUE) / 1000000)::numeric, 2) AS catch_area
    FROM
    firearea.fires_catchments
    JOIN
    combined_catchments ON (combined_catchments.usgs_site = fires_catchments.usgs_site)
  ) AS areas
  JOIN
  firearea.mtbs_fire_perimeters ON (mtbs_fire_perimeters.event_id = areas.event_id)
) AS area_stats ON (
area_stats.usgs_site = dd_stats.usgs_site
AND area_stats.event_id = dd_stats.event_id
)
WHERE
  dd_stats.days_q_pre_fire >= 0 AND dd_stats.days_q_post_fire >= 0 AND -- any duration
  dd_stats.usgs_site IN (
    SELECT usgs_site FROM firearea.ecoregion_catchments
  )
ORDER BY
  dd_stats.usgs_site,
  dd_stats.event_id
;
```

## export: ecoregions

Join (spatially) usgs_site to firearea.ecoregions table. Note that these
geometries are as 4326. Changing to a projected CRS (e.g., 5070) does
generate different (and probably more accurate) results but the
differences are trivial.

``` sql
\COPY (
SELECT
  ecoregion_catchments.usgs_site,
  ecoregions.ogc_fid,
  ecoregions.na_l3code,
  ecoregions.na_l3name,
  ecoregions.na_l2code,
  ecoregions.na_l2name,
  ecoregions.na_l1code,
  ecoregions.na_l1name,
  ecoregions.na_l3key,
  ecoregions.na_l2key,
  ecoregions.na_l1key,
  ecoregions.shape_leng,
  ecoregions.shape_area,
  ecoregions.ai_mode,
  ecoregions.ai_mean,
  ecoregions.ai_median,
  ecoregions.ai_mean_narm,
  100.0 * ST_Area(ST_Intersection(ecoregion_catchments.geometry, ecoregions.wkb_geometry)) / NULLIF(ST_Area(ecoregion_catchments.geometry), 0) AS percent_overlap
FROM firearea.ecoregion_catchments
JOIN firearea.ecoregions
  ON ST_Intersects(ecoregion_catchments.geometry, ecoregions.wkb_geometry)
) to '/tmp/study_sites_ecoregions.csv' WITH CSV HEADER
;
```

## export: catchment spatial overlap

Calculates pairwise spatial overlap percentages and areas between all
study catchments, including both USGS and non-USGS research network
sites. This analysis identifies nested and overlapping watersheds within
the study domain.

**input data sources** - `firearea.catchments` - USGS catchment
geometries - `firearea.non_usgs_catchments` - Non-USGS research network
catchment geometries (NEON, SBC-LTER) -
`firearea.ecoregion_catchments` - Study site definitions (filters USGS
catchments to study domain)

**processing logic**

1.  catchment integration

- UNION operation combines USGS and non-USGS catchment geometries
- Filtering: USGS catchments restricted to study sites in
  `ecoregion_catchments`
- Result: Unified catchment dataset across all monitoring networks

2.  spatial intersection analysis

- Pairwise comparison: Each catchment compared against every other
  catchment
- Self-exclusion: Uses `c1.usgs_site < c2.usgs_site` to avoid
  self-comparison and duplicate pairs
- Intersection detection: `ST_Intersects()` identifies overlapping
  catchment pairs
- Area calculations: Uses spheroidal geometry
  (`ST_Area(geometry, TRUE)`) for accuracy

3.  overlap metrics calculation

- Bidirectional percentages: Calculates what percent of each catchment
  overlaps with the other
- Actual overlap area: Intersection area in square kilometers
- Zero exclusion: Filters out pairs with no actual spatial overlap

**output fields**

| Field | Description | Units/Format |
|----|----|----|
| `site_1` | Primary catchment identifier | USGS site ID |
| `site_2` | Comparison catchment identifier | USGS site ID |
| `percent_overlap_site1` | Percent of site_1 area overlapped by site_2 | Percentage (2 decimal places) |
| `percent_overlap_site2` | Percent of site_2 area overlapped by site_1 | Percentage (2 decimal places) |
| `overlap_area_km2` | Actual overlapping area | Square kilometers (2 decimal places) |

**data quality controls** - Non-zero overlap requirement:
`ST_Area(ST_Intersection()) > 0` excludes trivial/edge-touching cases -
Spheroidal calculations: Accurate area measurements using Earth’s
curvature - NULLIF protection: Prevents division by zero errors in
percentage calculations - Precision control: Results rounded to 2
decimal places for consistency

**output characteristics** - File: `/tmp/catchment_overlaps.csv` -
Format: CSV with header row - Record scope: Only catchment pairs with
meaningful spatial overlap - Ordering: Alphabetical by primary site,
then comparison site

**common overlap patterns** - Nested catchments:
`percent_overlap_site2 = 100.00` indicates site_2 completely within
site_1 - Partial overlaps: Both percentages \< 100% indicate
intersecting but not nested watersheds - Small overlap areas: May
indicate catchments sharing drainage boundaries or measurement
uncertainties

This analysis supports understanding of spatial relationships between
monitoring sites and helps inform appropriate statistical approaches for
multi-site comparisons

``` sql
-- Calculate percent spatial overlap between all catchment pairs (including non-USGS)
\COPY (
WITH combined_catchments AS (
  SELECT usgs_site, geometry FROM firearea.catchments
  WHERE usgs_site IN (SELECT usgs_site FROM firearea.ecoregion_catchments)
  UNION 
  SELECT usgs_site, geometry FROM firearea.non_usgs_catchments
)
SELECT
  c1.usgs_site AS site_1,
  c2.usgs_site AS site_2,
  ROUND(
    (100.0 * ST_Area(ST_Intersection(c1.geometry, c2.geometry), TRUE) / 
     NULLIF(ST_Area(c1.geometry, TRUE), 0))::numeric, 
    2
  ) AS percent_overlap_site1,
  ROUND(
    (100.0 * ST_Area(ST_Intersection(c1.geometry, c2.geometry), TRUE) / 
     NULLIF(ST_Area(c2.geometry, TRUE), 0))::numeric, 
    2
  ) AS percent_overlap_site2,
  ROUND(
    (ST_Area(ST_Intersection(c1.geometry, c2.geometry), TRUE) / 1000000)::numeric, 
    2
  ) AS overlap_area_km2
FROM combined_catchments c1
JOIN combined_catchments c2 
  ON c1.usgs_site < c2.usgs_site
WHERE ST_Intersects(c1.geometry, c2.geometry)
  AND ST_Area(ST_Intersection(c1.geometry, c2.geometry), TRUE) > 0
ORDER BY c1.usgs_site, c2.usgs_site
) TO '/tmp/catchment_overlaps.csv' WITH CSV HEADER;
```
