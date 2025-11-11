# Tools to query summaries of discharge and fire occurrences in study
# catchments from the wildfire database. Easily accessible data are stored as
# an rds file in the lter-sparc-fire-arid shared drive on Aurora. A workflow to
# refresh the rds file as needed is detailed.

# query the data from rds ------------------------------------------------------

dd_area_stats <- readRDS("/home/shares/lter-sparc-fire-arid/dd_area_stats.rds")


# refresh the rds --------------------------------------------------------------

base_query <- "
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
      MIN(\"Date\") AS min_date_q,
      MAX(\"Date\") AS max_date_q,
      COUNT(\"Flow_cd\") AS records_q
    FROM firearea.discharge_daily
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
    ROUND((ST_Area(fires_catchments.geometry::geography) / 1000000)::numeric, 2) AS fire_catch_area,
    ROUND((ST_Area(catchments.geometry::geography) / 1000000)::numeric, 2) AS catch_area
  FROM
    firearea.fires_catchments
  JOIN
    firearea.catchments ON (catchments.usgs_site = fires_catchments.usgs_site)
) AS areas
JOIN
  firearea.mtbs_fire_perimeters ON (mtbs_fire_perimeters.event_id = areas.event_id)
) AS area_stats ON (area_stats.usgs_site = dd_stats.usgs_site AND area_stats.event_id = dd_stats.event_id)
WHERE
  dd_stats.days_q_pre_fire >= 0 AND dd_stats.days_q_post_fire >= 0 AND -- any duration
  dd_stats.usgs_site IN (
    SELECT usgs_site FROM firearea.ecoregion_catchments
  )
ORDER BY
  dd_stats.usgs_site,
  dd_stats.event_id
  ;
"

dd_area_stats <- DBI::dbGetQuery(pg, base_query)

saveRDS(dd_area_stats, "/home/shares/lter-sparc-fire-arid/dd_area_stats.rds")
