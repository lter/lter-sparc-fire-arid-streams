-- sampling just three sites
-- WITH all_sites AS (
-- SELECT usgs_site FROM firearea.largest_ammonium_valid_fire_per_site UNION ALL
-- SELECT usgs_site FROM firearea.largest_nitrate_valid_fire_per_site UNION ALL
-- SELECT usgs_site FROM firearea.largest_orthop_valid_fire_per_site UNION ALL
-- SELECT usgs_site FROM firearea.largest_spcond_valid_fire_per_site
-- )
-- SELECT *
-- FROM (
--     SELECT DISTINCT lower(usgs_site) AS usgs_site
--     FROM all_sites
-- ) AS distinct_sites
-- ORDER BY RANDOM()
-- LIMIT 3;

-- sampling just three sites with restriction on flowline counts
-- WITH all_sites AS (
--   SELECT usgs_site FROM firearea.largest_ammonium_valid_fire_per_site UNION ALL
--   SELECT usgs_site FROM firearea.largest_nitrate_valid_fire_per_site UNION ALL
--   SELECT usgs_site FROM firearea.largest_orthop_valid_fire_per_site UNION ALL
--   SELECT usgs_site FROM firearea.largest_spcond_valid_fire_per_site
-- ),
-- counts AS (
--   SELECT LOWER(usgs_site) as usgs_site
--   FROM firearea.flowlines
--   GROUP BY usgs_site
--   HAVING COUNT(nhdplus_comid) <= 300
-- )
-- SELECT distinct_sites.usgs_site
-- FROM (
--     SELECT DISTINCT LOWER(all_sites.usgs_site) AS usgs_site
--     FROM all_sites
--     JOIN counts ON LOWER(all_sites.usgs_site) = counts.usgs_site
-- ) AS distinct_sites
-- ORDER BY RANDOM()
-- LIMIT 3;

-- specific sites
SELECT usgs_site
FROM firearea.catchments
WHERE usgs_site IN (
  'USGS-13154400',
  'USGS-09405900',
  'USGS-07121500'
);