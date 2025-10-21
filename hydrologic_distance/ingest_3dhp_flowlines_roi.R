# 3DHP Flowline Ingestion: Region of Interest (ROI) Workflow
# -----------------------------------------------------------------------------
#
# overview:
#   This script ingests 3DHP CONUS flowlines relevant to project catchments by
#   clipping the national flowline layer to a unioned catchment Region of
#   Interest (ROI). The per-catchment clipped geometries are then materialized
#   in `firearea.hydrography` for analysis.
#
# data source:
#   https://www.sciencebase.gov/catalog/item/67464fb7d34e6d1dac3abec1
#
# Purpose
#   Ingest only those 3DHP CONUS flowlines relevant to project catchments by
#   clipping the national flowline layer to a unioned catchment Region of
#   Interest (ROI), then materializing per‑catchment clipped geometries in
#   `firearea.hydrography` for analysis.
#
# ROI Definition
#   All catchments (USGS and non‑USGS) whose `usgs_site` values appear in
#   `firearea.fires_catchments`.
#
# Resulting Tables
#   1. firearea.hydro_3dhp_flowline_roi   (staging; clipped source flowlines in EPSG:6350)
#   2. firearea.hydrography               (analysis; per‑catchment clipped flowlines in EPSG:4326)
#
# End-to-End Workflow
#   A. Query and union relevant catchments (USGS + non‑USGS) into a single ROI polygon.
#   B. Transform ROI to EPSG:6350 (native 3DHP Albers) for accurate planar intersections.
#   C. (Optional) Topology‑preserving simplify; accept simplified polygon only if area loss < 0.5%.
#   D. Write ROI polygon to `/tmp` as a GeoPackage (layer name: 'roi').
#   E. Manually run the emitted `ogr2ogr -clipsrc` command to create
#      `firearea.hydro_3dhp_flowline_roi` (clipped flowlines) in PostGIS.
#   F. Re-run this script: it detects the staging table and performs a single
#      CTAS (`CREATE TABLE AS`) to build `firearea.hydrography` by:
#         * Intersecting each flowline with each relevant catchment geometry
#           using ST_Intersection in EPSG:6350.
#         * Transforming the clipped result to EPSG:4326 for storage consistency.
#         * Adding descriptive attributes carried through from the 3DHP dataset.
#   G. Add a surrogate primary key and performance indexes (usgs_site, streamorder,
#      featuretype, GIST(geometry)), then ANALYZE for query planner statistics.
#
# Design Choices
#   * Single CTAS avoids iterative R-based clipping and minimizes client data shuttling.
#   * Retain EPSG:6350 for staging/intersection; store final geometry in EPSG:4326 to
#     align with other application layers.
#   * Area-difference threshold ensures simplification does not materially distort ROI.
#   * Explicit `/tmp` path for ROI artifact simplifies shell-based reproducibility.
#
# Validation Checklist
#   1. ROI file exists: `ls -lh /tmp/roi_polygon_*.gpkg`
#   2. Staging row count: `SELECT COUNT(*) FROM firearea.hydro_3dhp_flowline_roi;`
#   3. Hydrography row count: `SELECT COUNT(*) FROM firearea.hydrography;`
#   4. Non-null geometry: `SELECT COUNT(*) FROM firearea.hydrography WHERE geometry IS NULL;` (expect 0)
#   5. CRS sanity: `SELECT DISTINCT ST_SRID(geometry) FROM firearea.hydrography;` (expect 4326)
#   6. Sample intersection integrity: visually inspect a few sites or
#      `SELECT usgs_site, SUM(ST_Length(geometry::geography))/1000 FROM firearea.hydrography GROUP BY usgs_site LIMIT 10;`
#
# Performance Notes
#   * Ensure GIST index on staging table before CTAS if very large.
#   * Adjust PostgreSQL `work_mem` for large intersection operations.
#   * Consider partitioning `firearea.hydrography` by region or fire event if growth accelerates.
#
# Future Enhancements
#   * Materialized views for aggregate metrics (e.g., total flowline length per fire perimeter).
#   * Automated retry & logging wrapper for the ogr2ogr step.
#   * Dynamic simplification tolerance based on vertex count / complexity metrics.
#   * Parallelized ingestion or PostGIS foreign table + SELECT/INSERT pattern.
#
# Requirements
#   * GDAL (ogr2ogr) installed and accessible.
#   * PostGIS extension enabled in target database.
#   * R packages: sf, DBI, dplyr, units, glue.

source("~/Documents/localSettings/pg_local.R")
pg <- pg_local_connect("wildfire")

geopackage_path <- "/home/srearl/Desktop/3dhp_all_CONUS_20250313_GPKG.gpkg"
flowline_layer  <- "hydro_3dhp_all_flowline"

stopifnot(file.exists(geopackage_path))

# ------------------------------------------------------------------------------
# 1. Build ROI Union Geometry
# ------------------------------------------------------------------------------
message("Building ROI (union of catchments in fires_catchments)...")

roi_query <- "
SELECT
  usgs_site,
  geometry
FROM
  firearea.catchments
WHERE
  usgs_site IN (
    SELECT
    DISTINCT usgs_site
    FROM firearea.fires_catchments
  )
UNION ALL
SELECT
  usgs_site,
  geometry
FROM
  firearea.non_usgs_catchments
WHERE
  usgs_site IN (
    SELECT
    DISTINCT usgs_site
    FROM firearea.fires_catchments
  )
;
"

roi_catchments <- sf::st_read(
  dsn             = pg,
  query           = roi_query,
  geometry_column = "geometry",
  quiet           = TRUE
)

if (nrow(roi_catchments) == 0) stop("No ROI catchments found.")

# Ensure WGS84 then project to EPSG:6350 (native 3DHP)
if (is.null(sf::st_crs(roi_catchments)$epsg) || sf::st_crs(roi_catchments)$epsg != 4326) {
  roi_catchments <- sf::st_transform(roi_catchments, 4326)
}

roi_union <- sf::st_union(sf::st_geometry(roi_catchments)) |>
  sf::st_as_sf() |>
  sf::st_transform(6350)

roi_union$clip_id <- 1L

# Optional: simplify to speed clipping (tweak tolerance as needed)
simplify_tolerance <- units::set_units(5000, "m")  # adjust or set NULL to disable

roi_simplified <- tryCatch({
  sf::st_simplify(
    roi_union,
    dTolerance = as.numeric(simplify_tolerance),
    preserveTopology = TRUE
  )
}, error = function(e) roi_union)

# Decide which geometry to use (simplified if area difference small)
area_orig     <- as.numeric(sf::st_area(roi_union))
area_simp     <- as.numeric(sf::st_area(roi_simplified))
area_diff_pct <- abs(area_orig - area_simp) / area_orig * 100
use_geom      <- if (area_diff_pct < 0.5) roi_simplified else roi_union  # keep simplified if <0.5% diff

message(glue::glue("ROI polygon ready (area diff after simplify: {round(area_diff_pct,3)}%)"))

roi_gpkg <- file.path(
  "/tmp",
  sprintf(
    "roi_polygon_%s_%d.gpkg",
          format(Sys.time(), "%Y%m%d_%H%M%S"),
          Sys.getpid()
          )
)
sf::st_write(use_geom, roi_gpkg, layer = "roi", delete_dsn = TRUE, quiet = TRUE)
message(glue::glue("Wrote ROI polygon to {roi_gpkg}"))

# ------------------------------------------------------------------------------
# 2. Compose ogr2ogr Commands
# ------------------------------------------------------------------------------

ogr2ogr -f PostgreSQL "PG:host=localhost dbname=wildfire user=_user_ password=_pass_" /home/srearl/Desktop/3dhp_all_CONUS_20250313_GPKG.gpkg hydro_3dhp_all_flowline \
  -lco SCHEMA=firearea -nln hydro_3dhp_flowline_roi -overwrite \
  -clipsrc roi_polygon_20251016_120313_22117.gpkg -clipsrclayer roi \
  -nlt MULTILINESTRING -progress


## -----------------------------------------------------------------------------
## 3. Create firearea.hydrography (CTAS) if source table exists ----------------
## -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS hydro_3dhp_flowline_roi_geom_idx ON firearea.hydro_3dhp_flowline_roi USING GIST (shape);

tbl_exists <- DBI::dbGetQuery(
  conn      = pg,
  statement = "
  SELECT 1
  FROM information_schema.tables
  WHERE table_schema='firearea' AND table_name='hydro_3dhp_flowline_roi'"
  )

if (nrow(tbl_exists) == 1) {

  message("Source table firearea.hydro_3dhp_flowline_roi found. Creating firearea.hydrography via CTAS...")

  DBI::dbExecute(pg, "DROP TABLE IF EXISTS firearea.hydrography CASCADE;")

  ctas_sql <- "
  CREATE TABLE firearea.hydrography AS
  SELECT
    c.usgs_site,
    '3dhp_flowline'::text AS source_layer,
    CURRENT_DATE AS extraction_date,
    f.objectid,
    f.id3dhp,
    f.featuredate,
    f.mainstemid,
    f.featuretype,
    f.featuretypelabel,
    f.lengthkm,
    f.flowdirection,
    f.flowdirectionlabel,
    f.onsurface,
    f.onsurfacelabel,
    f.catchmentid3dhp,
    f.flowpathid3dhp,
    f.streamlevel,
    f.startflag,
    f.terminalflag,
    f.streamorder,
    f.streamcalculator,
    f.hydrosequence,
    f.dnhydrosequence,
    f.uphydrosequence,
    f.dnlevelpath,
    f.uplevelpath,
    f.pathlength,
    f.arbolatesum,
    f.divergence,
    f.divergencelabel,
    f.rtrndivergence,
    f.levelpath,
    f.terminalpath,
    f.workunitid,
    f.shape_Length AS shape_length,
    ST_Transform(ST_Intersection(f.shape, ST_Transform(c.geometry, 6350)), 4326)::geometry(MultiLineString,4326) AS geometry
  FROM firearea.catchments c
  JOIN firearea.hydro_3dhp_flowline_roi f ON ST_Intersects(f.shape, ST_Transform(c.geometry, 6350))
  WHERE c.usgs_site IN (
    SELECT DISTINCT usgs_site
    FROM firearea.fires_catchments
  )
  UNION ALL
  SELECT
    nc.usgs_site,
    '3dhp_flowline'::text AS source_layer,
    CURRENT_DATE AS extraction_date,
    f.objectid,
    f.id3dhp,
    f.featuredate,
    f.mainstemid,
    f.featuretype,
    f.featuretypelabel,
    f.lengthkm,
    f.flowdirection,
    f.flowdirectionlabel,
    f.onsurface,
    f.onsurfacelabel,
    f.catchmentid3dhp,
    f.flowpathid3dhp,
    f.streamlevel,
    f.startflag,
    f.terminalflag,
    f.streamorder,
    f.streamcalculator,
    f.hydrosequence,
    f.dnhydrosequence,
    f.uphydrosequence,
    f.dnlevelpath,
    f.uplevelpath,
    f.pathlength,
    f.arbolatesum,
    f.divergence,
    f.divergencelabel,
    f.rtrndivergence,
    f.levelpath,
    f.terminalpath,
    f.workunitid,
    f.shape_Length AS shape_length,
    ST_Transform(ST_Intersection(f.shape, ST_Transform(nc.geometry, 6350)), 4326)::geometry(MultiLineString,4326) AS geometry
  FROM firearea.non_usgs_catchments nc
  JOIN firearea.hydro_3dhp_flowline_roi f ON ST_Intersects(f.shape, ST_Transform(nc.geometry, 6350))
  WHERE nc.usgs_site IN (
    SELECT DISTINCT usgs_site
    FROM firearea.fires_catchments
  );"

DBI::dbWithTransaction(
  conn = pg,
  {
    DBI::dbExecute(
      statement = ctas_sql,
      conn      = pg
    )
  }
)
    
  # Add PK & Indexes
  DBI::dbExecute(
    pg, "ALTER TABLE firearea.hydrography ADD COLUMN id BIGSERIAL PRIMARY KEY;")

  idx_sql <- c(
    "CREATE INDEX hydrography_usgs_site_idx ON firearea.hydrography (usgs_site)",
    "CREATE INDEX hydrography_streamorder_idx ON firearea.hydrography (streamorder)",
    "CREATE INDEX hydrography_featuretype_idx ON firearea.hydrography (featuretype)",
    "CREATE INDEX hydrography_geom_idx ON firearea.hydrography USING GIST (geometry)"
  )
  purrr::walk(idx_sql, ~ DBI::dbExecute(pg, .x))
  DBI::dbExecute(pg, "ANALYZE firearea.hydrography;")
  message("firearea.hydrography created & indexed.")
} else {
  message("Table firearea.hydro_3dhp_flowline_roi not found. Run the ogr2ogr clip first.")
}

## -----------------------------------------------------------------------------
## 4. Performance Hints -------------------------------------------------------
## -----------------------------------------------------------------------------
# * If CTAS is slow, check EXPLAIN for intersection cost and ensure geom index exists:
#   CREATE INDEX IF NOT EXISTS hydro_3dhp_flowline_roi_geom_idx ON firearea.hydro_3dhp_flowline_roi USING GIST (geom);
# * Consider pre-simplifying catchments server-side: UPDATE firearea.catchments SET geometry = ST_SimplifyPreserveTopology(geometry, 0.0001) WHERE ...;
# * Validate geometry non-null: SELECT COUNT(*) FROM firearea.hydrography WHERE geometry IS NULL;

DBI::dbDisconnect(pg)
message("ROI ingestion script complete.")