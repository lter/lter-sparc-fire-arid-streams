#!/usr/bin/env Rscript

#' @title Prefetch and Cache Basemap Assets
#' @description
#' Downloads and stores reusable basemap inputs (CONUS state outlines and
#' hillshade raster) used by the wildfire-stream figure workflow.
#' @details
#' The script computes a study-area bounding box from `catch.geojson`, retrieves
#' generalized state boundaries, derives hillshade from DEM data, and writes both
#' artifacts to local cache files under `data_cache/`.
#'
#' Reuse is automatic when cache files already exist, with optional forced
#' refresh controlled via environment variable.
#' @section Inputs:
#' \itemize{
#'   \item `catch.geojson`: spatial extent driver for DEM/hillshade generation.
#' }
#' @section Outputs:
#' \itemize{
#'   \item `data_cache/conus_states_2024_epsg5070.gpkg`: cached state outlines.
#'   \item `data_cache/hillshade_bbox_epsg5070.tif`: cached hillshade raster.
#'   \item `data_cache/basemap_cache_meta.csv`: metadata summary.
#' }
#' @section Environment Variables:
#' \describe{
#'   \item{`REFRESH_BASEMAP_CACHE`}{{Set to `1` to regenerate both basemap cache
#'   artifacts.}}
#' }
#' @section Projection:
#' All cartographic basemap assets are written in EPSG:5070 to match the map
#' rendering projection in the main figure builder.
#' @section Execution:
#' Run from the project root:
#' \preformatted{
#' Rscript download_basemap_cache.R
#' }
#' @examples
#' \dontrun{
#' Rscript download_basemap_cache.R
#' REFRESH_BASEMAP_CACHE=1 Rscript download_basemap_cache.R
#' }
#' @author
#' GitHub Copilot helper script for cached basemap preprocessing.
#' @keywords internal
invisible(NULL)

options(stringsAsFactors = FALSE)

required_pkgs <- c("sf", "dplyr", "tigris", "elevatr", "terra")
missing <- required_pkgs[!vapply(required_pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  install.packages(missing, repos = "https://cloud.r-project.org")
}

sf::sf_use_s2(TRUE)
options(tigris_use_cache = TRUE)

map_crs <- 5070
catch_path <- "catch.geojson"
if (!file.exists(catch_path)) stop("Missing catch.geojson")

cache_dir <- "data_cache"
if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

states_cache_path <- file.path(cache_dir, "conus_states_2024_epsg5070.gpkg")
states_cache_layer <- "states"
hillshade_cache_path <- file.path(cache_dir, "hillshade_bbox_epsg5070.tif")
basemap_cache_meta <- file.path(cache_dir, "basemap_cache_meta.csv")
force_refresh <- identical(Sys.getenv("REFRESH_BASEMAP_CACHE"), "1")

if (force_refresh) {
  if (file.exists(states_cache_path)) unlink(states_cache_path)
  if (file.exists(hillshade_cache_path)) unlink(hillshade_cache_path)
}

catch <- sf::st_read(catch_path, quiet = TRUE)
catch <- sf::st_make_valid(catch)
catch <- sf::st_transform(catch, 4326)
catch_union <- sf::st_union(catch)
catch_bbox_sf <- sf::st_as_sf(sf::st_as_sfc(sf::st_bbox(catch_union), crs = sf::st_crs(catch)))

if (file.exists(states_cache_path)) {
  message("Using existing cached CONUS states layer.")
  states <- sf::st_read(states_cache_path, layer = states_cache_layer, quiet = TRUE)
} else {
  message("Downloading and caching CONUS states layer...")
  states <- tigris::states(cb = TRUE, year = 2024, class = "sf") |>
    dplyr::filter(!STUSPS %in% c("AK", "HI", "PR", "VI", "MP", "GU", "AS")) |>
    sf::st_transform(map_crs)
  sf::st_write(states, states_cache_path, layer = states_cache_layer, quiet = TRUE)
}

hill <- NULL
if (file.exists(hillshade_cache_path)) {
  message("Using existing cached hillshade raster.")
  hill <- terra::rast(hillshade_cache_path)
} else {
  message("Downloading DEM and caching hillshade raster...")
  dem <- elevatr::get_elev_raster(locations = catch_bbox_sf, z = 5, clip = "bbox")
  dem_terra <- terra::rast(dem)
  dem_proj <- terra::project(dem_terra, paste0("EPSG:", map_crs), method = "bilinear")
  slope <- terra::terrain(dem_proj, v = "slope", unit = "radians")
  aspect <- terra::terrain(dem_proj, v = "aspect", unit = "radians")
  hill <- terra::shade(slope, aspect, angle = 45, direction = 315)
  terra::writeRaster(hill, hillshade_cache_path, overwrite = TRUE)
}

hill_cells <- if (!is.null(hill)) {
  terra::global(!is.na(hill), "sum", na.rm = TRUE)[1, 1]
} else {
  0
}

bb <- sf::st_bbox(catch_bbox_sf)
meta <- data.frame(
  metric = c(
    "states_cache_file",
    "hillshade_cache_file",
    "map_crs",
    "bbox_xmin",
    "bbox_ymin",
    "bbox_xmax",
    "bbox_ymax",
    "states_feature_count",
    "hillshade_non_na_cells"
  ),
  value = c(
    states_cache_path,
    hillshade_cache_path,
    map_crs,
    bb["xmin"],
    bb["ymin"],
    bb["xmax"],
    bb["ymax"],
    nrow(states),
    hill_cells
  )
)
write.csv(meta, basemap_cache_meta, row.names = FALSE)

message("Basemap cache ready.")
message("States: ", states_cache_path)
message("Hillshade: ", hillshade_cache_path)
message("Metadata: ", basemap_cache_meta)
