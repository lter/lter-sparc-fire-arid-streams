#!/usr/bin/env Rscript

#' @title Prefetch and Cache NHDPlus Hydrology
#' @description
#' Downloads and stores higher-order NHDPlus flowlines for the study region so
#' iterative map builds do not repeatedly query remote services.
#' @details
#' The script reads `catch.geojson`, computes the study bounding box, requests
#' NHDPlus flowlines at or above a configured Strahler order threshold, and
#' writes the result to a local GeoPackage cache.
#'
#' If a valid cache already exists, it is reused unless refresh is explicitly
#' requested via environment variable.
#' @section Inputs:
#' \itemize{
#'   \item `catch.geojson`: study-area polygon used to define retrieval extent.
#' }
#' @section Outputs:
#' \itemize{
#'   \item `data_cache/nhdplus_flowlines_so*_bbox.gpkg`: cached flowlines.
#'   \item `data_cache/nhdplus_flowlines_so*_bbox_meta.csv`: cache metadata.
#' }
#' @section Environment Variables:
#' \describe{
#'   \item{`REFRESH_HYDRO_CACHE`}{{Set to `1` to force redownload and overwrite
#'   existing hydrology cache.}}
#' }
#' @section Validation:
#' The script enforces non-empty hydrology results both before and after stream
#' order filtering, and exits with an error if no features remain.
#' @section Execution:
#' Run from the project root:
#' \preformatted{
#' Rscript download_hydrology_cache.R
#' }
#' @examples
#' \dontrun{
#' Rscript download_hydrology_cache.R
#' REFRESH_HYDRO_CACHE=1 Rscript download_hydrology_cache.R
#' }
#' @author
#' GitHub Copilot helper script for cached hydrology preprocessing.
#' @keywords internal
invisible(NULL)

options(stringsAsFactors = FALSE)

required_pkgs <- c("sf", "dplyr", "nhdplusTools")
missing <- required_pkgs[!vapply(required_pkgs, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing) > 0) {
  install.packages(missing, repos = "https://cloud.r-project.org")
}

sf::sf_use_s2(TRUE)
hydro_streamorder_min <- 5

catch_path <- "catch.geojson"
if (!file.exists(catch_path)) stop("Missing catch.geojson")

cache_dir <- "data_cache"
if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

hydro_cache_path <- file.path(
  cache_dir,
  paste0("nhdplus_flowlines_so", hydro_streamorder_min, "_bbox.gpkg")
)
hydro_cache_layer <- paste0("flowlines_so", hydro_streamorder_min)
hydro_cache_meta <- file.path(
  cache_dir,
  paste0("nhdplus_flowlines_so", hydro_streamorder_min, "_bbox_meta.csv")
)
force_refresh <- identical(Sys.getenv("REFRESH_HYDRO_CACHE"), "1")

catch <- sf::st_read(catch_path, quiet = TRUE)
catch <- sf::st_make_valid(catch)
catch <- sf::st_transform(catch, 4326)
catch_union <- sf::st_union(catch)
catch_bbox <- sf::st_as_sf(sf::st_as_sfc(sf::st_bbox(catch_union), crs = sf::st_crs(catch)))

if (file.exists(hydro_cache_path) && !force_refresh) {
  streams <- sf::st_read(hydro_cache_path, layer = hydro_cache_layer, quiet = TRUE)
  streams <- streams |>
    dplyr::mutate(streamorde = as.numeric(streamorde)) |>
    dplyr::filter(streamorde >= hydro_streamorder_min)

  message("Hydrology cache already exists and was reused.")
  message("Cache: ", hydro_cache_path)
  message("Features: ", nrow(streams))
  quit(save = "no", status = 0)
}

message(
  "Downloading NHDPlus flowlines for study bbox (Strahler >= ",
  hydro_streamorder_min,
  ")..."
)
streams <- nhdplusTools::get_nhdplus(
  AOI = catch_bbox,
  realization = "flowline",
  streamorder = hydro_streamorder_min
)

if (!inherits(streams, "sf") || nrow(streams) == 0) {
  stop("NHDPlus query returned no flowlines.")
}

streams <- streams |>
  dplyr::mutate(streamorde = as.numeric(streamorde)) |>
  dplyr::filter(streamorde >= hydro_streamorder_min)

if (nrow(streams) == 0) {
  stop("No flowlines remained after streamorder filtering.")
}

if (file.exists(hydro_cache_path)) unlink(hydro_cache_path)
sf::st_write(streams, hydro_cache_path, layer = hydro_cache_layer, quiet = TRUE)

bb <- sf::st_bbox(catch_bbox)
cache_meta <- data.frame(
  metric = c(
    "cache_file",
    "streamorder_min",
    "bbox_xmin",
    "bbox_ymin",
    "bbox_xmax",
    "bbox_ymax",
    "feature_count"
  ),
  value = c(
    hydro_cache_path,
    hydro_streamorder_min,
    bb["xmin"],
    bb["ymin"],
    bb["xmax"],
    bb["ymax"],
    nrow(streams)
  )
)
write.csv(cache_meta, hydro_cache_meta, row.names = FALSE)

message("Hydrology cache written.")
message("Cache: ", hydro_cache_path)
message("Metadata: ", hydro_cache_meta)
message("Features: ", nrow(streams))
