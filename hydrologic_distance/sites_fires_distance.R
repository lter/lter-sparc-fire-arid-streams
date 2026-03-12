# settings
source("~/Documents/localSettings/pg_local.R")
pg <- pg_local_connect("wildfire")


# data source: largest fire * analyte
largest_fire_queries <- purrr::map(
  .x = c("nitrate","ammonium","orthop","spcond"),
  .f = ~ glue::glue("
      SELECT
        '{.x}' AS analyte,
        usgs_site,
        year,
        unnest AS event,
        ordinality AS event_index,
        start_date,
        end_date,
        cum_fire_area
      FROM firearea.largest_{.x}_valid_fire_per_site
      CROSS JOIN LATERAL unnest(events) WITH ORDINALITY
    ")
)

largest_fires <- purrr::map_dfr(
  .x = largest_fire_queries,
  .f = ~ DBI::dbGetQuery(pg, .x)
)

distances <- DBI::dbGetQuery(
  conn      = pg,
  statement = "
  SELECT
    usgs_site,
    event_id,
    distance_m AS distance,
    status
  FROM firearea.site_fire_distances
   ;"
)

# computation: join largest fires to distances
largest_distance <- dplyr::left_join(
  largest_fires |> dplyr::mutate(usgs_site = tolower(usgs_site)),
  distances |> dplyr::mutate(usgs_site = tolower(usgs_site)),
  by = c(
    "usgs_site" = "usgs_site",
    "event"     = "event_id"
  )
)

# output: hits and misses
largest_distance |>
  dplyr::distinct(
    usgs_site,
    event,
    distance
  ) |>
  dplyr::mutate(
    has_distance = dplyr::case_when(
      is.na(distance) ~ FALSE,
      !is.na(distance) ~ TRUE,
      TRUE ~ distance
    )
  ) |>
  dplyr::count(has_distance)

# has_distance   n
#            0 116
#            1 432

# computation: sites sans distances
sans_distances <- largest_distance |>
  dplyr::distinct(
    usgs_site,
    event,
    distance
  ) |>
  dplyr::mutate(
    has_distance = dplyr::case_when(
      is.na(distance) ~ FALSE,
      !is.na(distance) ~ TRUE,
      TRUE ~ distance
    )
  ) |>
  dplyr::filter(is.na(distance))


#' Export Site-Level Spatial Layers and Availability Log
#'
#' Build a case-insensitive site-level spatial extract and an event-specific
#' fire-catchment extract from Postgres, then write GeoJSON outputs and a
#' per-site presence log to disk.
#'
#' @details
#' `chem_sites` is expected to come from `sans_distances` and include both
#' site and event identifiers. The function:
#' 1) normalizes `usgs_site` to lowercase for case-insensitive matching,
#' 2) queries `catchments`, `flowlines`, `hydrography`, and `pour_points`
#'    by site,
#' 3) queries `fires_catchments` by unique `(usgs_site, event)` pairs, and
#' 4) writes `spatial_presence_log.csv` plus any non-empty GeoJSON layers.
#'
#' @param chem_sites A data frame with at least:
#' \describe{
#'   \item{usgs_site}{Character site identifier (for example, "usgs-06274300").}
#'   \item{event}{Character fire event identifier used to match `event_id`.}
#' }
#' Duplicate rows are allowed; unique site and site-event combinations are used.
#'
#' @param path Character scalar. Output directory for log and GeoJSON files.
#' Directory is created recursively if needed.
#'
#' @return Invisibly returns a list with:
#' \describe{
#'   \item{presence_log}{Data frame with one row per normalized site and columns
#'     `has_catchment`, `has_fires_catchment`, `has_flowlines`,
#'     `has_pour_point`, and `any_missing`.}
#'   \item{files}{Named list of written GeoJSON file paths (only layers with
#'     `nrow(.) > 0` are included).}
#' }
#'
#' @section Side Effects:
#' Writes files under `path`:
#' \itemize{
#'   \item `spatial_presence_log.csv`
#'   \item `catchments.geojson` (if data present)
#'   \item `fires_catchments.geojson` (if data present)
#'   \item `flowlines.geojson` (if data present)
#'   \item `hydrography.geojson` (if data present)
#'   \item `pour_points.geojson` (if data present)
#' }
#'
#' @examples
#' \dontrun{
#' failed_sites_resources <- postgres_to_geojson_log(
#'   chem_sites = sans_distances,
#'   path = "/tmp/geojson"
#' )
#' }
postgres_to_geojson_log <- function(
  chem_sites,
  path
) {

  stopifnot(is.data.frame(chem_sites))
  stopifnot(all(c("usgs_site", "event") %in% names(chem_sites)))

  # normalize output directory path (ensure exists + trailing slash)
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  if (!grepl("/$", path)) {
    path <- paste0(path, "/")
  }

  # normalize site ids once for case-insensitive matching
  sans_sites <- chem_sites |>
    dplyr::pull(usgs_site) |>
    trimws() |>
    tolower() |>
    unique()

  catchments_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.catchments WHERE LOWER(usgs_site) IN ({ sans_sites* }) ;",
      .con = pg
    ),
    geometry_column = "geometry"
  )
  catchments <- catchments_usgs

  # chem_sites is the sans_distances data frame
  pairs <- chem_sites |>
    dplyr::distinct(usgs_site, event) |>
    dplyr::transmute(
      usgs_site = tolower(trimws(usgs_site)),
      event_id  = trimws(event)
    ) |>
    dplyr::filter(!is.na(usgs_site), !is.na(event_id), usgs_site != "", event_id != "")

  if (nrow(pairs) > 0) {
    pair_sql <- purrr::pmap(
      pairs,
      \(usgs_site, event_id) glue::glue_sql("({usgs_site}, {event_id})", .con = pg)
    )

    fires_catchments <- sf::st_read(
      dsn = pg,
      query = glue::glue_sql(
        "
        SELECT fc.*
        FROM firearea.fires_catchments fc
        WHERE (lower(fc.usgs_site), fc.event_id) IN ({pair_sql*});
        ",
        pair_sql = pair_sql,
        .con = pg
      ),
      geometry_column = "geometry"
    )
  } else {
    fires_catchments <- sf::st_read(
      dsn = pg,
      query = "SELECT * FROM firearea.fires_catchments WHERE FALSE;",
      geometry_column = "geometry"
    )
  }

  flowlines <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.flowlines WHERE LOWER(usgs_site) IN ({ sans_sites* }) ;",
      .con = pg
    ),
    geometry_column = "geometry"
  )

  hydrography <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.hydrography WHERE LOWER(usgs_site) IN ({ sans_sites* }) ;",
      .con = pg
    ),
    geometry_column = "geometry"
  )

  pour_points_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.pour_points WHERE LOWER(usgs_site) IN ({ sans_sites* }) ;",
      .con = pg
    ),
    geometry_column = "geometry"
  )
  pour_points <- pour_points_usgs

  # build per-site presence log
  # presence_log: records whether each requested site has spatial data
  # columns: has_catchment, has_fires_catchment, has_flowlines, has_pour_point, any_missing
  # written to spatial_presence_log.csv in provided path and returned invisibly
  presence_log <- data.frame(
    usgs_site = sans_sites,
    stringsAsFactors = FALSE
  ) |>
    dplyr::mutate(
      has_catchment       = usgs_site %in% tolower(catchments$usgs_site),
      has_fires_catchment = usgs_site %in% tolower(fires_catchments$usgs_site),
      has_flowlines       = usgs_site %in% tolower(flowlines$usgs_site),
      has_pour_point      = usgs_site %in% tolower(pour_points$usgs_site),
      any_missing = !(has_catchment & has_fires_catchment & has_flowlines & has_pour_point)
    )
  # write log to path
  readr::write_csv(presence_log, file = file.path(path, "spatial_presence_log.csv"))

  out_files <- list()

  if (nrow(catchments) > 0) {
    sf::st_write(
      obj = catchments,
      dsn = paste0(path, "catchments.geojson"),
      delete_dsn = TRUE,
      quiet = TRUE
    )
    out_files$catchments <- paste0(path, "catchments.geojson")
  } else {
    message("No catchments to write.")
  }

  if (nrow(fires_catchments) > 0) {
    sf::st_write(
      obj = fires_catchments,
      dsn = paste0(path, "fires_catchments.geojson"),
      delete_dsn = TRUE,
      quiet = TRUE
    )
    out_files$fires_catchments <- paste0(path, "fires_catchments.geojson")
  } else {
    message("No fires_catchments to write.")
  }

  if (nrow(flowlines) > 0) {
    sf::st_write(
      obj = flowlines,
      dsn = paste0(path, "flowlines.geojson"),
      delete_dsn = TRUE,
      quiet = TRUE
    )
    out_files$flowlines <- paste0(path, "flowlines.geojson")
  } else {
    message("No flowlines to write.")
  }

  if (nrow(hydrography) > 0) {
    sf::st_write(
      obj = hydrography,
      dsn = paste0(path, "hydrography.geojson"),
      delete_dsn = TRUE,
      quiet = TRUE
    )
    out_files$hydrography <- paste0(path, "hydrography.geojson")
  } else {
    message("No hydrography to write.")
  }

  if (nrow(pour_points) > 0) {
    sf::st_write(
      obj = pour_points,
      dsn = paste0(path, "pour_points.geojson"),
      delete_dsn = TRUE,
      quiet = TRUE
    )
    out_files$pour_points <- paste0(path, "pour_points.geojson")
  } else {
    message("No pour_points to write.")
  }

  # summary message
  message("postgres_to_geojson wrote ", length(out_files), " GeoJSON file(s) to ", path)

  # return both presence log and file paths
  return(invisible(list(
    presence_log = presence_log,
    files = out_files
  )))

  invisible(presence_log)
}

failed_sites_resources <- postgres_to_geojson_log(
  chem_sites = sans_distances,
  path       = "/tmp/geojson"
)