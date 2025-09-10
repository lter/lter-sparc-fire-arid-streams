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

# data source: calculated distances
distances <- readr::read_csv("combined_distances.csv")


# computation: join largest fires to distances
largest_distance <- dplyr::left_join(
  largest_fires,
  distances,
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
  # dplyr::filter(is.na(distance)) |>
# dplyr::distinct(usgs_site) # |>
# readr::write_csv("/tmp/sans_distances.csv")

# only able to calculate distance for ~40% of distinct(fire*catchment)
# distance>=0	count	percent
# False     	337 	61.50
# True	      211 	38.50

# data source: distance calc logs
failed_sites <- readr::read_csv("failed_sites.csv") |>
  dplyr::mutate(usgs_site = paste0("USGS-", site))


# data source: comid counts
counts <- DBI::dbGetQuery(
  pg,
  "select
    usgs_site,
    count(nhdplus_comid) AS comid_count
  from firearea.flowlines
  group by usgs_site ;"
  )


# computation: sites sans distances (adjust distinct() as needed)
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
  dplyr::filter(is.na(distance)) |>
  dplyr::distinct(usgs_site) |>
  dplyr::arrange(usgs_site)
  # readr::write_csv("/tmp/sans_distances.csv")


# data source: use a modified postres_to_geojson fn to get a lot of layer availability 

postgres_to_geojson_log <- function(
  chem_sites,
  path
) {
  # normalize output directory path (ensure exists + trailing slash)
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
  if (!grepl("/$", path)) {
    path <- paste0(path, "/")
  }
  # initialize list to store presence/absence per site
  presence_list <- list()
  catchments_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )
  catchments <- catchments_usgs

  fires_catchments <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.fires_catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  flowlines <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.flowlines WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  pour_points_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.pour_points WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )
  pour_points <- pour_points_usgs

  # build per-site presence log
  # presence_log: records whether each requested site has spatial data
  # columns: has_catchment, has_fires_catchment, has_flowlines, has_pour_point, any_missing
  # written to spatial_presence_log.csv in provided path and returned invisibly
  unique_sites <- unique(chem_sites)
  for (s in unique_sites) {
    presence_list[[s]] <- data.frame(
      usgs_site = s,
      has_catchment       = any(catchments$usgs_site == s),
      has_fires_catchment = any(fires_catchments$usgs_site == s),
      has_flowlines       = any(flowlines$usgs_site == s),
      has_pour_point      = any(pour_points$usgs_site == s),
      stringsAsFactors = FALSE
    )
  }
  presence_log <- dplyr::bind_rows(presence_list)
  presence_log$any_missing <- !(presence_log$has_catchment & presence_log$has_fires_catchment & presence_log$has_flowlines & presence_log$has_pour_point)
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
  chem_sites = sans_distances$usgs_site,
  path       = "/tmp/geojson"
)

failed_sites_resources$presence_log


# output: detailed hits and misses

#   success:
#     none  = no distances for any fire (probably a layer problem)
#     all   = distances for all fires (all good)
#     mixed = distances for some fires (likely a fire not on flowline)

dplyr::left_join(
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
  ),
  failed_sites,
  by = "usgs_site"
) |>
dplyr::left_join(
  counts,
  by = "usgs_site"
) |>
dplyr::left_join(
  failed_sites_resources$presence_log,
  by = "usgs_site"
) |>
dplyr::left_join(
largest_distance |>
  dplyr::distinct(usgs_site, event, distance) |>
  dplyr::mutate(has_distance = as.integer(!is.na(distance))) |>
  dplyr::group_by(usgs_site) |>
  dplyr::summarise(
    min_has = min(has_distance),
    max_has = max(has_distance),
    .groups = "drop"
  ) |>
  dplyr::mutate(
    success = dplyr::case_when(
      min_has == 0 & max_has == 0 ~ "none",
      min_has == 1 & max_has == 1 ~ "all",
      min_has == 0 & max_has == 1 ~ "mixed"
    )
  ),
  by = "usgs_site"
) |>
dplyr::arrange(usgs_site) |>
dplyr::filter(success == "none") |>
readr::write_csv("/tmp/sans_distances.csv")


# helper: get geos (catch, fires, flows, pour) for a site
get_geos <- function(chem_sites) {
  catchments_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )
  catchments <- catchments_usgs

  fires_catchments <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.fires_catchments WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  flowlines <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.flowlines WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )

  pour_points_usgs <- sf::st_read(
    dsn = pg,
    query = glue::glue_sql(
      "SELECT * FROM firearea.pour_points WHERE usgs_site IN ({ chem_sites* }) ;",
      .con = DBI::ANSI()
    ),
    geometry_column = "geometry"
  )
  pour_points <- pour_points_usgs

  return(
    list(
      catchments = catchments,
      fires_catchments = fires_catchments,
      flowlines = flowlines,
      pour_points = pour_points
    )
  )

}


# investigate

source("distance_helper_functions.R")

this_site <- get_geos(chem_sites = c("USGS-11060400"))

this_site <- get_geos(chem_sites = c("USGS-08402000"))
simple_plot(chem_site = "USGS-08402000", interactive = TRUE)

this_site <- get_geos(chem_sites = c("USGS-07110400"))
simple_plot(chem_site = "USGS-07110400", interactive = TRUE)

this_site <- get_geos(chem_sites = c("USGS-07103990")) # single fire without overlap
simple_plot(chem_site = "USGS-07103990")

# something is wrong with firearea::calculate_fire_flow_length (source it instead)

# this_dist <- firearea::calculate_fire_flow_length(
#   catchment           = this_site$catchments,
#   catchment_fire      = this_site$fires_catchments,
#   catchment_flowlines = this_site$flowlines,
#   catchment_sampling  = this_site$pour_points
# )

catchment_fires_distances <- split(
  x = this_site$fires_catchments,
  f = this_site$fires_catchments$event_id
) |>
  {
    \(fire) {
      purrr::map(
        .x = fire,
        .f = ~ calculate_fire_flow_length(
          catchment = this_site$catchments,
          catchment_fire = .x,
          catchment_flowlines = this_site$flowlines,
          catchment_sampling = this_site$pour_points
        )
      )
    }
  }() |>
  dplyr::bind_rows()