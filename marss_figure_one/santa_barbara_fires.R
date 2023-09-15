#' @title Delineate catchments of Santa Barbara study sites and pair to
#' corresponding catchment fires
#'
#' @description This is a modified version of the workflow to delineate
#' catchments of Santa Barbara study sites, and crop relevant fires to those
#' catchments. This is not the original workflow used to produce these data and
#' is included here for reference only, though it should mirror closely, if not
#' exactly, the original workflow.
#'
#' @note `mtbs_fire_perimiters` is the layer of MTBS fire data, which is not
#' included as part of this resource.
#'
santa_barbara <- readr::read_csv("data/sbc_sites_streams.csv")

dcss_possibly <- purrr::possibly(
  .f = firearea::delineate_catchment_streamstats,
  otherwise = NULL
)

santa_barbara_ws <- split(
  x = santa_barbara,
  f = santa_barbara$sitecode
) |>
{\(df) purrr::map_df(.x = df, ~ dcss_possibly(longitude = .x$Lon, latitude = .x$Lat, state_code = "ca", location_identifier = .x$sitecode))}()

santa_barbara_ws <- sf::st_transform(
  x   = santa_barbara_ws,
  crs = sf::st_crs(mtbs_fire_perimiters)
)

santa_barbara_ws <- firearea::validate_sf_objects(santa_barbara_ws)

santa_barbara_fires <- split(
  x = santa_barbara_ws,
  f = santa_barbara_ws$location_id
) |>
{\(catchment) purrr::map_dfr(.x = catchment, ~ sf::st_intersection(x = .x, y = mtbs_fire_perimiters))}()

santa_barbara_fires <- santa_barbara_fires |>
dplyr::mutate(
  total_burn_area_km2 = BurnBndAc * 0.0040468564,
  total_burn_area_km2 = units::set_units(total_burn_area_km2, "km^2"),
  ws_burn_area_m2     = sf::st_area(x = geometry),
  ws_burn_area_km2    = units::set_units(ws_burn_area_m2, value = "km^2")
  ) |>
dplyr::select(-ws_burn_area_m2)

santa_barbara_fires_data <- santa_barbara_fires |>
  sf::st_drop_geometry()

sf::st_write(
  obj        = santa_barbara_ws,
  dsn        = "santa_barbara_catchments.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)

sf::st_write(
  obj        = santa_barbara_fires,
  dsn        = "santa_barbara_fires.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)

## single site: MC06

# Lat: 34.44072
# Lon: -119.71244
# Site: Mission Creek (at Rocky Nook)
# sitecode: MC06

santa_barbara_ws <- firearea::delineate_catchment_streamstats(
  longitude           = -119.71244,
  latitude            = 34.44072,
  state_code          = "ca",
  location_identifier = "MC06"
)

santa_barbara_fires <- sf::st_intersection(
  x = santa_barbara_ws,
  y = mtbs_fire_perimiters
)

santa_barbara_fires <- santa_barbara_fires |>
dplyr::mutate(
  total_burn_area_km2 = BurnBndAc * 0.0040468564,
  total_burn_area_km2 = units::set_units(total_burn_area_km2, "km^2"),
  ws_burn_area_m2     = sf::st_area(x = geometry),
  ws_burn_area_km2    = units::set_units(ws_burn_area_m2, value = "km^2")
  ) |>
dplyr::select(-ws_burn_area_m2)

santa_barbara_fires_data <- santa_barbara_fires |>
  sf::st_drop_geometry()

sf::st_write(
  obj        = santa_barbara_ws |> sf::st_transform(crs = 4326),
  dsn        = "MC06.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)

sf::st_write(
  obj        = santa_barbara_fires |> sf::st_transform(crs = 4326),
  dsn        = "MC06_fires.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)

## check area

# from streamstats output:  Shape_Area  <int> 17011800
# calculated:               sf::st_area(santa_barbara_ws) ~ 17013912 [m^2]
