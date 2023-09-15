#' @title Delineate catchments of Valles Caldera study sites and pair to
#' corresponding catchment fires
#'
#' @description This is a modified version of the workflow to delineate
#' catchments of Valles Caldera study sites, and crop relevant fires to those
#' catchments. This is not the original workflow used to produce these data and
#' is included here for reference only, though it should mirror closely, if not
#' exactly, the original workflow.
#'
#' @note `fire_perimiters` is the layer of MTBS fire data, which is not
#' included in this resource.
#'
#' @note sampling location data are downloaded from a CRASS project GoogleSheet
#' https://docs.google.com/spreadsheets/d/1lds34KtrZ90v658RFqgWJRyGARgXq2ku7-HiGr_CrIk/edit?pli=1#gid=0
#'

# sampling locations -----------------------------------------------------------

new_mexico_points <- readr::read_csv("BGC_datasets_datasets.csv") |>
  dplyr::filter(grepl("mexico", State, ignore.case = TRUE)) |>
  tibble::add_row(
    LAT    = 35.97269,
    LON    = -106.59759,
    Stream = "san_antonio_custom"
  ) |>
  dplyr::group_by(Stream) |>
  dplyr::distinct(LAT, LON) |>
  dplyr::ungroup() |> 
  dplyr::rename(
    longitude = LON,
    latitude  = LAT,
    ) |>
  sf::st_as_sf(
    coords = c("longitude", "latitude"),
    crs    = 4326
    ) |>
  sf::st_transform(crs = 4269)

sf::st_write(
  obj        = new_mexico_points,
  dsn        = "new_mexico_points.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)


# catchments -------------------------------------------------------------------

new_mexico_ws <- split(
  x = new_mexico_points,
  f = new_mexico_points$Stream
) |>
  purrr::map(~ firearea::delineate_catchment(location_sf = .x, stream_length = 1000, location_identifier = "Stream")) |> 
  purrr::list_rbind()

sf::st_write(
  obj        = new_mexico_ws,
  dsn        = "new_mexico_catchments.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)


# fires ------------------------------------------------------------------------

fire_perimiters <- firearea::format_fire_perimeters(
  fire_layer       = fire_perimiters,
  state_code       = "nm",
  year_lower_bound = 2005,
  year_upper_bound = 2015
)

nm_fires <- split(
  x = new_mexico_ws,
  f = new_mexico_ws$location_id
) |>
{\(catchment) purrr::map_dfr(.x = catchment, ~ sf::st_intersection(x = .x, y = fire_perimiters))}()

sf::st_write(
  obj        = nm_fires,
  dsn        = "new_mexico_fires.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)
