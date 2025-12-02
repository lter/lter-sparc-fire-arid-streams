#' @title NLCD of USGS sites
#'
#' @description Compute the percent contribution of each NLCD class's pixels to the total
#' pixel count per usgs_site.
#'
#' Workflow:
#' - Read NLCD pixel histogram CSV and NLCD code lookup.
#' - Parse histogram strings into tidy rows (class, pixels) per usgs_site.
#' - Aggregate pixels by (usgs_site, class), compute per-site totals.
#' - Derive percent: 100 * class_pixels / site_total_pixels.
#'
#' Assumptions:
#' - Input histogram column is formatted as {code=pixels, code=pixels, ...} or code=pixels pairs.
#' - nlcd_codes optionally maps integer class to labels; not required here.
#'
#' Reproducibility:
#' - Deterministic ordering by usgs_site, class.
#' - No external randomness; strictly functional transforms.
#'
#' @note data source: https://developers.google.com/earth-engine/datasets/catalog/USGS_NLCD_RELEASES_2020_REL_NALCMS
#'
#' @export

nlcd <- readr::read_csv(
  file           = here::here("data", "NLCD_Pixel_Counts_crass-catchments.csv"),
  show_col_types = FALSE
)

nlcd_codes <- readr::read_csv(
  file           = here::here("data", "nlcd_codes.csv"),
  show_col_types = FALSE
)

nlcd_long <- nlcd |>
  dplyr::select(
    usgs_site,
    histogram
  ) |>
  dplyr::mutate(
    histogram = stringr::str_remove_all(histogram, "^\\s*\\{|\\}\\s*$")
  ) |>
  tidyr::separate_rows(histogram, sep = ",\\s*") |>
  tidyr::separate(
    histogram,
    into    = c("class", "pixels"),
    sep     = "=",
    fill    = "right",
    convert = TRUE
  ) |>
  dplyr::mutate(
    class  = as.integer(stringr::str_trim(class)),
    pixels = as.numeric(stringr::str_trim(pixels))
  )

nlcd_percent <- nlcd_long |>
  dplyr::filter(
    !is.na(usgs_site),
    !is.na(class),
    !is.na(pixels)
  ) |>
  dplyr::mutate(
    class  = as.integer(class),
    pixels = as.numeric(pixels)
  ) |>
  dplyr::group_by(
    usgs_site,
    class
  ) |>
  dplyr::summarise(
    class_pixels = sum(pixels, na.rm = TRUE),
    .groups      = "drop_last"
  ) |>
  dplyr::mutate(site_total_pixels = sum(class_pixels, na.rm = TRUE)) |>
  dplyr::ungroup() |>
  dplyr::mutate(
    class_percent = dplyr::if_else(
      site_total_pixels > 0,
      100 * class_pixels / site_total_pixels,
      NA_real_
    )
  ) |>
  dplyr::arrange(usgs_site, class) |>
  dplyr::left_join(
    y  = nlcd_codes,
    by = c("class")
  )

DBI::dbWriteTable(
  conn      = pg,
  name      = c("firearea", "nlcd"),
  value     = nlcd_percent,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS nlcd_site_idx ON firearea.nlcd (usgs_site)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS nlcd_class_idx ON firearea.nlcd (class)"
)