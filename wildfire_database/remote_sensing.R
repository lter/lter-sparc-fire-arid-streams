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

#' @title Fire Classifications by USGS Site
#'
#' @description Build a normalized fire classifications table for each site from
#' return interval, regime group, vegetation departure, and risk products.
#'
#' Workflow:
#' - Read source CSVs for each classification product.
#' - Standardize to a common schema and stack rows.
#' - Rename value/category columns for downstream clarity.
#' - Deterministically order output before database write.
#'
#' Assumptions:
#' - Each source has one representative mean value per usgs_site.
#' - Classification labels are fixed to return_interval, regime_group,
#'   veg_departure, and fire_risk.
#'
#' Reproducibility:
#' - Deterministic ordering by usgs_site, fire_classification.
#' - No stochastic transforms.

fire_classifications <- dplyr::bind_rows(
  readr::read_csv(
    file = here::here("data", "fri_crass-catchments.csv"),
    show_col_types = FALSE
  ) |>
    dplyr::select(usgs_site, mean) |>
    dplyr::mutate(fire_classification = "return_interval"),
  readr::read_csv(
    file = here::here("data", "frg_crass-catchments.csv"),
    show_col_types = FALSE
  ) |>
    dplyr::select(usgs_site, mean) |>
    dplyr::mutate(fire_classification = "regime_group"),
  readr::read_csv(
    file = here::here("data", "vd_crass-catchments.csv"),
    show_col_types = FALSE
  ) |>
    dplyr::select(usgs_site, mean) |>
    dplyr::mutate(fire_classification = "veg_departure"),
  readr::read_csv(
    file = here::here("data", "risk_crass-catchments.csv"),
    show_col_types = FALSE
  ) |>
    dplyr::select(usgs_site, mean) |>
    dplyr::mutate(fire_classification = "fire_risk")
) |>
  dplyr::rename(classification_value = mean) |>
  dplyr::arrange(usgs_site, fire_classification)

DBI::dbWriteTable(
  conn      = pg,
  name      = c("firearea", "fire_classifications"),
  value     = fire_classifications,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE UNIQUE INDEX IF NOT EXISTS fire_classifications_site_class_uq_idx ON firearea.fire_classifications (usgs_site, fire_classification)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS fire_classifications_class_idx ON firearea.fire_classifications (fire_classification)"
)

#' @title Fire Severity by Site and Event
#'
#' @description Transform fire severity class totals from wide to long format
#' for each usgs_site and fire event.
#'
#' Workflow:
#' - Read fire severity source data.
#' - Convert low/moderate/high area values to catchment-relative values using
#'   class_area / fire_area.
#' - Pivot to long format with severity class and numeric value.
#' - Drop rows with missing severity values and deterministically order output.
#'
#' Assumptions:
#' - X1, X2, and X3 correspond to low, moderate, and high severity.
#' - firearea is the denominator used to scale severity class values.
#' - Unburned class (X0) is intentionally excluded from this table.
#'
#' Reproducibility:
#' - Deterministic ordering by usgs_site, event_id, fire_year, fire_severity.
#' - No stochastic transforms.

fire_severity <- readr::read_csv(
  file = here::here("data", "all_crass_fire_severity.csv"),
  show_col_types = FALSE
) |>
  dplyr::select(
    usgs.site,
    fireid,
    fireYear,
    firearea,
    X1,
    X2,
    X3
  ) |>
  dplyr::rename(
    usgs_site = usgs.site,
    event_id = fireid,
    fire_year = fireYear,
    fire_area = firearea,
    low = X1,
    moderate = X2,
    high = X3
  ) |>
  dplyr::mutate(
    dplyr::across(
      .cols = c(low, moderate, high),
      .fns = ~ dplyr::if_else(
        !is.na(fire_area) & fire_area > 0,
        .x / fire_area,
        NA_real_
      )
    )
  ) |>
  dplyr::select(-fire_area) |>
  dplyr::mutate(
    dplyr::across(
      .cols = c(low, moderate, high),
      .fns = as.numeric
    )
  ) |>
  tidyr::pivot_longer(
    cols = c(low, moderate, high),
    names_to = "fire_severity",
    values_to = "severity_value"
  ) |>
  dplyr::filter(!is.na(severity_value)) |>
  dplyr::arrange(usgs_site, event_id, fire_year, fire_severity)

DBI::dbWriteTable(
  conn      = pg,
  name      = c("firearea", "fire_severity"),
  value     = fire_severity,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE UNIQUE INDEX IF NOT EXISTS fire_severity_site_event_year_class_uq_idx ON firearea.fire_severity (usgs_site, event_id, fire_year, fire_severity)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS fire_severity_site_event_idx ON firearea.fire_severity (usgs_site, event_id)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS fire_severity_event_class_idx ON firearea.fire_severity (event_id, fire_severity)"
)

#' @title Landcover Group Composition by Site and Event
#'
#' @description Transform landcover group percentages from wide to long format
#' for each usgs_site and fire event.
#'
#' Workflow:
#' - Read landcover group source data.
#' - Keep core landcover groups used in analysis.
#' - Pivot to long format with group label and value.
#' - Drop missing values and deterministically order output.
#'
#' Assumptions:
#' - Landcover values are percent-like metrics and should remain as provided.
#' - event_id identifies fire events consistently across rows.
#'
#' Reproducibility:
#' - Deterministic ordering by usgs_site, event_id, fire_year, landcover_group.
#' - No stochastic transforms.

landcover_groups <- readr::read_csv(
  file = here::here("data", "landcover_groups.csv"),
  show_col_types = FALSE
) |>
  dplyr::select(
    catchment,
    event_id,
    fireYear,
    forest,
    shrub,
    grass,
    urban,
    ag,
    barren
  ) |>
  dplyr::rename(
    usgs_site = catchment,
    fire_year = fireYear
  ) |>
  tidyr::pivot_longer(
    cols = c(forest, shrub, grass, urban, ag, barren),
    names_to = "landcover_group",
    values_to = "group_value"
  ) |>
  dplyr::filter(!is.na(group_value)) |>
  dplyr::arrange(usgs_site, event_id, fire_year, landcover_group)

DBI::dbWriteTable(
  conn      = pg,
  name      = c("firearea", "landcover_groups"),
  value     = landcover_groups,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE UNIQUE INDEX IF NOT EXISTS landcover_groups_site_event_year_group_uq_idx ON firearea.landcover_groups (usgs_site, event_id, fire_year, landcover_group)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS landcover_groups_site_event_idx ON firearea.landcover_groups (usgs_site, event_id)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS landcover_groups_event_group_idx ON firearea.landcover_groups (event_id, landcover_group)"
)
DBI::dbExecute(
  pg,
  "COMMENT ON COLUMN firearea.landcover_groups.landcover_group IS 'landcover class label for the burned area category'"
)
DBI::dbExecute(
  pg,
  "COMMENT ON COLUMN firearea.landcover_groups.group_value IS 'percent of the area burned of each landcover group'"
  )