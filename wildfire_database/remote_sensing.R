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
#' Execution:
#' - Ensure `pg` is an active DBI connection.
#' - Run this section (or source this file) to write `covariates.nlcd`.
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

DBI::dbExecute(
  pg,
  "CREATE SCHEMA IF NOT EXISTS covariates"
)

DBI::dbWriteTable(
  conn      = pg,
  name      = c("covariates", "nlcd"),
  value     = nlcd_percent,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS nlcd_site_idx ON covariates.nlcd (usgs_site)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS nlcd_class_idx ON covariates.nlcd (class)"
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

DBI::dbExecute(
  pg,
  "CREATE SCHEMA IF NOT EXISTS covariates"
)

DBI::dbWriteTable(
  conn      = pg,
  name      = c("covariates", "fire_classifications"),
  value     = fire_classifications,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE UNIQUE INDEX IF NOT EXISTS fire_classifications_site_class_uq_idx ON covariates.fire_classifications (usgs_site, fire_classification)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS fire_classifications_class_idx ON covariates.fire_classifications (fire_classification)"
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
#'
#' Execution:
#' - Ensure `pg` is an active DBI connection.
#' - Run this section (or source this file) to write `covariates.fire_severity`.

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

  DBI::dbExecute(
    pg,
    "CREATE SCHEMA IF NOT EXISTS covariates"
  )

DBI::dbWriteTable(
  conn      = pg,
  name      = c("covariates", "fire_severity"),
  value     = fire_severity,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE UNIQUE INDEX IF NOT EXISTS fire_severity_site_event_year_class_uq_idx ON covariates.fire_severity (usgs_site, event_id, fire_year, fire_severity)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS fire_severity_site_event_idx ON covariates.fire_severity (usgs_site, event_id)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS fire_severity_event_class_idx ON covariates.fire_severity (event_id, fire_severity)"
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
#'
#' Execution:
#' - Ensure `pg` is an active DBI connection.
#' - Run this section (or source this file) to write `covariates.landcover_groups`.

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

  DBI::dbExecute(
    pg,
    "CREATE SCHEMA IF NOT EXISTS covariates"
  )

DBI::dbWriteTable(
  conn      = pg,
  name      = c("covariates", "landcover_groups"),
  value     = landcover_groups,
  overwrite = TRUE,
  row.names = FALSE
)

DBI::dbExecute(
  pg,
  "CREATE UNIQUE INDEX IF NOT EXISTS landcover_groups_site_event_year_group_uq_idx ON covariates.landcover_groups (usgs_site, event_id, fire_year, landcover_group)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS landcover_groups_site_event_idx ON covariates.landcover_groups (usgs_site, event_id)"
)
DBI::dbExecute(
  pg,
  "CREATE INDEX IF NOT EXISTS landcover_groups_event_group_idx ON covariates.landcover_groups (event_id, landcover_group)"
)
DBI::dbExecute(
  pg,
  "COMMENT ON COLUMN covariates.landcover_groups.landcover_group IS 'landcover class label for the burned area category'"
)
DBI::dbExecute(
  pg,
  "COMMENT ON COLUMN covariates.landcover_groups.group_value IS 'percent of the area burned of each landcover group'"
  )


#' @title Read Catchment Fire Risk Mean
#'
#' @description Read and standardize catchment fire risk values from
#' `risk_crass-catchments.csv`.
#'
#' Workflow:
#' - Read source CSV.
#' - Validate required columns (`usgs_site`, `mean`).
#' - Rename `mean` to `risk_mean` and coerce numeric/text types.
#' - Enforce one record per `usgs_site`.
#'
#' Execution:
#' - Call `read_fire_risk_covariate()` to return a standardized tibble.
#' - Optionally provide `file_path` to use a non-default source file.
#'
#' @param file_path Path to risk CSV.
#'
#' @return A tibble with columns `usgs_site` and `risk_mean`.
read_fire_risk_covariate <- function(file_path = here::here("data", "risk_crass-catchments.csv")) {
  read_catchment_metric_covariate(
    file_path = file_path,
    metric_name = "risk_mean"
  )
}


#' @title Read Catchment Fire Regime Group Mean
#'
#' @description Read and standardize catchment fire regime group values from
#' `frg_crass-catchments.csv`.
#'
#' Execution:
#' - Call `read_fire_regime_group_covariate()` to return a standardized tibble.
#' - Optionally provide `file_path` to use a non-default source file.
#'
#' @param file_path Path to fire regime group CSV.
#'
#' @return A tibble with columns `usgs_site` and `frg_mean`.
read_fire_regime_group_covariate <- function(file_path = here::here("data", "frg_crass-catchments.csv")) {
  read_catchment_metric_covariate(
    file_path = file_path,
    metric_name = "frg_mean"
  )
}


#' @title Read Catchment Mean Fire Return Interval
#'
#' @description Read and standardize catchment mean fire return interval values
#' from `fri_crass-catchments.csv`.
#'
#' Execution:
#' - Call `read_fire_return_interval_covariate()` to return a standardized tibble.
#' - Optionally provide `file_path` to use a non-default source file.
#'
#' @param file_path Path to fire return interval CSV.
#'
#' @return A tibble with columns `usgs_site` and `fri_mean`.
read_fire_return_interval_covariate <- function(file_path = here::here("data", "fri_crass-catchments.csv")) {
  read_catchment_metric_covariate(
    file_path = file_path,
    metric_name = "fri_mean"
  )
}


#' @title Read Catchment Vegetation Departure Mean
#'
#' @description Read and standardize catchment vegetation departure values from
#' `vd_crass-catchments.csv`.
#'
#' Execution:
#' - Call `read_vegetation_departure_covariate()` to return a standardized tibble.
#' - Optionally provide `file_path` to use a non-default source file.
#'
#' @param file_path Path to vegetation departure CSV.
#'
#' @return A tibble with columns `usgs_site` and `vd_mean`.
read_vegetation_departure_covariate <- function(file_path = here::here("data", "vd_crass-catchments.csv")) {
  read_catchment_metric_covariate(
    file_path = file_path,
    metric_name = "vd_mean"
  )
}


#' @title Build and Export Catchment Fire Regime Covariates
#'
#' @description Build a consolidated catchment-level fire regime covariate table
#' and write it to `covariates.catchment_fire_regime`.
#'
#' Workflow:
#' - Read four source products (risk, FRG, FRI, vegetation departure).
#' - Standardize each source to one row per `usgs_site` and one metric column.
#' - Full-join all sources so partial coverage is retained and missing metrics
#'   remain `NA`.
#' - Validate output key and column set.
#' - Recreate destination table and add index + metadata comments.
#'
#' Assumptions:
#' - `usgs_site` values are retained exactly as provided (including `USGS-`
#'   prefix) to preserve source fidelity.
#' - Units are source-defined and not reliably documented in this repository;
#'   comments therefore document meaning without asserting units.
#' - This table is staged for future inclusion in
#'   `firearea.export_analyte_covariates_pre_post` by joining on `usgs_site`.
#'
#' Reproducibility:
#' - Deterministic ordering by `usgs_site`.
#' - Duplicate `usgs_site` values in any source are treated as an error.
#'
#' Execution:
#' - Ensure `conn` is an active DBI connection.
#' - Run `build_catchment_fire_regime_covariates(conn = pg)` to read all four
#'   sources and recreate `covariates.catchment_fire_regime`.
#'
#' @param conn DBI connection.
#'
#' @return Invisibly returns the written tibble.
build_catchment_fire_regime_covariates <- function(conn) {
  if (missing(conn) || is.null(conn)) {
    rlang::abort("A valid DBI connection must be supplied via `conn`.")
  }

  metric_readers <- list(
    read_fire_risk_covariate,
    read_fire_regime_group_covariate,
    read_fire_return_interval_covariate,
    read_vegetation_departure_covariate
  )

  fire_regime_covariates <- metric_readers |>
    purrr::map(\(reader) reader()) |>
    purrr::reduce(
      .f = dplyr::full_join,
      by = "usgs_site"
    )

  fire_regime_covariates <- fire_regime_covariates[, c("usgs_site", "risk_mean", "frg_mean", "fri_mean", "vd_mean")]
  fire_regime_covariates <- fire_regime_covariates[order(fire_regime_covariates$usgs_site), , drop = FALSE]

  if (!all(c("usgs_site", "risk_mean", "frg_mean", "fri_mean", "vd_mean") %in% names(fire_regime_covariates))) {
    rlang::abort("Consolidated covariate table is missing one or more required columns.")
  }

  if (any(is.na(fire_regime_covariates$usgs_site) | fire_regime_covariates$usgs_site == "")) {
    rlang::abort("Consolidated covariate table contains missing or empty usgs_site values.")
  }

  if (anyDuplicated(fire_regime_covariates$usgs_site) > 0) {
    rlang::abort("Consolidated covariate table contains duplicate usgs_site values after merge.")
  }

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS covariates")

  DBI::dbWriteTable(
    conn = conn,
    name = c("covariates", "catchment_fire_regime"),
    value = fire_regime_covariates,
    overwrite = TRUE,
    row.names = FALSE
  )

  DBI::dbExecute(
    conn,
    "CREATE UNIQUE INDEX IF NOT EXISTS catchment_fire_regime_usgs_site_uq_idx ON covariates.catchment_fire_regime (usgs_site)"
  )

  DBI::dbExecute(
    conn,
    "COMMENT ON TABLE covariates.catchment_fire_regime IS 'Catchment-level fire regime covariates consolidated from risk, FRG, FRI, and vegetation departure products; intended for future analyte export joins by usgs_site'"
  )
  DBI::dbExecute(
    conn,
    "COMMENT ON COLUMN covariates.catchment_fire_regime.usgs_site IS 'Source-provided USGS site identifier used as the join key for covariate exports'"
  )
  DBI::dbExecute(
    conn,
    "COMMENT ON COLUMN covariates.catchment_fire_regime.risk_mean IS 'Catchment mean fire risk metric from risk_crass-catchments.csv (units source-defined)'"
  )
  DBI::dbExecute(
    conn,
    "COMMENT ON COLUMN covariates.catchment_fire_regime.frg_mean IS 'Catchment mean fire regime group metric from frg_crass-catchments.csv (units source-defined)'"
  )
  DBI::dbExecute(
    conn,
    "COMMENT ON COLUMN covariates.catchment_fire_regime.fri_mean IS 'Catchment mean fire return interval metric from fri_crass-catchments.csv (units source-defined)'"
  )
  DBI::dbExecute(
    conn,
    "COMMENT ON COLUMN covariates.catchment_fire_regime.vd_mean IS 'Catchment mean vegetation departure metric from vd_crass-catchments.csv (units source-defined)'"
  )

  invisible(fire_regime_covariates)
}


read_catchment_metric_covariate <- function(file_path, metric_name) {
  covariate_raw <- readr::read_csv(
    file = file_path,
    show_col_types = FALSE
  )

  required_cols <- c("usgs_site", "mean")
  if (!all(required_cols %in% names(covariate_raw))) {
    rlang::abort(
      paste0(
        "Input file is missing required columns (usgs_site, mean): ",
        file_path
      )
    )
  }

  covariate_clean <- covariate_raw |>
    dplyr::select("usgs_site", "mean")

  covariate_clean$usgs_site <- as.character(covariate_clean$usgs_site)
  covariate_clean$mean <- as.numeric(covariate_clean$mean)

  names(covariate_clean)[names(covariate_clean) == "mean"] <- metric_name
  covariate_clean <- covariate_clean[order(covariate_clean$usgs_site), , drop = FALSE]

  if (any(is.na(covariate_clean$usgs_site) | covariate_clean$usgs_site == "")) {
    rlang::abort(
      paste0(
        "Input file contains missing or empty usgs_site values: ",
        file_path
      )
    )
  }

  dup_sites <- unique(covariate_clean$usgs_site[duplicated(covariate_clean$usgs_site)])

  if (length(dup_sites) > 0) {
    rlang::abort(
      paste0(
        "Input file contains duplicate usgs_site values: ",
        file_path,
        ". Example duplicate site: ",
        dup_sites[[1]]
      )
    )
  }

  covariate_clean
}