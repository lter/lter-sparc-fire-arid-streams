#' Merge Additional Sites Into Aggregated RDS
#'
#' @description
#' Integrates three supplemental source files into the baseline aggregated
#' discharge dataset by standardizing each source to a reduced schema
#' (`usgs_site`, `time`, `value`), validating, deduplicating by
#' `usgs_site` + `time`, and writing a new dated RDS output.
#'
#' Sources:
#'
#' 1. **sbc.csv** — Santa Barbara Coastal LTER discharge.
#'    Mapping: `usgs_site` -> `usgs_site`, `Date` -> `time`, `Flow` -> `value`.
#'    Site IDs (e.g. `sbc_lter_bel`) are kept unchanged.
#'
#' 2. **usgs_09406000.tab** — USGS NWIS DV tab-delimited file for site
#'    09406000 (Virgin River at Virgin, UT). Comment lines start with `#`;
#'    a type-token row immediately follows the header and must be skipped.
#'    Discharge column identified by regex `00060_00003` (mean daily).
#'    `usgs_site` is set to `USGS-09406000`.
#'
#' 3. **usgs_09510200.csv** — USGS daily discharge CSV for site 09510200
#'    (Sycamore Creek). Columns `monitoring_location_id`, `time`, `value`
#'    are mapped to `usgs_site`, `time`, `value`.
#'
#' @details
#' Deduplication key: `usgs_site` + `time`.
#' Precedence: baseline rows are kept; only new keys from supplemental
#' sources are appended.
#' Final output schema: exactly 3 columns — `usgs_site` (character),
#' `time` (Date), `value` (numeric).
#'
#' @seealso
#' [usgs_latest_discharge_to_rds.R] — style and workflow reference.
#' [duckdb_workflow/usgs_manual_import.sql] — mapping reference for 09406000.
#' [duckdb_workflow/usgs_manual_import_09510200.sql] — mapping reference for
#'   09510200.
#' [duckdb_workflow/usgs_import_sbc_syca.sql] — mapping reference for SBC.
NULL

# ── Configuration ────────────────────────────────────────────────────────────

BASELINE_RDS_PATH <- "/home/srearl/Desktop/rds/usgs_20260427.rds"
OUTPUT_DIR        <- "/home/srearl/Desktop/rds"
DATA_DIR          <- fs::path(
  "/home/srearl/localRepos/lter-sparc-fire-arid-streams",
  "hydrologic_variability", "data"
)

# ── Shared validator ─────────────────────────────────────────────────────────

#' Validate Standardized Discharge Data Frame
#'
#' @description
#' Enforces column names, types, and drops rows missing any of the three
#' required fields.
#'
#' @param df  A data.frame with columns `usgs_site`, `time`, `value`.
#' @param source_label  Character label for diagnostic messages.
#' @return A tibble with exactly columns `usgs_site` (character), `time`
#'   (Date), `value` (numeric), with incomplete rows removed.
validate_discharge <- function(df, source_label) {
  required <- c("usgs_site", "time", "value")
  missing_cols <- setdiff(required, names(df))
  if (length(missing_cols) > 0) {
    stop(sprintf(
      "[%s] Missing required columns: %s",
      source_label, paste(missing_cols, collapse = ", ")
    ))
  }

  out <- tibble::tibble(
    usgs_site = as.character(df$usgs_site),
    time      = as.Date(df$time),
    value     = as.numeric(df$value)
  )

  n_before <- nrow(out)
  out <- out[
    !is.na(out$usgs_site) & nzchar(out$usgs_site) &
    !is.na(out$time) &
    !is.na(out$value),
  ]
  n_dropped <- n_before - nrow(out)

  if (n_dropped > 0) {
    message(sprintf(
      "[%s] Dropped %d rows with missing usgs_site/time/value.",
      source_label, n_dropped
    ))
  }

  out
}

#' Print QA Summary for a Parsed Source
#'
#' @param df  Validated tibble (`usgs_site`, `time`, `value`).
#' @param label  Character label.
qa_summary <- function(df, label) {
  n <- nrow(df)
  dup_keys <- sum(duplicated(df[, c("usgs_site", "time")]))
  date_range <- if (n > 0) {
    sprintf("%s to %s", min(df$time), max(df$time))
  } else {
    "N/A"
  }
  message(sprintf(
    "[%s] rows: %d | duplicates (usgs_site+time): %d | date range: %s",
    label, n, dup_keys, date_range
  ))
}


# ── Parser A: sbc.csv ───────────────────────────────────────────────────────

#' Parse sbc.csv to Standard Schema
#'
#' @description
#' Reads the Santa Barbara Coastal LTER discharge CSV. Mapping:
#' `usgs_site` -> `usgs_site`, `Date` -> `time`, `Flow` -> `value`.
#' SBC site labels (e.g. `sbc_lter_bel`) are kept unchanged.
#'
#' @param path  Path to sbc.csv.
#' @return Validated tibble with `usgs_site`, `time`, `value`.
parse_sbc <- function(path = fs::path(DATA_DIR, "sbc.csv")) {
  raw <- readr::read_csv(path, show_col_types = FALSE)
  df <- tibble::tibble(
    usgs_site = raw$usgs_site,
    time      = as.Date(raw$Date),
    value     = as.numeric(raw$Flow)
  )
  validate_discharge(df, "sbc.csv")
}


# ── Parser B: usgs_09406000.tab ─────────────────────────────────────────────

#' Parse usgs_09406000.tab to Standard Schema
#'
#' @description
#' Reads the USGS NWIS DV tab-delimited file for site 09406000. Skips
#' comment lines (start with `#`) and the type-token row that immediately
#' follows the header. The discharge column is identified by a regex match
#' for `00060_00003` (mean daily discharge). `usgs_site` is set to
#' `USGS-09406000`.
#'
#' @param path  Path to usgs_09406000.tab.
#' @return Validated tibble with `usgs_site`, `time`, `value`.
parse_09406000 <- function(path = fs::path(DATA_DIR, "usgs_09406000.tab")) {
  lines <- readLines(path)

  # Drop comment lines
  comment_idx <- grep("^#", lines)
  lines <- lines[-comment_idx]

  # First remaining line is header; second is the type-token row (e.g. "5s\t15s\t...")
  header_line    <- lines[1]
  data_lines     <- lines[3:length(lines)]  # skip header + type-token

  col_names <- strsplit(header_line, "\t")[[1]]

  # Identify the discharge value column via regex

  value_col <- grep("00060_00003$", col_names, value = TRUE)
  if (length(value_col) == 0) {
    stop("[usgs_09406000.tab] Cannot find discharge column matching '00060_00003'.")
  }
  value_col <- value_col[1]

  raw <- readr::read_tsv(
    I(c(header_line, data_lines)),
    col_types  = readr::cols(.default = "c"),
    show_col_types = FALSE
  )

  df <- tibble::tibble(
    usgs_site = "USGS-09406000",
    time      = as.Date(raw$datetime),
    value     = as.numeric(raw[[value_col]])
  )
  validate_discharge(df, "usgs_09406000.tab")
}


# ── Parser C: usgs_09510200.csv ─────────────────────────────────────────────

#' Parse usgs_09510200.csv to Standard Schema
#'
#' @description
#' Reads the USGS daily discharge CSV for site 09510200 (Sycamore Creek).
#' Mapping: `monitoring_location_id` -> `usgs_site`, `time` -> `time`,
#' `value` -> `value`.
#'
#' @param path  Path to usgs_09510200.csv.
#' @return Validated tibble with `usgs_site`, `time`, `value`.
parse_09510200 <- function(path = fs::path(DATA_DIR, "usgs_09510200.csv")) {
  raw <- readr::read_csv(path, show_col_types = FALSE)
  df <- tibble::tibble(
    usgs_site = as.character(raw$monitoring_location_id),
    time      = as.Date(raw$time),
    value     = as.numeric(raw$value)
  )
  validate_discharge(df, "usgs_09510200.csv")
}


# ── Merge workflow ───────────────────────────────────────────────────────────

#' Run the Full Merge Workflow
#'
#' @description
#' 1. Parse each supplemental source.
#' 2. Read baseline RDS and project to `usgs_site`/`time`/`value`.
#' 3. Append new rows; deduplicate by `usgs_site` + `time` (baseline wins).
#' 4. Print diagnostics and write a new dated RDS.
#'
#' @param baseline_rds_path  Path to the baseline aggregated RDS.
#' @param output_dir  Directory for the output RDS.
#' @return Invisible path to the written RDS file.
run_merge <- function(
  baseline_rds_path = BASELINE_RDS_PATH,
  output_dir        = OUTPUT_DIR
) {

  # ── Phase 2: Parse sources ──────────────────────────────────────────────
  message("── Parsing supplemental sources ──")
  sbc     <- parse_sbc()
  tab406  <- parse_09406000()
  csv510  <- parse_09510200()

  # ── Phase 3: QA summaries ───────────────────────────────────────────────
  message("\n── QA summaries (per source) ──")
  qa_summary(sbc,    "sbc.csv")
  qa_summary(tab406, "usgs_09406000.tab")
  qa_summary(csv510, "usgs_09510200.csv")

  # Deduplicate within each source before merging
  sbc     <- sbc[!duplicated(sbc[, c("usgs_site", "time")]), ]
  tab406  <- tab406[!duplicated(tab406[, c("usgs_site", "time")]), ]
  csv510  <- csv510[!duplicated(csv510[, c("usgs_site", "time")]), ]

  new_rows <- rbind(sbc, tab406, csv510)

  # Deduplicate across supplemental sources (shouldn't overlap but be safe)
  new_rows <- new_rows[!duplicated(new_rows[, c("usgs_site", "time")]), ]

  message(sprintf(
    "\n── Supplemental rows after dedup: %d ──", nrow(new_rows)
  ))

  # ── Phase 4: Read baseline and project ──────────────────────────────────
  message("\n── Reading baseline RDS ──")
  baseline <- readRDS(baseline_rds_path)
  baseline_proj <- tibble::tibble(
    usgs_site = as.character(baseline$usgs_site),
    time      = as.Date(baseline$time),
    value     = as.numeric(baseline$value)
  )

  # Drop baseline rows missing key fields
  baseline_proj <- baseline_proj[
    !is.na(baseline_proj$usgs_site) & nzchar(baseline_proj$usgs_site) &
    !is.na(baseline_proj$time),
  ]

  # Deduplicate baseline on the reduced key
  baseline_proj <- baseline_proj[
    !duplicated(baseline_proj[, c("usgs_site", "time")]),
  ]

  message(sprintf(
    "Baseline projected rows: %d", nrow(baseline_proj)
  ))

  # ── Anti-join: keep only new keys ───────────────────────────────────────
  new_rows$key <- paste(new_rows$usgs_site, new_rows$time, sep = "|")
  baseline_proj$key <- paste(baseline_proj$usgs_site, baseline_proj$time, sep = "|")

  truly_new <- new_rows[!new_rows$key %in% baseline_proj$key, ]
  truly_new$key <- NULL
  baseline_proj$key <- NULL

  dup_drops <- nrow(new_rows) - nrow(truly_new)

  message(sprintf(
    "New rows to add: %d | Duplicate drops (already in baseline): %d",
    nrow(truly_new), dup_drops
  ))

  # ── Combine ─────────────────────────────────────────────────────────────
  merged <- rbind(baseline_proj, truly_new)

  message(sprintf(
    "Final merged rows: %d (baseline %d + added %d)",
    nrow(merged), nrow(baseline_proj), nrow(truly_new)
  ))

  # Sanity: zero duplicate keys
  dup_check <- sum(duplicated(merged[, c("usgs_site", "time")]))
  if (dup_check > 0) {
    warning(sprintf("Unexpected duplicate keys in merged data: %d", dup_check))
  } else {
    message("Duplicate key check: PASS (0 duplicates)")
  }

  # ── Merge diagnostics ──────────────────────────────────────────────────
  message("\n── Date ranges by site (new rows only) ──")
  if (nrow(truly_new) > 0) {
    site_summary <- aggregate(
      time ~ usgs_site, data = truly_new,
      FUN = function(x) sprintf("%s to %s (%d rows)", min(x), max(x), length(x))
    )
    for (i in seq_len(nrow(site_summary))) {
      message(sprintf("  %s: %s", site_summary$usgs_site[i], site_summary$time[i]))
    }
  }

  # ── Phase 5: Write output ──────────────────────────────────────────────
  date_tag <- format(Sys.Date(), "%Y%m%d")
  out_path <- fs::path(output_dir, sprintf("usgs_merged_%s.rds", date_tag))

  if (file.exists(out_path)) {
    stop(sprintf("Output file already exists — will not overwrite: %s", out_path))
  }

  saveRDS(merged, out_path)
  message(sprintf("\nWrote: %s (%d rows, 3 columns)", out_path, nrow(merged)))

  invisible(out_path)
}

# ── Spot-check helper ────────────────────────────────────────────────────────

#' Spot-Check Merged Output
#'
#' @description
#' Loads the merged RDS and prints summary statistics for selected sites.
#'
#' @param rds_path  Path to the merged RDS.
#' @param sites  Character vector of site IDs to check.
spot_check <- function(
  rds_path,
  sites = c("sbc_lter_bel", "USGS-09406000", "USGS-09510200")
) {
  d <- readRDS(rds_path)
  stopifnot(identical(names(d), c("usgs_site", "time", "value")))
  stopifnot(ncol(d) == 3)
  message(sprintf("Loaded %s: %d rows, %d columns", rds_path, nrow(d), ncol(d)))

  for (s in sites) {
    sub <- d[d$usgs_site == s, ]
    if (nrow(sub) == 0) {
      message(sprintf("  %s: NOT FOUND", s))
    } else {
      message(sprintf(
        "  %s: %d rows, %s to %s, value range [%.4f, %.4f]",
        s, nrow(sub), min(sub$time), max(sub$time),
        min(sub$value, na.rm = TRUE), max(sub$value, na.rm = TRUE)
      ))
    }
  }
}
