# Purpose & Goals
# - Fetch daily discharge data from USGS Water Data (NWIS) for a set of sites
#   and store them in the local DuckDB database for downstream hydrologic
#   analyses (e.g., DFFT metrics and fire-related assessments).
# - Robustly handle rate limits and transient API errors via pacing + backoff.
# - Ensure idempotent writes using a natural-key dedup strategy.
# - Produce lightweight operational logs of each fetch attempt.
#
# Inputs
# - usgs_sites (vector): Monitoring locations in the format "USGS-{site_no}".
#   These are sourced from Postgres queries selecting relevant fire-affected sites.
# - duckdb_path (string): Path to the local DuckDB database file.
# - Read configuration: `pause_seconds`, `backoff_min`, `backoff_cap`,
#   `progress_every`, `chunk_size` control pacing, retries, and progress logging.
#
# Data Source & Scope
# - Service: USGS Water Data daily values (NWIS DV).
# - Parameter: Discharge `parameter_code = "00060"`.
# - Statistic: Mean daily `statistic_id = "00003"`.
# - Time window: Up to a fixed end date (currently "2025-12-31") with flexible
#   start (API defaults to earliest available when blank).
#
# Processing Steps
# 1. Site discovery: Build `site_vec` from Postgres queries; filter valid IDs.
# 2. Chunking: Split the site list into manageable chunks (`chunk_size`).
# 3. Fetching: For each site, call `dataRetrieval::read_waterdata_daily()` with
#    pacing (`pause_seconds`) and backoff (`backoff_min` → `backoff_cap`) to
#    mitigate rate limits. Capture warnings/messages.
# 4. Normalization: Append `usgs_site` and `retrieved_at` to the result.
# 5. Dedup key: Anti-join against DuckDB using composite key
#    (`usgs_site`, `time`, `parameter_code`, `statistic_id`) to keep only new rows.
# 6. Write: Append new rows into `usgs_discharge_daily`.
# 7. Index: Ensure a unique index exists on the composite key to guard against
#    accidental duplicates (`CREATE UNIQUE INDEX IF NOT EXISTS`).
# 8. Logging: Write an entry per attempt into `usgs_discharge_daily_log` with
#    status, duration, counts, warnings, and messages.
# 9. Progress: Emit progress messages every `progress_every` sites.
# 10. Retry pass: Optionally retry only the sites that did not appear in DuckDB
#    after the first pass (based on `usgs_success_sites`).
#
# Output & Artifacts
# - Primary table: `usgs_discharge_daily` in DuckDB contains daily discharge
#   records per site with the required columns from the NWIS DV service.
#   Key columns used for dedup: `usgs_site`, `time`, `parameter_code`, `statistic_id`.
# - Log table: `usgs_discharge_daily_log` records operational metadata per site
#   fetch (status: success_data/success_empty/error, rows written, timings, etc.).
# - Unique index: `idx_usgs_discharge_daily_unique` enforces the composite key.
#
# Error Handling & Rate Limiting
# - Uses `purrr::slowly` for pacing and `purrr::insistently` with backoff to retry
#   transient failures exactly once (max_times = 2, jitter enabled).
# - Catches and logs errors without halting the entire run; records empty
#   responses distinctly.
#
# Data Quality & Consistency Notes
# - `usgs_site` is preserved as text in the `USGS-{site_no}` format to maintain
#   leading zeros and consistency with existing datasets.
# - Only `parameter_code = "00060"` and `statistic_id = "00003"` are requested
#   by this workflow to align with discharge mean daily values.
# - Dedup strategy is conservative: it prevents inserting duplicate rows for the
#   same site/date/parameter/statistic even across multiple runs.
#
# Fallback: Manual Import
# - When API rate limits or outages prevent fetching, use the manual import SQL
#   script `usgs_manual_import.sql` to load tab-delimited NWIS DV files. That
#   script ingests the minimal column set and performs the same composite-key
#   dedup on insert.
#
# Usage
# - Configure `duckdb_path` and obtain `usgs_sites` as shown below.
# - Invoke `run_usgs_to_duckdb(sites = site_vec, duckdb_path = duckdb_path, ...)`.
# - Review `usgs_discharge_daily_log` for operational status and counts.
# - For retries, compute the set difference of desired sites against those
#   present in `usgs_discharge_daily` and pass to `run_usgs_to_duckdb`.
# -----------------------------------------------------------------------------

# Configuration
duckdb_path      <- "~/Desktop/wildfire_discharge.duckdb"
pause_seconds    <- 60
backoff_min      <- 30
backoff_cap      <- 120
progress_every   <- 5
chunk_size       <- 50

# build sites list
sites_query <- "
WITH sites AS (
  SELECT usgs_site FROM firearea.largest_ammonium_valid_fire_per_site
  UNION
  SELECT usgs_site FROM firearea.largest_nitrate_valid_fire_per_site
  UNION
  SELECT usgs_site FROM firearea.largest_orthop_valid_fire_per_site
  UNION
  SELECT usgs_site FROM firearea.largest_spcond_valid_fire_per_site
  )
SELECT DISTINCT usgs_site
FROM sites
WHERE usgs_site ~~ 'USGS-%' ;
"

usgs_sites <- dbGetQuery(
  conn      = pg,
  statement = sites_query
)

# Extract site vector (ensure character) and basic validation
site_vec <- unique(as.character(usgs_sites$usgs_site))
site_vec <- site_vec[!is.na(site_vec) & nzchar(site_vec)]

# Helper: split a vector into chunks of size n
split_chunks <- function(x, n) {
  if (length(x) == 0) return(list())
  idx <- (seq_along(x) - 1L) %/% n + 1L
  split(x, idx)
}

# Helper: ensure unique index exists on data table
ensure_unique_index <- function(conn) {
  # Create unique index after table exists; IF NOT EXISTS makes this idempotent
  sql <- paste0(
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_usgs_discharge_daily_unique ",
    "ON usgs_discharge_daily (usgs_site, time, parameter_code, statistic_id);"
  )
  try(DBI::dbExecute(conn, sql), silent = TRUE)
}

# Helper: write a single log row to DuckDB
write_log_row <- function(conn, log_row_df) {
  DBI::dbWriteTable(
    conn      = conn,
    name      = "usgs_discharge_daily_log",
    value     = log_row_df,
    append    = TRUE
  )
}

# Wrapped fetcher with quietly + safely + one backoff retry + pacing ≥ 10s
fetch_daily_safe_paced <- (function() {
  base_fetch <- function(site) {
    dataRetrieval::read_waterdata_daily(
      monitoring_location_id = site,
      parameter_code         = "00060",
      statistic_id           = "00003",
      skipGeometry           = TRUE,
      time                   = c("", "2025-12-31")
    )
  }

  quiet_fetch <- purrr::quietly(base_fetch)
  safe_quiet  <- purrr::safely(quiet_fetch, otherwise = NULL)
  retry_once  <- purrr::insistently(
    f    = safe_quiet,
    rate = purrr::rate_backoff(
      pause_min = backoff_min,
      pause_cap = backoff_cap,
      jitter    = TRUE,
      max_times = 2
    )
  )
  purrr::slowly(retry_once, rate = purrr::rate_delay(pause = pause_seconds))
})()

# Main execution block
run_usgs_to_duckdb <- function(
  sites,
  duckdb_path,
  progress_every = 5L,
  chunk_size = 50L
) {
  if (length(sites) == 0) {
    base::message("No USGS sites to process.")
    return(invisible(NULL))
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path.expand(duckdb_path))
  on.exit(
    {
      try(DBI::dbDisconnect(conn, shutdown = TRUE), silent = TRUE)
    },
    add = TRUE
  )

  chunks <- split_chunks(sites, as.integer(chunk_size))

  total_attempts <- 0L
  total_success  <- 0L
  total_empty    <- 0L
  total_errors   <- 0L
  total_rows     <- 0L

  purrr::iwalk(
    .x = chunks,
    .f = function(chunk_sites, chunk_idx) {
      base::message(sprintf(
        "Processing chunk %s with %s sites…",
        chunk_idx,
        length(chunk_sites)
      ))

      purrr::walk2(
        .x = chunk_sites,
        .y = seq_along(chunk_sites),
        .f = function(site, i) {
          total_attempts <<- total_attempts + 1L
          start_time <- base::Sys.time()

          res <- fetch_daily_safe_paced(site)
          end_time <- base::Sys.time()
          duration_s <- as.numeric(base::difftime(
            end_time,
            start_time,
            units = "secs"
          ))

          # Unpack safely+quietly structure
          err_obj   <- res$error
          out_df    <- NULL
          warns_chr <- character(0)
          msgs_chr  <- character(0)
          if (is.null(err_obj) && !is.null(res$result)) {
            out_df    <- res$result$result
            warns_chr <- res$result$warnings
            msgs_chr  <- res$result$messages
          }

          to_log <- NULL

          if (!is.null(err_obj)) {
            total_errors <<- total_errors + 1L
            to_log <- data.frame(
              site             = site,
              start_time       = start_time,
              end_time         = end_time,
              duration_secs    = duration_s,
              status           = "error",
              rows_fetched     = 0L,
              error            = paste0(capture.output(print(err_obj)), collapse = "\n"),
              warnings         = paste(warns_chr, collapse = " | "),
              messages         = paste(msgs_chr, collapse = " | "),
              stringsAsFactors = FALSE
            )
            write_log_row(conn, to_log)
            return(invisible(NULL))
          }

          if (
            is.null(out_df) || (is.data.frame(out_df) && nrow(out_df) == 0L)
          ) {
            total_empty <<- total_empty + 1L
            to_log <- data.frame(
              site             = site,
              start_time       = start_time,
              end_time         = end_time,
              duration_secs    = duration_s,
              status           = "success_empty",
              rows_fetched     = 0L,
              error            = NA_character_,
              warnings         = paste(warns_chr, collapse = " | "),
              messages         = paste(msgs_chr, collapse = " | "),
              stringsAsFactors = FALSE
            )
            write_log_row(conn, to_log)
            return(invisible(NULL))
          }

          # Normalize output: add usgs_site and retrieval timestamp
          out_df$usgs_site    <- site
          out_df$retrieved_at <- end_time

          # Pre-deduplicate by anti-joining existing keys for this site
          new_rows <- out_df
          if (DBI::dbExistsTable(conn, "usgs_discharge_daily")) {
            site_q <- as.character(DBI::dbQuoteString(conn, site))
            existing_keys <- try(
              DBI::dbGetQuery(
                conn,
                paste0(
                  "SELECT usgs_site, time, parameter_code, statistic_id ",
                  "FROM usgs_discharge_daily WHERE usgs_site = ",
                  site_q
                )
              ),
              silent = TRUE
            )
            if (
              !inherits(existing_keys, "try-error") &&
                is.data.frame(existing_keys)
            ) {
              # Keep only rows not already present per composite key
              if (
                all(
                  c("usgs_site", "time", "parameter_code", "statistic_id") %in%
                    names(new_rows)
                )
              ) {
                new_rows <- dplyr::anti_join(
                  x = new_rows,
                  y = existing_keys,
                  by = c("usgs_site", "time", "parameter_code", "statistic_id")
                )
              }
            }
          }

          written_n <- 0L
          if (is.data.frame(new_rows) && nrow(new_rows) > 0L) {
            DBI::dbWriteTable(
              conn = conn,
              name = "usgs_discharge_daily",
              value = new_rows,
              append = TRUE
            )
            # Create unique index after first write
            ensure_unique_index(conn)
            written_n <- nrow(new_rows)
            total_rows <<- total_rows + written_n
            total_success <<- total_success + 1L
          } else {
            total_empty <<- total_empty + 1L
          }

          # Log success (with data or dedup to zero after filter)
          to_log <- data.frame(
            site             = site,
            start_time       = start_time,
            end_time         = end_time,
            duration_secs    = duration_s,
            status           = ifelse(written_n > 0L, "success_data", "success_empty"),
            rows_fetched     = written_n,
            error            = NA_character_,
            warnings         = paste(warns_chr, collapse = " | "),
            messages         = paste(msgs_chr, collapse = " | "),
            stringsAsFactors = FALSE
          )
          write_log_row(conn, to_log)

          if ((i %% progress_every) == 0L) {
            base::message(sprintf(
              "Processed %s/%s sites in chunk %s (rows written so far: %s)",
              i,
              length(chunk_sites),
              chunk_idx,
              total_rows
            ))
          }
        }
      )
    }
  )

  base::message(sprintf(
    "Done. Attempts: %s | Success(data): %s | Success(empty): %s | Errors: %s | Rows written: %s",
    total_attempts,
    total_success,
    total_empty,
    total_errors,
    total_rows
  ))

  invisible(list(
    attempts = total_attempts,
    success  = total_success,
    empty    = total_empty,
    errors   = total_errors,
    rows     = total_rows
  ))
}

# Execute the fetch → DuckDB flow
run_usgs_to_duckdb(
  sites          = site_vec,
  duckdb_path    = duckdb_path,
  progress_every = progress_every,
  chunk_size     = chunk_size
)

# If you are missing sites due to rate limits or errors, retry failed sites.
# This is a clunky, manual approach for now but should only need to be run once;
# consider functionalizing if this becomes a common need.

wildfire_duck_db <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = path.expand(duckdb_path)
)

usgs_success_sites <- DBI::dbGetQuery(
  conn      = wildfire_duck_db,
  statement = "SELECT DISTINCT usgs_site FROM usgs_discharge_daily ;"
)

DBI::dbDisconnect(conn = wildfire_duck_db, shutdown = TRUE)

usgs_fail_sites <- usgs_sites |>
  dplyr::filter(!usgs_site %in% usgs_success_sites$usgs_site)
  
site_vec_fails <- unique(as.character(usgs_fail_sites$usgs_site))
site_vec_fails <- site_vec_fails[!is.na(site_vec_fails) & nzchar(site_vec_fails)]

run_usgs_to_duckdb(
  sites          = site_vec_fails,
  duckdb_path    = duckdb_path,
  progress_every = progress_every,
  chunk_size     = chunk_size
)
