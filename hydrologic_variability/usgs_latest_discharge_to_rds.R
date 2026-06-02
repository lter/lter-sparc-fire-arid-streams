#' USGS Daily Discharge Fetch to RDS
#'
#' @description
#' Reconstructs the USGS daily discharge ingestion workflow so fetched data are
#' written to RDS (instead of DuckDB tables). Site discovery is read from a
#' local repository resource at `data/usgs_latest_discharge_sites.csv`.
#'
#' A helper is provided to regenerate that resource from Postgres using the same
#' site query logic as the original DuckDB pipeline.
#'
#' The workflow uses rate-limit-aware pacing and retry backoff, writes an
#' operation log to CSV, and writes duplicate-key diagnostics to a separate CSV.
#'
#' Duplicate keys are identified (not dropped) using the key:
#' `usgs_site`, `time`, `parameter_code`, `statistic_id`.
#'
#' @details
#' Query parameters match the existing workflow:
#' - `parameter_code = "00060"` (discharge)
#' - `statistic_id = "00003"` (mean daily)
#' - `time = c("", "2025-12-31")`
#'
#' Rate-limit settings are intentionally slower than the prior script:
#' - `pause_seconds = 180`
#' - `backoff_min = 90`
#' - `backoff_cap = 360`
#'
#' @seealso
#' [run_usgs_to_rds()], [combine_site_rds_to_single()]
NULL

#' Default Local Site Resource Path
#'
#' @description
#' Repository-local site list used by the USGS fetch workflow.
USGS_SITES_RESOURCE_PATH <- fs::path("data", "usgs_latest_discharge_sites.csv")

#' Default Output Directory
#'
#' @description
#' Base directory for workflow output artifacts.
USGS_OUTPUT_DIR <- "~/Desktop/parker/"

#' Default Output Paths
#'
#' @description
#' Canonical output artifact paths. Override these constants at the top of this
#' file to change output locations globally.
USGS_OUTPUT_RDS_PATH     <- fs::path(USGS_OUTPUT_DIR, "usgs_discharge_daily.rds")
USGS_LOG_CSV_PATH        <- fs::path(USGS_OUTPUT_DIR, "usgs_discharge_daily_log.csv")
USGS_DUPLICATES_CSV_PATH <- fs::path(USGS_OUTPUT_DIR, "usgs_discharge_daily_duplicates.csv")
USGS_CHECKPOINT_CSV_PATH <- fs::path(USGS_OUTPUT_DIR, "usgs_discharge_daily_checkpoint.csv")
USGS_PER_SITE_DIR        <- fs::path(USGS_OUTPUT_DIR, "usgs_discharge_daily_by_site")

#' Default Rate-Limit Settings
#'
#' @description
#' API pacing and retry backoff values. Override these constants at the top of
#' this file to tune request behavior globally.
USGS_PAUSE_SECONDS       <- 180
USGS_BACKOFF_MIN_SECONDS <- 90
USGS_BACKOFF_CAP_SECONDS <- 360

#' Return USGS Site IDs From Local Resource
#'
#' @description
#' Reads distinct USGS monitoring location IDs in `USGS-<site_no>` format from
#' a repository data resource (CSV).
#'
#' Function name is retained for backward compatibility with prior workflow
#' naming, but this function now reads from file and does not connect to
#' Postgres.
#'
#' @param resource_path Path to the site resource CSV.
#' @return A character vector of unique, non-empty `USGS-*` site IDs.
get_usgs_sites_from_pg <- function(resource_path = USGS_SITES_RESOURCE_PATH) {
  resource_path <- base::path.expand(resource_path)

  if (!base::file.exists(resource_path)) {
    base::stop(base::sprintf(
      "Site resource file does not exist: %s",
      resource_path
    ))
  }

  usgs_sites <- readr::read_csv(resource_path, show_col_types = FALSE)

  if (!("usgs_site" %in% base::names(usgs_sites))) {
    base::stop(base::sprintf(
      "Site resource must contain column 'usgs_site': %s",
      resource_path
    ))
  }

  site_vec <- base::unique(base::as.character(usgs_sites$usgs_site))
  site_vec <- site_vec[!base::is.na(site_vec) & base::nzchar(site_vec)]
  site_vec
}

#' Rebuild Local Site Resource From Postgres
#'
#' @description
#' Queries Postgres for the cross-analyte USGS site union and writes the result
#' to `data/usgs_latest_discharge_sites.csv`.
#'
#' Use this helper only when refreshing the local site resource; the main fetch
#' workflow itself does not require Postgres.
#'
#' @param pg A live DBI Postgres connection object.
#' @param resource_path Path to the output site resource CSV.
#'
#' @return Invisibly returns the written site table.
write_usgs_latest_discharge_sites_resource <- function(
  pg,
  resource_path = USGS_SITES_RESOURCE_PATH
) {
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

  usgs_sites <- DBI::dbGetQuery(
    conn = pg,
    statement = sites_query
  )

  usgs_sites$usgs_site <- base::as.character(usgs_sites$usgs_site)
  usgs_sites <- usgs_sites[
    !base::is.na(usgs_sites$usgs_site) &
      base::nzchar(usgs_sites$usgs_site) &
      stringr::str_detect(usgs_sites$usgs_site, "^USGS-"),
    "usgs_site",
    drop = FALSE
  ]
  usgs_sites <- usgs_sites[!base::duplicated(usgs_sites$usgs_site), , drop = FALSE]
  usgs_sites <- usgs_sites[base::order(usgs_sites$usgs_site), , drop = FALSE]

  resource_path <- base::path.expand(resource_path)
  fs::dir_create(path = fs::path_dir(resource_path), recurse = TRUE)
  readr::write_csv(usgs_sites, file = resource_path)

  base::message(base::sprintf(
    "Wrote %s sites to %s",
    base::nrow(usgs_sites),
    resource_path
  ))

  invisible(usgs_sites)
}

#' Split Vector Into Equal-Sized Chunks
#'
#' @param x A vector to split.
#' @param n Chunk size.
#'
#' @return A list of vector chunks.
split_chunks <- function(x, n) {
  if (base::length(x) == 0L) {
    return(base::list())
  }

  idx <- (base::seq_along(x) - 1L) %/% n + 1L
  base::split(x, idx)
}

#' Build a Rate-Limited Safe USGS Fetcher
#'
#' @description
#' Creates a fetch function with quiet capture, safe error handling, retry
#' backoff, and fixed inter-call delay.
#'
#' @param pause_seconds Delay between API calls in seconds.
#' @param backoff_min Minimum retry backoff in seconds.
#' @param backoff_cap Maximum retry backoff in seconds.
#' @param end_date End date for NWIS DV query in `YYYY-MM-DD` format.
#'
#' @return A function that accepts one `site` string and returns a
#'   safely/quietly wrapped result object.
make_fetch_daily_safe_paced <- function(
  pause_seconds = USGS_PAUSE_SECONDS,
  backoff_min = USGS_BACKOFF_MIN_SECONDS,
  backoff_cap = USGS_BACKOFF_CAP_SECONDS,
  end_date = "2025-12-31"
) {
  base_fetch <- function(site) {
    dataRetrieval::read_waterdata_daily(
      monitoring_location_id = site,
      parameter_code = "00060",
      statistic_id = "00003",
      skipGeometry = TRUE,
      time = base::c("", end_date)
    )
  }

  quiet_fetch <- purrr::quietly(base_fetch)
  safe_quiet <- purrr::safely(quiet_fetch, otherwise = NULL)
  retry_once <- purrr::insistently(
    f = safe_quiet,
    rate = purrr::rate_backoff(
      pause_min = backoff_min,
      pause_cap = backoff_cap,
      jitter = TRUE,
      max_times = 2
    )
  )

  purrr::slowly(retry_once, rate = purrr::rate_delay(pause = pause_seconds))
}

#' Append One Checkpoint Row
#'
#' @description
#' Appends a one-row checkpoint record to CSV so a run can resume from completed
#' sites in future sessions.
#'
#' @param checkpoint_csv_path Path to checkpoint CSV.
#' @param checkpoint_row One-row data frame with checkpoint fields.
append_checkpoint_row <- function(checkpoint_csv_path, checkpoint_row) {
  checkpoint_csv_path <- base::path.expand(checkpoint_csv_path)
  fs::dir_create(path = fs::path_dir(checkpoint_csv_path), recurse = TRUE)

  file_exists <- base::file.exists(checkpoint_csv_path)

  utils::write.table(
    x = checkpoint_row,
    file = checkpoint_csv_path,
    sep = ",",
    row.names = FALSE,
    col.names = !file_exists,
    append = file_exists,
    qmethod = "double"
  )
}

#' Read Completed Sites From Checkpoint CSV
#'
#' @description
#' Reads a checkpoint CSV and returns sites with terminal success statuses.
#'
#' @param checkpoint_csv_path Path to checkpoint CSV.
#'
#' @return Character vector of completed site IDs.
get_completed_sites_from_checkpoint <- function(checkpoint_csv_path) {
  checkpoint_csv_path <- base::path.expand(checkpoint_csv_path)

  if (!base::file.exists(checkpoint_csv_path)) {
    return(base::character(0))
  }

  checkpoint_df <- tryCatch(
    readr::read_csv(checkpoint_csv_path, show_col_types = FALSE),
    error = function(e) {
      base::warning(base::sprintf(
        "Could not read checkpoint CSV at %s: %s",
        checkpoint_csv_path,
        base::conditionMessage(e)
      ))
      return(NULL)
    }
  )

  if (base::is.null(checkpoint_df)) {
    return(base::character(0))
  }

  if (!base::all(base::c("site", "status") %in% base::names(checkpoint_df))) {
    return(base::character(0))
  }

  checkpoint_df |>
    (
      function(y) {
        y <- y[y$status %in% base::c("success_data", "success_empty"), , drop = FALSE]
        base::unique(base::as.character(y$site))
      }
    )()
}

#' Fetch One Site and Standardize Result Payload
#'
#' @param site A single `USGS-*` monitoring location ID.
#' @param fetcher Function returned by [make_fetch_daily_safe_paced()].
#' @param verbose_api_messages Logical; if `TRUE`, print captured USGS messages
#'   and warnings to console for each site.
#'
#' @return A named list with:
#' - `data`: data frame or `NULL`
#' - `log`: one-row data frame with status, timing, and diagnostics
fetch_site_payload <- function(site, fetcher, verbose_api_messages = TRUE) {
  start_time <- base::Sys.time()
  res <- fetcher(site)
  end_time <- base::Sys.time()

  duration_s <- base::as.numeric(
    base::difftime(end_time, start_time, units = "secs")
  )

  err_obj <- res$error
  out_df <- NULL
  warns_chr <- base::character(0)
  msgs_chr <- base::character(0)

  if (base::is.null(err_obj) && !base::is.null(res$result)) {
    out_df <- res$result$result
    warns_chr <- res$result$warnings
    msgs_chr <- res$result$messages
  }

  if (isTRUE(verbose_api_messages)) {
    if (base::length(msgs_chr) > 0L) {
      purrr::walk(
        msgs_chr,
        function(msg) {
          base::message(base::sprintf("[USGS][%s][message] %s", site, msg))
        }
      )
    }

    if (base::length(warns_chr) > 0L) {
      purrr::walk(
        warns_chr,
        function(wrn) {
          base::message(base::sprintf("[USGS][%s][warning] %s", site, wrn))
        }
      )
    }
  }

  if (!base::is.null(err_obj)) {
    log_row <- data.frame(
      site = site,
      start_time = start_time,
      end_time = end_time,
      duration_secs = duration_s,
      status = "error",
      rows_fetched = 0L,
      error = base::paste0(base::capture.output(base::print(err_obj)), collapse = "\n"),
      warnings = base::paste(warns_chr, collapse = " | "),
      messages = base::paste(msgs_chr, collapse = " | "),
      stringsAsFactors = FALSE
    )

    return(base::list(data = NULL, log = log_row))
  }

  if (base::is.null(out_df) || (base::is.data.frame(out_df) && base::nrow(out_df) == 0L)) {
    log_row <- data.frame(
      site = site,
      start_time = start_time,
      end_time = end_time,
      duration_secs = duration_s,
      status = "success_empty",
      rows_fetched = 0L,
      error = NA_character_,
      warnings = base::paste(warns_chr, collapse = " | "),
      messages = base::paste(msgs_chr, collapse = " | "),
      stringsAsFactors = FALSE
    )

    return(base::list(data = NULL, log = log_row))
  }

  out_df$usgs_site <- site
  out_df$retrieved_at <- end_time

  log_row <- data.frame(
    site = site,
    start_time = start_time,
    end_time = end_time,
    duration_secs = duration_s,
    status = "success_data",
    rows_fetched = base::nrow(out_df),
    error = NA_character_,
    warnings = base::paste(warns_chr, collapse = " | "),
    messages = base::paste(msgs_chr, collapse = " | "),
    stringsAsFactors = FALSE
  )

  base::list(data = out_df, log = log_row)
}

#' Detect Duplicate Composite Keys
#'
#' @description
#' Computes duplicate key counts using the composite key
#' (`usgs_site`, `time`, `parameter_code`, `statistic_id`).
#'
#' @param x A data frame returned by the USGS fetch workflow.
#'
#' @return A data frame with duplicate keys and count `n`; empty if none found.
find_duplicate_keys <- function(x) {
  required_cols <- base::c("usgs_site", "time", "parameter_code", "statistic_id")

  if (!base::all(required_cols %in% base::names(x))) {
    return(data.frame())
  }

  x |>
    dplyr::count(!!!rlang::syms(required_cols), name = "n") |>
    (
      function(y) {
        y <- y[y$n > 1L, , drop = FALSE]
        y[base::order(-y$n, y$usgs_site, y$time), , drop = FALSE]
      }
    )()
}

#' Build Safe Per-Site RDS File Name
#'
#' @param site Site ID (for example `USGS-06279500`).
#'
#' @return A sanitized file stem for use as `<stem>.rds`.
site_to_file_stem <- function(site) {
  site |>
    stringr::str_replace_all("[^A-Za-z0-9_-]", "_")
}

#' Combine Per-Site RDS Files Into One RDS
#'
#' @description
#' Reads all `*.rds` files from a directory, row-binds them, and writes a single
#' consolidated RDS file.
#'
#' @param per_site_dir Directory containing per-site RDS files.
#' @param output_rds_path Target path for combined RDS.
#'
#' @return Invisibly returns the combined data frame.
combine_site_rds_to_single <- function(
  per_site_dir,
  output_rds_path = USGS_OUTPUT_RDS_PATH
) {
  files <- fs::dir_ls(per_site_dir, recurse = FALSE, regexp = "\\.rds$")

  combined <- files |>
    purrr::map(base::readRDS) |>
    dplyr::bind_rows()

  readr::write_rds(combined, file = output_rds_path)
  invisible(combined)
}

#' Run USGS Daily Discharge Fetch and Write RDS Outputs
#'
#' @description
#' Executes site discovery, rate-limited USGS fetching, and file persistence.
#' Main data are written to a single RDS file; operation logs and duplicate-key
#' diagnostics are written to CSV files.
#'
#' @param sites Optional character vector of `USGS-*` site IDs. If `NULL`,
#'   sites are loaded from the local resource via [get_usgs_sites_from_pg()].
#' @param sites_resource_path Path to the local site resource CSV. Used only
#'   when `sites` is `NULL`.
#' @param output_rds_path Path for combined discharge RDS output.
#' @param log_csv_path Path for operational fetch log CSV.
#' @param duplicates_csv_path Path for duplicate-key diagnostics CSV.
#' @param per_site_dir Optional directory to also write one RDS per site. Use
#'   `NULL` to disable.
#' @param pause_seconds Delay between API calls in seconds.
#' @param backoff_min Minimum retry backoff in seconds.
#' @param backoff_cap Maximum retry backoff in seconds.
#' @param progress_every Frequency of progress messages in number of sites.
#' @param chunk_size Number of sites per logical chunk for progress messaging.
#' @param end_date End date for USGS query (`YYYY-MM-DD`).
#' @param resume_from_checkpoint Logical; if `TRUE`, skip sites that previously
#'   completed successfully according to `checkpoint_csv_path`.
#' @param checkpoint_csv_path Path to checkpoint CSV used for site-level resume.
#' @param verbose_api_messages Logical; if `TRUE`, print captured API
#'   message/warning output for each site to console.
#'
#' @return Invisibly returns a named list with summary counts and output paths.
run_usgs_to_rds <- function(
  sites = NULL,
  sites_resource_path = USGS_SITES_RESOURCE_PATH,
  output_rds_path = USGS_OUTPUT_RDS_PATH,
  log_csv_path = USGS_LOG_CSV_PATH,
  duplicates_csv_path = USGS_DUPLICATES_CSV_PATH,
  per_site_dir = USGS_PER_SITE_DIR,
  pause_seconds = USGS_PAUSE_SECONDS,
  backoff_min = USGS_BACKOFF_MIN_SECONDS,
  backoff_cap = USGS_BACKOFF_CAP_SECONDS,
  progress_every = 5L,
  chunk_size = 50L,
  end_date = "2025-12-31",
  resume_from_checkpoint = TRUE,
  checkpoint_csv_path = USGS_CHECKPOINT_CSV_PATH,
  verbose_api_messages = TRUE
) {
  if (base::is.null(sites)) {
    sites <- get_usgs_sites_from_pg(resource_path = sites_resource_path)
  } else {
    sites <- base::unique(base::as.character(sites))
    sites <- sites[!base::is.na(sites) & base::nzchar(sites)]
  }

  if (base::length(sites) == 0L) {
    base::message("No USGS sites available to process.")
    return(invisible(NULL))
  }

  checkpoint_csv_path <- base::path.expand(checkpoint_csv_path)

  completed_sites <- base::character(0)
  if (isTRUE(resume_from_checkpoint)) {
    completed_sites <- get_completed_sites_from_checkpoint(checkpoint_csv_path)
  }

  sites_to_process <- base::setdiff(sites, completed_sites)

  if (isTRUE(resume_from_checkpoint)) {
    base::message(base::sprintf(
      "Resume mode: %s/%s sites already completed; %s remaining.",
      base::length(completed_sites),
      base::length(sites),
      base::length(sites_to_process)
    ))
  }

  if (base::length(sites_to_process) == 0L) {
    base::message("No remaining sites to process after checkpoint filtering.")
    return(
      invisible(
        base::list(
          sites_requested = base::length(sites),
          sites_completed_prior = base::length(completed_sites),
          sites_attempted_this_run = 0L,
          output_rds_path = base::path.expand(output_rds_path),
          log_csv_path = base::path.expand(log_csv_path),
          duplicates_csv_path = base::path.expand(duplicates_csv_path),
          checkpoint_csv_path = checkpoint_csv_path
        )
      )
    )
  }

  if (!base::is.null(per_site_dir)) {
    fs::dir_create(path = per_site_dir, recurse = TRUE)
  }

  fetcher <- make_fetch_daily_safe_paced(
    pause_seconds = pause_seconds,
    backoff_min = backoff_min,
    backoff_cap = backoff_cap,
    end_date = end_date
  )

  site_chunks <- split_chunks(sites_to_process, n = base::as.integer(chunk_size))

  payloads <- site_chunks |>
    purrr::imap(
      function(chunk_sites, chunk_idx) {
        base::message(base::sprintf(
          "Processing chunk %s with %s sites...",
          chunk_idx,
          base::length(chunk_sites)
        ))

        chunk_sites |>
          purrr::imap(
            function(site, i) {
              payload <- fetch_site_payload(
                site = site,
                fetcher = fetcher,
                verbose_api_messages = verbose_api_messages
              )

              if ((i %% progress_every) == 0L) {
                base::message(base::sprintf(
                  "  Chunk %s progress: %s/%s",
                  chunk_idx,
                  i,
                  base::length(chunk_sites)
                ))
              }

              if (!base::is.null(per_site_dir) && !base::is.null(payload$data)) {
                out_file <- fs::path(
                  per_site_dir,
                  base::paste0(site_to_file_stem(site), ".rds")
                )
                readr::write_rds(payload$data, file = out_file)
              }

              checkpoint_row <- payload$log |>
                dplyr::mutate(
                  checkpoint_written_at = base::Sys.time(),
                  stringsAsFactors = FALSE
                )
              append_checkpoint_row(
                checkpoint_csv_path = checkpoint_csv_path,
                checkpoint_row = checkpoint_row
              )

              payload
            }
          )
      }
    ) |>
    purrr::flatten()

  log_df <- payloads |>
    purrr::map("log") |>
    dplyr::bind_rows()

  all_data <- payloads |>
    purrr::map("data") |>
    purrr::compact() |>
    dplyr::bind_rows()

  if (!base::is.null(per_site_dir)) {
    per_site_files <- fs::dir_ls(
      path = per_site_dir,
      recurse = FALSE,
      regexp = "\\.rds$"
    )

    if (base::length(per_site_files) > 0L) {
      all_data <- per_site_files |>
        purrr::map(base::readRDS) |>
        dplyr::bind_rows()
    }
  } else {
    if (isTRUE(resume_from_checkpoint) && base::file.exists(base::path.expand(output_rds_path))) {
      previous_data <- tryCatch(
        base::readRDS(base::path.expand(output_rds_path)),
        error = function(e) {
          base::warning(base::sprintf(
            "Could not read existing output RDS at %s: %s",
            base::path.expand(output_rds_path),
            base::conditionMessage(e)
          ))
          return(NULL)
        }
      )

      if (!base::is.null(previous_data) && base::is.data.frame(previous_data)) {
        all_data <- dplyr::bind_rows(previous_data, all_data)
      }
    }
  }

  duplicates_df <- find_duplicate_keys(all_data)

  fs::dir_create(path = fs::path_dir(base::path.expand(output_rds_path)), recurse = TRUE)
  fs::dir_create(path = fs::path_dir(base::path.expand(log_csv_path)), recurse = TRUE)
  fs::dir_create(path = fs::path_dir(base::path.expand(duplicates_csv_path)), recurse = TRUE)

  readr::write_rds(all_data, file = output_rds_path)
  readr::write_csv(log_df, file = log_csv_path)
  readr::write_csv(duplicates_df, file = duplicates_csv_path)

  status_counts <- base::as.data.frame(
    base::table(log_df$status),
    stringsAsFactors = FALSE
  )
  base::names(status_counts) <- base::c("status", "n")

  base::message("Done.")
  base::message(base::sprintf("Sites attempted: %s", base::nrow(log_df)))
  base::message(base::sprintf(
    "Rows fetched total: %s",
    base::ifelse(base::nrow(log_df) > 0L, base::sum(log_df$rows_fetched, na.rm = TRUE), 0L)
  ))
  base::message(base::sprintf("Duplicate keys found: %s", base::nrow(duplicates_df)))

  invisible(
    base::list(
      sites_attempted = base::nrow(log_df),
      rows_fetched = base::ifelse(base::nrow(log_df) > 0L, base::sum(log_df$rows_fetched, na.rm = TRUE), 0L),
      status_counts = status_counts,
      duplicates_n = base::nrow(duplicates_df),
      output_rds_path = base::path.expand(output_rds_path),
      log_csv_path = base::path.expand(log_csv_path),
      duplicates_csv_path = base::path.expand(duplicates_csv_path),
      checkpoint_csv_path = checkpoint_csv_path
    )
  )
}

#' Example Invocation
#'
#' @description
#' Main workflow does not require Postgres. To refresh the local site resource,
#' call [write_usgs_latest_discharge_sites_resource()] with a Postgres
#' connection.
#'
#' @examples
#' \dontrun{
#' # Optional refresh step (only when you need to rebuild site resource):
#' # write_usgs_latest_discharge_sites_resource(
#' #   pg = pg,
#' #   resource_path = fs::path("data", "usgs_latest_discharge_sites.csv")
#' # )
#'
#' run_usgs_to_rds(
#'   sites_resource_path = fs::path("data", "usgs_latest_discharge_sites.csv"),
#'   output_rds_path = "~/Desktop/usgs_discharge_daily.rds",
#'   log_csv_path = "~/Desktop/usgs_discharge_daily_log.csv",
#'   duplicates_csv_path = "~/Desktop/usgs_discharge_daily_duplicates.csv",
#'   per_site_dir = "~/Desktop/usgs_discharge_daily_by_site",
#'   pause_seconds = 180,
#'   backoff_min = 90,
#'   backoff_cap = 360,
#'   progress_every = 5L,
#'   chunk_size = 50L,
#'   end_date = "2025-12-31",
#'   resume_from_checkpoint = TRUE,
#'   checkpoint_csv_path = "~/Desktop/usgs_discharge_daily_checkpoint.csv",
#'   verbose_api_messages = TRUE
#' )
#' }
NULL
