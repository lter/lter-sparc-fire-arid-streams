#!/usr/bin/env Rscript

# HPC runner for usgs_latest_discharge_to_rds.R
# - Installs required packages if missing
# - Sources workflow script
# - Runs USGS fetch to RDS using local site resource

options(repos = c(CRAN = "https://cloud.r-project.org"))

required_packages <- c(
  "dataRetrieval",
  "dplyr",
  "fs",
  "purrr",
  "readr",
  "rlang",
  "stringr"
)

install_if_missing <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0L) {
    install.packages(missing, dependencies = TRUE)
  }
}

get_script_dir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grepl("^--file=", args)]
  if (length(file_arg) > 0L) {
    return(normalizePath(dirname(sub("^--file=", "", file_arg[1])), mustWork = TRUE))
  }
  # Fallback for interactive/debug contexts
  normalizePath(getwd(), mustWork = TRUE)
}

install_if_missing(required_packages)

script_dir <- get_script_dir()
workflow_file <- file.path(script_dir, "usgs_latest_discharge_to_rds.R")
source(workflow_file)

sites_resource_path <- fs::path(script_dir, "data", "usgs_latest_discharge_sites.csv")

# Optional output override via environment variable on HPC.
# If unset, defaults from usgs_latest_discharge_to_rds.R are used.
output_dir_env <- Sys.getenv("USGS_OUTPUT_DIR", unset = "")

if (nzchar(output_dir_env)) {
  output_rds_path <- fs::path(output_dir_env, "usgs_discharge_daily.rds")
  log_csv_path <- fs::path(output_dir_env, "usgs_discharge_daily_log.csv")
  duplicates_csv_path <- fs::path(output_dir_env, "usgs_discharge_daily_duplicates.csv")
  checkpoint_csv_path <- fs::path(output_dir_env, "usgs_discharge_daily_checkpoint.csv")
  per_site_dir <- fs::path(output_dir_env, "usgs_discharge_daily_by_site")

  run_usgs_to_rds(
    sites_resource_path = sites_resource_path,
    output_rds_path = output_rds_path,
    log_csv_path = log_csv_path,
    duplicates_csv_path = duplicates_csv_path,
    checkpoint_csv_path = checkpoint_csv_path,
    per_site_dir = per_site_dir,
    resume_from_checkpoint = TRUE,
    verbose_api_messages = TRUE
  )
} else {
  run_usgs_to_rds(
    sites_resource_path = sites_resource_path,
    resume_from_checkpoint = TRUE,
    verbose_api_messages = TRUE
  )
}

