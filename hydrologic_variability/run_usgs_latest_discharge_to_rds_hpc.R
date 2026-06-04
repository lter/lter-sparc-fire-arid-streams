#!/usr/bin/env Rscript

# HPC runner for usgs_latest_discharge_to_rds.R
# - Installs required packages if missing
# - Sources workflow script
# - Runs USGS fetch to RDS using local site resource

options(repos = c(CRAN = "https://cloud.r-project.org"))

configure_user_library <- function() {
  r_ver <- paste0(R.version$major, ".", strsplit(R.version$minor, "\\.")[[1]][1])
  user_lib <- Sys.getenv(
    "R_LIBS_USER",
    unset = file.path(Sys.getenv("HOME"), "R", paste0("library-", r_ver))
  )

  dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
  .libPaths(unique(c(normalizePath(user_lib, mustWork = FALSE), .libPaths())))

  message("Using R user library: ", .libPaths()[1])
}

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
    install.packages(missing, dependencies = TRUE, lib = .libPaths()[1])
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

configure_user_library()
install_if_missing(required_packages)

script_dir <- get_script_dir()
workflow_file <- file.path(script_dir, "usgs_latest_discharge_to_rds.R")
source(workflow_file)

sites_resource_path <- fs::path(script_dir, "data", "usgs_latest_discharge_sites.csv")

run_usgs_to_rds(
  sites_resource_path = sites_resource_path,
  resume_from_checkpoint = TRUE,
  verbose_api_messages = TRUE
)

