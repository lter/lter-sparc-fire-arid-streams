#!/usr/bin/env Rscript

# Get command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2) {
  stop("Usage: Rscript run_single_site.R <site_id> <output_dir>")
}

site_id <- args[1]
output_dir <- args[2]

# Load required libraries
library(sf)
library(dplyr)
library(purrr)
library(readr)
library(future)
library(furrr)

# Set an upper bound for serialized globals (~2 TiB). This is effectively
# near-unlimited for practical purposes while still finite.
options(future.globals.maxSize = 2 * 1024^4)
message("future.globals.maxSize set to ", format(2 * 1024^4, scientific = FALSE), " bytes (~2 TiB)")

# Source the calculate function
source("/scratch/srearl/firearea/R/calculate_hydrologic_distance.R")

# Set up parallelization ONCE at the top level for the entire site
# calculate_fire_flow_length() will inherit this plan for its furrr::future_map2_dbl() calls

# Allow override via environment variable (set in SLURM script if desired)
if (!is.na(Sys.getenv("SLURM_CPUS_PER_TASK", unset = NA))) {
  # Use SLURM allocation directly
  available_cores <- as.numeric(Sys.getenv("SLURM_CPUS_PER_TASK"))
  workers <- available_cores
} else {
  # Fallback to detection
  available_cores <- future::availableCores()
  workers <- max(1, available_cores - 1)  # Leave one core free
}

message("Site: ", site_id)
message("Available cores: ", available_cores)
message("Using workers: ", workers)

if (workers > 1) {
  future::plan(future::multicore, workers = workers)
} else {
  future::plan(future::sequential)
  message("Using sequential processing")
}

# Define the return_distances function (same as before but without outer parallelization)
return_distances <- function(path, this_site) {

  message("starting site: ", this_site)

  catchment <- sf::st_read(dsn = paste0(path, "catchments.geojson")) |>
    dplyr::filter(
      grepl(
        pattern     = this_site,
        x           = usgs_site,
        ignore.case = TRUE
      )
    )

  fires <- sf::st_read(dsn = paste0(path, "fires_catchments.geojson")) |>
    dplyr::filter(
      grepl(
        pattern     = this_site,
        x           = usgs_site,
        ignore.case = TRUE
      )
    )

  flowlines <- sf::st_read(dsn = paste0(path, "flowlines.geojson")) |>
    dplyr::filter(
      grepl(
        pattern     = this_site,
        x           = usgs_site,
        ignore.case = TRUE
      )
    )

  sampling <- sf::st_read(dsn = paste0(path, "pour_points.geojson")) |>
    dplyr::filter(
      grepl(
        pattern     = this_site,
        x           = usgs_site,
        ignore.case = TRUE
      )
    )

  message(
    "catchment: ",
    nrow(catchment),
    "\ncatchment_fires: ",
    nrow(fires),
    "\ncatchment_flowlines: ",
    nrow(flowlines),
    "\ncatchment_sampling: ",
    nrow(sampling)
  )

  if (
    nrow(catchment) == 0 ||
      nrow(fires) == 0 ||
      nrow(flowlines) == 0 ||
      nrow(sampling) == 0
  ) {
    stop("one or more required data frames have zero rows")
  }

  # Process fires sequentially - calculate_fire_flow_length will use the future plan set above
  distances <- split(
    x = fires,
    f = fires$event_id
  ) |>
    {
      \(fire) {
        purrr::map(
          .x = fire,
          .f = ~ calculate_fire_flow_length(
            catchment           = catchment,
            catchment_fire      = .x,
            catchment_flowlines = flowlines,
            catchment_sampling  = sampling
          )
        )
      }
    }() |>
    dplyr::bind_rows()

  message("completing site: ", this_site)

  return(distances)
}

# Process the site with error handling
result <- tryCatch({
  return_distances(
    path = "/scratch/srearl/nitrate_spatial/",
    this_site = site_id
  )
}, error = function(e) {
  message("Error for site ", site_id, ": ", e$message)
  write_lines(
    paste0(site_id, ": ", e$message),
    file = file.path(output_dir, "failed_sites.txt"),
    append = TRUE
  )
  return(NULL)
})

# Write results
if (!is.null(result) && nrow(result) > 0) {
  write_csv(
    result, 
    file = file.path(output_dir, paste0("distance_", site_id, ".csv"))
  )
  message("Successfully wrote results for site: ", site_id)
} else {
  message("No results to write for site: ", site_id)
  write_lines(
    site_id,
    file = file.path(output_dir, "sites_no_data.txt"),
    append = TRUE
  )
}

# Clean up - reset to sequential to be a good citizen
future::plan(future::sequential)
