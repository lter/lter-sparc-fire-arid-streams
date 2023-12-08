## ------------------------------------------------------- ##
# SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## ELEVATION

# Data Source
## NASA Shuttle Radar Topography Mission Global 1 arc second
## https://lpdaac.usgs.gov/products/srtmgl1v003/

## -------------------------------- ##
          # Housekeeping ----
## -------------------------------- ##

# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, sf, ncdf4, terra, exactextractr, geojsonio, scicomptools, googledrive)

# Silence `dplyr::summarize` preemptively
options(dplyr.summarise.inform = FALSE)

# Clear environment / collect garbage
rm(list = ls()); gc()

# Identify path to location of shared data
(path <- scicomptools::wd_loc(local = F, 
                              remote_path = file.path('/', "home", "shares",
                                                      "lter-sparc-fire-arid"),
                              local_path = getwd()))

## -------------------------------- ##
        # Extraction Prep ----
## -------------------------------- ##

# Load in the catchment delineations (stored as GeoJSON)
sf_file <- geojsonio::geojson_read(x = file.path(path, "catchment-geojsons",
                                                 "four_corners_catchments.geojson"),
                                   what = "sp") %>%
  ## Convert to simple features object
  sf::st_as_sf(x = .)

# Check result
dplyr::glimpse(sf_file)

# Exploratory plot
plot(sf_file["usgs_site"], axes = T)

# Identify the grouping columns
(group_cols <- c(setdiff(x = names(sf_file), y = c("geometry", "geom"))))

# Load elevation data
elev_raw <- terra::rast(x = file.path(path, "raw-spatial-data", "nasa_elevation", 
                                      "SRTMGL1_NC.003_SRTMGL1_DEM_doy2000042_aid0001.tif"))

# Visual check for overlap
plot(elev_raw, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
# Extract ----
## -------------------------------- ##



# BASEMENT----


# Strip out land cover for our polygons
elev_out <- exactextractr::exact_extract(x = elev_raw, y = sheds,
                                         include_cols = c("LTER", "Shapefile_Name")) %>%
  # Above returns a list so switch it to a dataframe
  purrr::map_dfr(dplyr::select, dplyr::everything()) %>%
  # Filter out NAs
  dplyr::filter(!is.na(value))

# Check that dataframe
dplyr::glimpse(elev_out)

## ------------------------------------------------------- ##
# Elevation - Summarize ----
## ------------------------------------------------------- ##

# Wrangle extracted data
elev_actual <- elev_out %>%
  # Summarize elevation within river ID
  dplyr::group_by(LTER, Shapefile_Name) %>%
  dplyr::summarize(elevation_median_m = stats::median(value, na.rm = T),
                   elevation_mean_m = mean(value, na.rm = T),
                   elevation_min_m = min(value, na.rm = T),
                   elevation_max_m = max(value, na.rm = T)) %>%
  dplyr::ungroup()

# Glimpse this
dplyr::glimpse(elev_actual)


## -------------------------------- ##
# Wrangle ----
## -------------------------------- ##



## -------------------------------- ##
# Export ----
## -------------------------------- ##


# End ----

