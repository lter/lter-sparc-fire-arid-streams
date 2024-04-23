## ------------------------------------------------------- ##
# SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## pH

# Data Source
## POLARIS: A 30-meter probabilistic soil series map of the contiguous United States
## https://pubs.usgs.gov/publication/70170912

# Actual download link
## http://hydrology.cee.duke.edu/POLARIS/PROPERTIES/v1.0/ph/mean/0_5/

## -------------------------------- ##
# Housekeeping ----
## -------------------------------- ##

# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, sf, terra, exactextractr, geojsonio, scicomptools, googledrive)

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
                                                 "aridland_fire_catchments.geojson"),
                                   what = "sp") %>%
  ## Convert to simple features object
  sf::st_as_sf(x = .)

# Check result
dplyr::glimpse(sf_file)

# Exploratory plot
plot(sf_file["usgs_site"], axes = T)

# Identify the grouping columns
(group_cols <- c(setdiff(x = names(sf_file), y = c("geometry", "geom"))))

# Read in pH raster
ph_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "nasa_phation", 
                                       "SRTMGL3_NC.003_SRTMGL3_DEM_doy2000042_aid0001.tif"))

# Visual check for overlap
plot(ph_rast, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
# Extract pH ----
## -------------------------------- ##

# Actually extract pH data
ph_v1 <- exactextractr::exact_extract(x = ph_rast, y = sf_file,
                                        include_cols = group_cols,
                                        progress = T) %>%
  # Above returns a list so switch it to a dataframe
  purrr::list_rbind(x = .) %>% 
  # Filter out NAs
  dplyr::filter(!is.na(value))

## -------------------------------- ##
# Wrangle pH ----
## -------------------------------- ##

# Do needed post-processing
ph_v2 <- ph_v1 %>% 
  # Summarize within existing groups
  dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
  dplyr::summarize(ph_avg_m = mean(value, na.rm = T),
                   ph_median_m = median(value, na.rm = T),
                   ph_min_m = min(value, na.rm = T),
                   ph_max_m = max(value, na.rm = T)) %>%
  dplyr::ungroup()

# Check structure
dplyr::glimpse(ph_v2)

## -------------------------------- ##
# Export ----
## -------------------------------- ##

# Pick final object name
final_ph <- ph_v2

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
ph_path <- file.path(path, "extracted-data", "fire-arid_soil-ph.csv")

# Export the summarized data
write.csv(x = final_ph, na = '', row.names = F, file = ph_path)

# Upload to GoogleDrive
# googledrive::drive_upload(media = ph_path, overwrite = T,
#                           path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----
