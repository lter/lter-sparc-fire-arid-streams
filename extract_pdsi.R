## ------------------------------------------------------- ##
  # SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## PALMER DROUGHT SEVERITY INDEX (PDSI)

# Data Source
## NOAA Palmer Drought Severity Index
## https://catalog.data.gov/dataset/palmer-drought-severity-index

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

# Read in the netCDF file and examine for context on units / etc.
pdsi_nc <- ncdf4::nc_open(filename = file.path(path, "raw-spatial-data", "noaa_pdsi", 
                                               "noaa_pdsi-monthly.nc"))

# Look at this
print(pdsi_nc)

# Read it as a raster too
## This format is more easily manipulable for our purposes
pdsi_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "noaa_pdsi", 
                                       "noaa_pdsi-monthly.nc"))

# Check names
names(pdsi_rast)

# Check out just one of those
print(pdsi_rast$pdsi_1)

# Visual check for overlap
plot(pdsi_rast$pdsi_1, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
# Extract ----
## -------------------------------- ##




## -------------------------------- ##
# Wrangle ----
## -------------------------------- ##



## -------------------------------- ##
# Export ----
## -------------------------------- ##


# End ----

