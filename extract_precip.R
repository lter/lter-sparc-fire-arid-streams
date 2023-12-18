## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## PRECIPITATION

# Data Source
## GridMET
## https://www.climatologylab.org/gridmet.html

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

# Read in one netCDF file and examine for context
precip_nc <- ncdf4::nc_open(filename = file.path(path, "raw-spatial-data", "gridmet_precip", 
                                                 "gridmet_pr_1980.nc"))

# Examine the netCDF example further
print(precip_nc)

# Read it as a raster too (more easily manipulable format)
precip_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "gridmet_precip", 
                                         "gridmet_pr_1980.nc"))

# Check names
names(precip_rast)

# Check out just one of those
print(precip_rast$"precipitation_amount_day=29219")

# Visual check for overlap
plot(precip_rast$"precipitation_amount_day=29219", axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
# Extract ----
## -------------------------------- ##

# Annual precip data (one netCDF / year) in this folder
(annual_ncdfs <- dir(file.path(path, "raw-spatial-data", "gridmet_precip")))







# Fill value: 32767


## -------------------------------- ##
# Wrangle ----
## -------------------------------- ##



## -------------------------------- ##
# Export ----
## -------------------------------- ##


# End ----

