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

# Clean up environment
rm(list = setdiff(ls(), c('path', 'sf_file', 'group_cols')))

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

