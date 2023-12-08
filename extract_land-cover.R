## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## LAND COVER

# Data Source
## MODIS/Terra+Aqua Land Cover Type Yearly L3 Global 500 m SIN Grid
## https://lpdaac.usgs.gov/products/mcd12q1v061/

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
land_nc <- ncdf4::nc_open(filename = file.path(path, "raw-spatial-data", "modis_land-cover",
                                               "MCD12Q1.061_500m_aid0001.nc"))

# Look at this
print(land_nc)

# Read it as a raster too
## This format is more easily manipulable for our purposes
land_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "modis_land-cover",
                                       "MCD12Q1.061_500m_aid0001.nc"))

# Check names
names(land_rast)

# Check out just one of those
print(land_rast$LC_Type1_1)


## -------------------------------- ##
# Extract ----
## -------------------------------- ##




# Create an empty list to store this information in
out_list <- list()

# Identify how many layers are in this
(layer_ct <- length(names(air_rast)))

# We'll need to strip each layer separately
for(k in 1:layer_ct){
  # for(k in 1:2){ # Test loop
  
  # Build name of layer
  focal_layer <- paste0("air_", k)
  
  # Rotate so longitude is from -180 to 180 (rather than 0 to 360)
  rotated <- terra::rotate(x = air_rast[[focal_layer]])
  
  # Identify time of this layer
  layer_time <- terra::time(x = rotated)
  
  # Strip out the relevant bit
  small_out_df <- exactextractr::exact_extract(x = rotated, y = sf_file,
                                               include_cols = group_cols,
                                               progress = F) %>%
    # Above returns a list so switch it to a dataframe
    purrr::list_rbind(x = .) %>%
    # Filter out NAs
    dplyr::filter(!is.na(value)) %>%
    # Convert from Kelvin to Celsius
    dplyr::mutate(value_c = value - 273.15) %>%
    # Average temperature within river ID
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
    dplyr::summarize(value_avg = mean(value_c, na.rm = T)) %>%
    dplyr::ungroup() %>%
    # Add a column for what timestamp this is
    dplyr::mutate(time = layer_time, 
                  .before = dplyr::everything())
  
  # Add it to the list
  out_list[[focal_layer]] <- small_out_df
  
  # Success message
  message("Processing complete for ", layer_time, " (number ", k, " of ", layer_ct, ")") }

# Exploratory plot one of what we just extracted
plot(rotated, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)



## -------------------------------- ##
# Wrangle ----
## -------------------------------- ##



## -------------------------------- ##
# Export ----
## -------------------------------- ##


# End ----

