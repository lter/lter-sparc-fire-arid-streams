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

# Visual check for overlap
plot(land_rast$LC_Type1_1, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
          # Extract ----
## -------------------------------- ##

# Create an empty list for storing extracted data
out_list <- list()

# Identify the names of the layers we want to extract
(wanted_layers <- setdiff(x = names(land_rast), y = paste0("QC_", 1:22)))

# Loop across layers extracting each as we go
for(focal_layer in wanted_layers){
  
  # Print start message
  message("Extraction begun for '", focal_layer, "'")
  
  # Identify time of this layer
  layer_time <- terra::time(x = land_rast[[focal_layer]])
  
  # Extract information
  small_out_df <- exactextractr::exact_extract(x = land_rast[[focal_layer]],
                                               y = sf_file,
                                               include_cols = group_cols,
                                               progress = T) %>%
    # Above returns a list so switch it to a dataframe
    purrr::list_rbind(x = .) %>%
    # Filter out NAs
    dplyr::filter(!is.na(value)) %>%
    # Count number of pixels per land cover type
    dplyr::group_by(dplyr::across(dplyr::all_of(c(group_cols, "value")))) %>% 
    dplyr::summarize(pixel_ct = dplyr::n()) %>% 
    dplyr::ungroup() %>%
    # Add a column for the layer name and layer time
    dplyr::mutate(type = focal_layer,
                  time = layer_time, 
                  .before = dplyr::everything())
  
  # Add this to the list
  out_list[[focal_layer]] <- small_out_df
  
  # Success message
  message("Processing complete for ", focal_layer, " at ", layer_time) }

## -------------------------------- ##
# Wrangle ----
## -------------------------------- ##



## -------------------------------- ##
# Export ----
## -------------------------------- ##


# End ----

