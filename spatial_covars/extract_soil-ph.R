## ------------------------------------------------------- ##
# SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## pH

# Data Source
## NASA Shuttle Radar Topography Mission Global 3 arc second
## https://lpdaac.usgs.gov/products/srtmgl3v003/

## -------------------------------- ##
# Housekeeping ----
## -------------------------------- ##

# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, sf, ncdf4, terra, exactextractr, spatialEco, geojsonio, scicomptools, googledrive)

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

# No scale factor used
# Valid range is >= -32767 and <= 32767

# Actually extract pH data
ph_v1 <- exactextractr::exact_extract(x = ph_rast, y = sf_file,
                                        include_cols = group_cols,
                                        progress = T) %>%
  # Above returns a list so switch it to a dataframe
  purrr::list_rbind(x = .) %>% 
  # Filter out NAs
  dplyr::filter(!is.na(value)) %>%
  # Drop fill values
  dplyr::filter(value > -32768)

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
# Identify Slope ----
## -------------------------------- ##

# Create an empty list to store results
slope_list <- list()

# For each catchment polygon:
for(k in 1:nrow(sf_file)){
  
  # Starting message
  message("Extracting slope for catchment ", k, " (", (nrow(sf_file) - k), " remaining)")
  
  # Crop and mask the pH raster to each shapefile
  ph_crop <- terra::crop(x = ph_rast, y = terra::vect(sf_file[k,]), mask = T)
  
  # Calculate the slopes
  slope_rast <- terra::terrain(ph_crop, v = "slope", unit = "degrees")
  
  # Extract the slopes into a dataframe
  slope_v1 <- terra::as.data.frame(slope_rast)
  
  # If the dataframe is NOT empty...
  if(nrow(slope_v1) != 0){
    
    # Duplicate the data object
    slope_v2 <- slope_v1
    
    # Get a non-spatial version of catchment identifying information
    catch_id <- sf::st_drop_geometry(sf_file[k,])
    
    # Attach each piece of ID info to the slope information
    for(gp in group_cols){ slope_v2[[gp]] <- catch_id[[gp]] }
    
    # Within catchments, summarize various aspects of slope
    slope_v3 <- slope_v2 %>% 
      dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
      dplyr::summarize(slope_avg_deg = spatialEco::mean_angle(slope, angle = "degree"),
                       slope_median_deg = median(slope, na.rm = T),
                       slope_min_deg = min(slope, na.rm = T),
                       slope_max_deg = max(slope, na.rm = T)) %>%
      dplyr::ungroup()
    
    # Add to the list of slope information
    slope_list[[k]] <- slope_v3 }
  
} # Close loop

## -------------------------------- ##
# Wrangle Slope ----
## -------------------------------- ##

# Wrangle the extracted/summarized slope information
slope_v4 <- slope_list %>% 
  # Unlist the list
  purrr::list_rbind(x = .)

# Check structure of that output
dplyr::glimpse(slope_v4)

## -------------------------------- ##
# Combine pH & Slope ----
## -------------------------------- ##

# Combine the two dataframes
dem_out <- dplyr::full_join(x = ph_v2, y = slope_v4,
                            by = group_cols)

# Check structure
dplyr::glimpse(dem_out)

## -------------------------------- ##
# Export ----
## -------------------------------- ##

# Pick final object name
final_ph <- dem_out

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
ph_path <- file.path(path, "extracted-data", "fire-arid_phation.csv")

# Export the summarized data
write.csv(x = final_ph, na = '', row.names = F, file = ph_path)

# Upload to GoogleDrive
# googledrive::drive_upload(media = ph_path, overwrite = T,
#                           path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----
