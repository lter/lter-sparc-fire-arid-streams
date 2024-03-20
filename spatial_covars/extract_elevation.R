## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## ELEVATION

# Data Source
## NASA Shuttle Radar Topography Mission Global 3 arc second
## https://lpdaac.usgs.gov/products/srtmgl3v003/

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

# Read in elevation raster
elev_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "nasa_elevation", 
                                       "SRTMGL3_NC.003_SRTMGL3_DEM_doy2000042_aid0001.tif"))

# Visual check for overlap
plot(elev_rast, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
      # Extract Elevation ----
## -------------------------------- ##

# No scale factor used
# Valid range is >= -32767 and <= 32767

# Actually extract elevation data
elev_v1 <- exactextractr::exact_extract(x = elev_rast, y = sf_file,
                                        include_cols = group_cols,
                                        progress = T) %>%
  # Above returns a list so switch it to a dataframe
  purrr::list_rbind(x = .) %>% 
  # Filter out NAs
  dplyr::filter(!is.na(value)) %>%
  # Drop fill values
  dplyr::filter(value > -32768)

## -------------------------------- ##
      # Wrangle Elevation ----
## -------------------------------- ##

# Do needed post-processing
elev_v2 <- elev_v1 %>% 
  # Summarize within existing groups
  dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
  dplyr::summarize(elev_avg = mean(value, na.rm = T),
                   elev_median = median(value, na.rm = T),
                   elev_min = min(value, na.rm = T),
                   elev_max = max(value, na.rm = T)) %>%
  dplyr::ungroup()

# Check structure
dplyr::glimpse(elev_v2)

## -------------------------------- ##
        # Identify Slope ----
## -------------------------------- ##

# Create an empty list to store results
slope_list <- list()

# For each watershed shapefile...
for (i in 1:nrow(sheds)){
  
  # Starting message
  message("Extracting slope for watershed ", i, " (", (nrow(sheds) - i), " remaining)")
  
  # Crop and mask the elevation raster to each shapefile
  cropped_raster <- terra::crop(x = elev_raw, y = terra::vect(sheds[i,]), mask = TRUE)
  
  # Calculate the slopes
  slope_raster <- terra::terrain(cropped_raster, v = "slope", unit = "degrees")
  
  # Extract the slopes into a dataframe
  slope_dataframe <- terra::as.data.frame(slope_raster)
  
  # Create a dummy dataframe if the extracted dataframe has 0 rows
  if (nrow(slope_dataframe) == 0){
    LTER_Shapefile_slope_dataframe <- data.frame(LTER = sheds[i,]$LTER,
                                                 Shapefile_Name = sheds[i,]$Shapefile_Name,
                                                 basin_slope_median_degree = NA,
                                                 basin_slope_mean_degree = NA,
                                                 basin_slope_min_degree = NA,
                                                 basin_slope_max_degree = NA)
    
    # Save the dataframe into our list
    slope_list[[i]] <- LTER_Shapefile_slope_dataframe
    
    # If the dataframe is NOT empty...
  } else {
    # Calculate slope statistics
    LTER_Shapefile_slope_dataframe <- slope_dataframe %>%
      # Keep LTER/shapefile name for summarizing and joining later
      dplyr::mutate(LTER = sheds[i,]$LTER,
                    Shapefile_Name = sheds[i,]$Shapefile_Name) %>%
      # Calculate slope statistics within those groups
      dplyr::group_by(LTER, Shapefile_Name) %>%
      dplyr::summarize(basin_slope_median_degree = stats::median(slope, na.rm = T),
                       basin_slope_mean_degree = spatialEco::mean_angle(slope, angle = "degree"),
                       basin_slope_min_degree = min(slope, na.rm = T),
                       basin_slope_max_degree = max(slope, na.rm = T))
    
    # Save the dataframe into our list
    slope_list[[i]] <- LTER_Shapefile_slope_dataframe }
  
} # Close loop

# Unlist into one big dataframe
slope_actual <- slope_list %>% purrr::map_dfr(.f = select, everything())

# Glimpse this
dplyr::glimpse(slope_actual)



## -------------------------------- ##
             # Export ----
## -------------------------------- ##

# Pick final object name
final_elev <- elev_v2

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
elev_path <- file.path(path, "extracted-data", "fire-arid_elevation.csv")

# Export the summarized data
write.csv(x = final_elev, na = '', row.names = F, file = elev_path)

# Upload to GoogleDrive
googledrive::drive_upload(media = elev_path, overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----
