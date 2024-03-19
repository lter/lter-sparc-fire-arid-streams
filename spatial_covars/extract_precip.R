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

# Create an empty list for storing extracted data
out_list <- list()

# Annual precip data (one netCDF / year) in this folder
(annual_ncdfs <- dir(file.path(path, "raw-spatial-data", "gridmet_precip")))

# Loop across those
for(year_nc in annual_ncdfs){
  
  # Load in that file
  year_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "gridmet_precip", year_nc))
  
  # Identify year
  year_num <- stringr::str_extract(string = year_nc, pattern = "[[:digit:]]{4}")
  
  # Starting message
  message("Beginning extraction for daily precipitation in ", year_num)
  
  # Loop across days
  for(k in 1:length(names(year_rast))){
    
    # Identify focal layer name
    focal_layer <- sort(names(year_rast))[k]
    
    # Strip out the precipitation of that day
    small_out_df <- exactextractr::exact_extract(x = year_rast[[focal_layer]],
                                                 y = sf_file,
                                                 include_cols = group_cols,
                                                 progress = F) %>%
      # Above returns a list so switch it to a dataframe
      purrr::list_rbind(x = .) %>%
      # Filter out NAs
      dplyr::filter(!is.na(value)) %>%
      # Drop fill value
      dplyr::filter(value != 32767) %>% 
      # Apply scaling factor
      dplyr::mutate(value_fix = value * 0.1) %>% 
      # Drop unwanted columns
      dplyr::select(-value, -coverage_fraction) %>% 
      # Summarize across pixels within time
      dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>% 
      dplyr::summarize(value_avg = mean(value_fix, na.rm = T)) %>% 
      dplyr::ungroup() %>%
      # Add a column timing
      dplyr::mutate(year = year_num,
                    day = k,
                    gregorian_day = stringr::str_extract(string = focal_layer, 
                                                         pattern = "[[:digit:]]{5,7}"),
                    .before = dplyr::everything())
    
    # Add to list
    out_list[[paste0(year_num, "_", k)]] <- small_out_df
    
    # Success message
    message("Processing complete for day ", k, " in ", year_num) 
  } # Close day loop
} # Close year loop

## -------------------------------- ##
            # Wrangle ----
## -------------------------------- ##

# Unlist the output
precip_v1 <- purrr::list_rbind(x = out_list)

# Check structure
dplyr::glimpse(precip_v1)

# Do needed wrangling
precip_v2 <- precip_v1 %>% 
  # Reorder columns
  dplyr::relocate(usgs_site:area_km2, .before = year) %>% 
  # Rename the value column more intuitively
  dplyr::rename(precip_mm = value_avg)

# Re-check structure
dplyr::glimpse(precip_v2)

## -------------------------------- ##
             # Export ----
## -------------------------------- ##

# Pick final object name
final_precip <- precip_v2

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
precip_path <- file.path(path, "extracted-data", "fire-arid_precipitation.csv")

# Export the summarized data
write.csv(x = final_precip, na = '', row.names = F, file = precip_path)

# Upload to GoogleDrive
googledrive::drive_upload(media = precip_path, overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----

