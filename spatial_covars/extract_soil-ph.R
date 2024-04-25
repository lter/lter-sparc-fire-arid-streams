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

# Define soil-specific path
soil_path <- file.path(path, "raw-spatial-data", "polaris_soil", "polaris_ph-mean-0-5_tiles")

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

# Read in a pH raster
ph_rast <- terra::rast(x = file.path(soil_path, "lat2425_lon-98-97.tif"))

# Visual check for overlap
# plot(ph_rast, axes = T, reset = F)
# plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
            # Extract pH ----
## -------------------------------- ##

# List all files in that folder
ph_files <- sort(dir(path = soil_path, pattern = ".tif"))

# Make an empty list
ph_list <- list()

# Loop across each raster tile
for(focal_ph in ph_files){
  
  # Progress message
  message("Beginning extraction for raster tile '", focal_ph, "'")
  
  # Read it in as a raster
  focal_rast <- terra::rast(x = file.path(soil_path, focal_ph))
  
  # Actually extract the data
  focal_df <- exactextractr::exact_extract(x = focal_rast, y = sf_file,
                                           include_cols = group_cols,
                                           progress = F) %>%
    # Above returns a list so switch it to a dataframe
    purrr::list_rbind(x = .) %>% 
    # Filter out NAs
    dplyr::filter(!is.na(value))
  
  # Add to the list if there's any content
  if(nrow(focal_df) >= 1){ ph_list[[as.character(focal_ph)]] <- focal_df }
  
} # Close extraction loop

# Tidy environment
rm(list = setdiff(x = ls(), y = c("sf_file", "group_cols", "path", "soil_path", "ph_list")))

## -------------------------------- ##
          # Wrangle pH ----
## -------------------------------- ##

# Make a second empty list
ph_v0 <- list()

# Loop across *sites* to do summarization
for(focal_site in sort(unique(sf_file$usgs_site))){
  
  # Processing message
  message("Summarizing soil data for '", focal_site, "'")
  
  # Make a list for this site
  site_list <- list()
  
  # Loop across list elements of the original extraction
  for(k in 1:length(ph_list)){
  
    # Grab list element & subset to just this focal site
    focal_ph_data <- ph_list[[k]] %>% 
      dplyr::filter(usgs_site == focal_site)
    
    # Add this to the list for this site
    site_list[[paste0(focal_site, "-", k)]] <- focal_ph_data
    
  } # Close inner loop
  
  # Process the site-specific list
  site_df <- site_list %>% 
    # Unlist
    purrr::list_rbind(x = .) %>% 
    # Summarize to reduce data size
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
    dplyr::summarize(soil_ph_avg = mean(value, na.rm = T),
                     soil_ph_median = median(value, na.rm = T),
                     soil_ph_min = min(value, na.rm = T),
                     soil_ph_max = max(value, na.rm = T)) %>%
    dplyr::ungroup()
  
  # Add to larger list
  ph_v0[[paste(focal_site)]] <- site_df
  
} # Close outer loop

# Process that output
ph_v1 <- ph_v0 %>% 
  purrr::list_rbind(x = .)

# Check structure
dplyr::glimpse(ph_v1)

## -------------------------------- ##
            # Export ----
## -------------------------------- ##

# Pick final object name
final_ph <- ph_v1

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
ph_path <- file.path(path, "extracted-data", "fire-arid_soil-ph.csv")

# Export the summarized data
write.csv(x = final_ph, na = '', row.names = F, file = ph_path)

# Upload to GoogleDrive
googledrive::drive_upload(media = ph_path, overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----
