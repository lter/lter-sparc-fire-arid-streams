## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## GROSS PRIMARY PRODUCTIVITY (GPP)

# Data Source
## MODIS/Terra Gross Primary Productivity 8-Day L4 Global 500 m SIN Grid
## https://lpdaac.usgs.gov/products/mod17a2hv061/

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
gpp_nc <- ncdf4::nc_open(filename = file.path(path, "raw-spatial-data", "modis_gpp", 
                                               "MOD17A2H.061_500m_aid0001.nc"))

# Look at this
print(gpp_nc)

# Read it as a raster too
## This format is more easily manipulable for our purposes
gpp_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "modis_gpp", 
                                      "MOD17A2H.061_500m_aid0001.nc"))

# Check names
names(gpp_rast)

# Check out just one of those
print(gpp_rast$Gpp_500m_1)

# Visual check for overlap
plot(gpp_rast$Gpp_500m_1, axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
            # Extract ----
## -------------------------------- ##

# Create an empty list for storing extracted data
out_list <- list()

# Identify the names of the layers we want to extract
(wanted_layers <- setdiff(x = names(gpp_rast), y = paste0("Psn_QC_500m_", 1:133)))

# Loop across layers extracting each as we go
for(focal_layer in wanted_layers){
  
  # Print start message
  message("Extraction begun for '", focal_layer, "'")
  
  # Identify time of this layer
  layer_time <- terra::time(x = gpp_rast[[focal_layer]])
  
  # Extract information
  small_out_df <- exactextractr::exact_extract(x = gpp_rast[[focal_layer]],
                                               y = sf_file,
                                               include_cols = group_cols,
                                               progress = T) %>%
    # Above returns a list so switch it to a dataframe
    purrr::list_rbind(x = .) %>%
    # Filter out NAs
    dplyr::filter(!is.na(value)) %>%
    # Filter to only values within allowed range
    dplyr::filter(value >= 0 & value <= 30000) %>% 
    # Apply scaling factor
    dplyr::mutate(value_fix = value * 0.0001) %>% 
    # Drop unwanted columns
    dplyr::select(-value, -coverage_fraction) %>% 
    # Summarize across pixels within time
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>% 
    dplyr::summarize(value_avg = mean(value_fix, na.rm = T)) %>% 
    dplyr::ungroup() %>%
    # Add a column for the layer name and layer time
    dplyr::mutate(time = layer_time, 
                  .before = dplyr::everything())
  
  # Add this to the list
  out_list[[focal_layer]] <- small_out_df
  
  # Success message
  message("Processing complete for ", focal_layer, " at ", layer_time) }

## -------------------------------- ##
            # Wrangle ----
## -------------------------------- ##

# Unlist the output of that loop for easier wrangling
gpp_v1 <- purrr::list_rbind(x = out_list)

# Check structure
dplyr::glimpse(gpp_v1)

# Do needed wrangling
gpp_v2 <- gpp_v1 %>% 
  # Move the site info columns to the left
  dplyr::relocate(usgs_site:area_km2, .before = time) %>% 
  # Rename extracted information
  dplyr::rename(gpp_kg_C_m2 = value_avg) %>% 
  # Separate time into useful subcomponents
  dplyr::mutate(year = as.numeric(stringr::str_sub(string = time, start = 1, end = 4)),
                month = as.numeric(stringr::str_sub(string = time, start = 6, end = 7)),
                day = as.numeric(stringr::str_sub(string = time, start = 9, end = 10)),
                .after = time) %>% 
  # Make month column more legible
  dplyr::mutate(month_name = dplyr::case_when(
    month == 1 ~ "jan", month == 2 ~ "feb", month == 3 ~ "mar",
    month == 4 ~ "apr", month == 5 ~ "may", month == 6 ~ "jun",
    month == 7 ~ "jul", month == 8 ~ "aug", month == 9 ~ "sep",
    month == 10 ~ "oct", month == 11 ~ "nov", month == 12 ~ "dec",
    T ~ 'x'), .after = month)

# Re-check structure
dplyr::glimpse(gpp_v2)

## -------------------------------- ##
             # Export ----
## -------------------------------- ##

# Pick final object name
final_gpp <- gpp_v2

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
gpp_path <- file.path(path, "extracted-data", "fire-arid_gpp.csv")

# Export the summarized data
write.csv(x = final_gpp, na = '', row.names = F, file = gpp_path)

# Upload to GoogleDrive
googledrive::drive_upload(media = gpp_path, overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----