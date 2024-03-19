## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Extract Climate Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the provided shapefile(s), extract the following data:
## TEMPERATURE

# Data Source
## MODIS/Terra Land Surface Temperature/Emissivity Daily L3 Global 1
## https://lpdaac.usgs.gov/products/mod11a1v061/

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
temp_nc <- ncdf4::nc_open(filename = file.path(path, "raw-spatial-data", "modis_temperature", 
                                               "MOD11A1.061_1km_1993_2000.nc"))

# Examine the netCDF example further
print(temp_nc)

# Read it as a raster too (more easily manipulable format)
temp_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "modis_temperature", 
                                       "MOD11A1.061_1km_1993_2000.nc"))

# Check names
names(temp_rast)

# Check out just one of those
print(temp_rast$"LST_Day_1km_51")

# Visual check for overlap
plot(temp_rast$"LST_Day_1km_51", axes = T, reset = F)
plot(sf_file["usgs_site"], axes = T, add = T)

## -------------------------------- ##
             # Extract ----
## -------------------------------- ##

# Define scale factor and fill value
scale_factor <- 0.02
fill_value <- 0

# Create an empty list for storing extracted data
out_list <- list()

# Get list of netCDF files
(temp_spans <- dir(file.path(path, "raw-spatial-data", "modis_temperature")))

# Loop across those
for(span_nc in temp_spans){
  
  # Load in that file
  span_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "modis_temperature", span_nc))
  
  # Identify layers of land surface temp
  span_names <- setdiff(x = names(span_rast), y = paste0("QC_Day_", 1:297))
  
  # Starting message
  message("Beginning land surface temperature extraction within ", span_nc)
  
  # Loop across days
  for(k in 1:length(span_names)){
    
    # Identify focal layer name
    focal_layer <- sort(span_names)[k]
    
    # Strip out the precipitation of that day
    small_out_df <- exactextractr::exact_extract(x = span_rast[[focal_layer]],
                                                 y = sf_file,
                                                 include_cols = group_cols,
                                                 progress = F) %>%
      # Above returns a list so switch it to a dataframe
      purrr::list_rbind(x = .) %>%
      # Filter out NAs
      dplyr::filter(!is.na(value)) %>%
      # Filter out fill values
      dplyr::filter(value != fill_value) %>% 
      # Apply scaling factor
      dplyr::mutate(value_scaled = value * scale_factor) %>% 
      # Summarize across pixels within time
      dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>% 
      dplyr::summarize(value_avg = mean(value_scaled, na.rm = T)) %>% 
      dplyr::ungroup() %>%
      # Add a column timing
      dplyr::mutate(time = terra::time(span_rast[[focal_layer]]),
                    .before = value_avg)
    
    # Add to list
    out_list[[paste0(span_nc, "_", k)]] <- small_out_df
    
    # Success message
    message("Processing complete for layer ", k, " of ", length(span_names)) 
  } # Close day loop
} # Close year loop

## -------------------------------- ##
            # Wrangle ----
## -------------------------------- ##

# Unlist the output of that loop for easier wrangling
temp_v1 <- purrr::list_rbind(x = out_list)

# Check structure
dplyr::glimpse(temp_v1)

# Do needed wrangling
temp_v2 <- temp_v1 %>% 
  # Move the site info columns to the left
  dplyr::relocate(usgs_site:area_km2, .before = time) %>% 
  # Rename extracted information
  dplyr::rename(temp_K = value_avg) %>% 
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
dplyr::glimpse(temp_v2)

## -------------------------------- ##
            # Export ----
## -------------------------------- ##

# Pick final object name
final_temp <- temp_v2

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
temp_path <- file.path(path, "extracted-data", "fire-arid_temperature.csv")

# Export the summarized data
write.csv(x = final_temp, na = '', row.names = F, file = temp_path)

# Upload to GoogleDrive
googledrive::drive_upload(media = temp_path, overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----

