## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Extract Air Temperature
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Using the watershed shapefile(s), extract the following data:
## AIR TEMPERATURE (MONTHLY)

# NOTE ON TIMING
## Takes a few minutes when run on NCEAS' Server (Aurora)
## Will likely take longer (possibly *much* longer) on a laptop / desktop computer

## ------------------------------------------------------- ##
                    # Housekeeping ----
## ------------------------------------------------------- ##

# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, sf, ncdf4, stars, terra, exactextractr, 
                 geojsonio, NCEAS/scicomptools, googledrive)

# Silence `dplyr::summarize` preemptively
options(dplyr.summarise.inform = FALSE)

# Clear environment
rm(list = ls())

# Identify path to location of shared data
(path <- scicomptools::wd_loc(local = F, 
                              remote_path = file.path('/', "home", "shares",
                                                      "lter-sparc-fire-arid"),
                              local_path = getwd()))

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

## ------------------------------------------------------- ##
                # Air Temp - Extract ----
## ------------------------------------------------------- ##
# Read in the netCDF file and examine for context on units / etc.
air_nc <- ncdf4::nc_open(filename = file.path(path, "raw-spatial-data", "airtemp",
                                              "air.mon.mean.nc"))

# Look at this
print(air_nc)

# Read it as a raster too
## This format is more easily manipulable for our purposes
air_rast <- terra::rast(x = file.path(path, "raw-spatial-data", "airtemp",
                                      "air.mon.mean.nc"))

# Check names
names(air_rast)

# Check out just one of those
print(air_rast$air_99)

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

## ------------------------------------------------------- ##
                  # Air Temp - Summarize ----
## ------------------------------------------------------- ##
# Unlist that list
full_out_df <- out_list %>%
  purrr::list_rbind() %>%
  # Strip out year and month
  dplyr::mutate(year = stringr::str_sub(string = time, start = 1, end = 4),
                month = stringr::str_sub(string = time, start = 6, end = 7),
                .after = time)

# Glimpse it
dplyr::glimpse(full_out_df)

# Summarize within month across years
year_df <- full_out_df %>%
  # Do summarization
  dplyr::group_by(dplyr::across(dplyr::all_of(c(group_cols, "year")))) %>%
  dplyr::summarize(value = mean(value_avg, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Make more informative year column
  dplyr::mutate(name = paste0("temp_", year, "_degC")) %>%
  # Drop simple year column
  dplyr::select(-year) %>%
  # Pivot to wide format
  tidyr::pivot_wider(names_from = name,
                     values_from = value)

# Glimpse this
dplyr::glimpse(year_df)

# Then summarize within year across months
month_df <- full_out_df %>%
  # Do summarization
  dplyr::group_by(dplyr::across(dplyr::all_of(c(group_cols, "month")))) %>%
  dplyr::summarize(value = mean(value_avg, na.rm = T)) %>%
  dplyr::ungroup() %>%
  # Change month number to letters
  dplyr::mutate(month_simp = dplyr::case_when(
    month == "01" ~ "jan",
    month == "02" ~ "feb",
    month == "03" ~ "mar",
    month == "04" ~ "apr",
    month == "05" ~ "may",
    month == "06" ~ "jun",
    month == "07" ~ "jul",
    month == "08" ~ "aug",
    month == "09" ~ "sep",
    month == "10" ~ "oct",
    month == "11" ~ "nov",
    month == "12" ~ "dec")) %>%
  # Make more informative month column
  dplyr::mutate(name = paste0("temp_", month_simp, "_degC")) %>%
  # Drop simple month column
  dplyr::select(-month, -month_simp) %>%
  # Pivot to wide format
  tidyr::pivot_wider(names_from = name,
                     values_from = value) %>%
  # Reorder months into chronological order
  dplyr::select(dplyr::all_of(group_cols),
                dplyr::contains("_jan_"), dplyr::contains("_feb_"),
                dplyr::contains("_mar_"), dplyr::contains("_apr_"),
                dplyr::contains("_may_"), dplyr::contains("_jun_"),
                dplyr::contains("_jul_"), dplyr::contains("_aug_"),
                dplyr::contains("_sep_"), dplyr::contains("_oct_"),
                dplyr::contains("_nov_"), dplyr::contains("_dec_"))

# Glimpse this
dplyr::glimpse(month_df)

# Combine these dataframes
air_actual <- year_df %>%
  dplyr::left_join(y = month_df, by = group_cols)

# Glimpse again
dplyr::glimpse(air_actual)

# Clean up environment
rm(list = setdiff(ls(), c('path', 'sf_file', 'group_cols', 'air_actual')))

## ------------------------------------------------------- ##
                  # Air Temp - Export ----
## ------------------------------------------------------- ##
# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Export the summarized data
write.csv(x = air_actual, na = '', row.names = F,
          file = file.path(path, "extracted-data", "fire-arid_air-temp.csv"))

# Upload to GoogleDrive
googledrive::drive_upload(media = file.path(path, "extracted-data", "fire-arid_air-temp.csv"),
                          overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# Clean up environment
rm(list = setdiff(ls(), c('path', 'sf_file', 'group_cols', 'air_actual')))

# End ----
