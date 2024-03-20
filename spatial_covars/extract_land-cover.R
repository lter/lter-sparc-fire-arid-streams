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

## Land Cover Categories Legend in Data Product User Guide
## Link to PDF:
## https://lpdaac.usgs.gov/documents/101/MCD12_User_Guide_V6.pdf

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

# Define scale factor and fill value
fill_value <- 255

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
    # Filter out fill values
    dplyr::filter(value != fill_value) %>% 
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

# Unlist the output of that loop for easier wrangling
lc_v1 <- purrr::list_rbind(x = out_list)

# Check structure
dplyr::glimpse(lc_v1)

# Perform needed wrangling
lc_v2 <- lc_v1 %>% 
  # Move the site info columns to the left
  dplyr::relocate(usgs_site:area_km2, .before = type) %>% 
  # Clean up layer type column
  dplyr::mutate(type = gsub(pattern = "_[[:digit:]]{1,2}", replacement = "", x = type)) %>% 
  # Get a better variant of the "type" column that is less ambiguous
  dplyr::mutate(lc_system = dplyr::case_when(
    type == "LC_Type1" ~ "IGBP",
    type == "LC_Type2" ~ "UMD",
    type == "LC_Type3" ~ "LAI",
    type == "LC_Type4" ~ "BGC",
    type == "LC_Type5" ~ "PFT",
    T ~ 'x'), .before = type) %>% 
  # Use those to get the actual land cover categories
  dplyr::mutate(land_cover = dplyr::case_when(
    ## International Geosphere-Biosphere Programme (IGBP)
    lc_system == "IGBP" & value == "1" ~ "evergreen_needleleaf_forest",
    lc_system == "IGBP" & value == "2" ~ "evergreen_broadleaf_forest",
    lc_system == "IGBP" & value == "3" ~ "deciduous_needleleaf_forest",
    lc_system == "IGBP" & value == "4" ~ "deciduous_broadleaf_forest",
    lc_system == "IGBP" & value == "5" ~ "mixed_forest",
    lc_system == "IGBP" & value == "6" ~ "closed_shrubland",
    lc_system == "IGBP" & value == "7" ~ "open_shrubland",
    lc_system == "IGBP" & value == "8" ~ "woody_savanna",
    lc_system == "IGBP" & value == "9" ~ "savanna",
    lc_system == "IGBP" & value == "10" ~ "grassland",
    lc_system == "IGBP" & value == "11" ~ "permanent_wetland",
    lc_system == "IGBP" & value == "12" ~ "cropland",
    lc_system == "IGBP" & value == "13" ~ "urban",
    lc_system == "IGBP" & value == "14" ~ "cropland_natural_mosaic",
    lc_system == "IGBP" & value == "15" ~ "snow_and_ice",
    lc_system == "IGBP" & value == "16" ~ "barren",
    lc_system == "IGBP" & value == "17" ~ "water_body",
    ## Annual University of Maryland (UMD)
    lc_system == "UMD" & value == "0" ~ "water_body",
    lc_system == "UMD" & value == "1" ~ "evergreen_needleleaf_forest",
    lc_system == "UMD" & value == "2" ~ "evergreen_broadleaf_forest",
    lc_system == "UMD" & value == "3" ~ "deciduous_needleleaf_forest",
    lc_system == "UMD" & value == "4" ~ "deciduous_broadleaf_forest",
    lc_system == "UMD" & value == "5" ~ "mixed_forest",
    lc_system == "UMD" & value == "6" ~ "closed_shrubland",
    lc_system == "UMD" & value == "7" ~ "open_shrubland",
    lc_system == "UMD" & value == "8" ~ "woody_savanna",
    lc_system == "UMD" & value == "9" ~ "savanna",
    lc_system == "UMD" & value == "10" ~ "grassland",
    lc_system == "UMD" & value == "11" ~ "permanent_wetland",
    lc_system == "UMD" & value == "12" ~ "cropland",
    lc_system == "UMD" & value == "13" ~ "urban",
    lc_system == "UMD" & value == "14" ~ "cropland_natural_mosaic",
    lc_system == "UMD" & value == "15" ~ "non_vegetated",
    ## Leaf Area Index (LAI)
    lc_system == "LAI" & value == "0" ~ "water_body",
    lc_system == "LAI" & value == "1" ~ "grassland",
    lc_system == "LAI" & value == "2" ~ "shrubland",
    lc_system == "LAI" & value == "3" ~ "broadleaf_cropland",
    lc_system == "LAI" & value == "4" ~ "savanna",
    lc_system == "LAI" & value == "5" ~ "evergreen_broadleaf_forest",
    lc_system == "LAI" & value == "6" ~ "deciduous_broadleaf_forest",
    lc_system == "LAI" & value == "7" ~ "evergreen_needleleaf_forest",
    lc_system == "LAI" & value == "8" ~ "deciduous_needleleaf_forest",
    lc_system == "LAI" & value == "9" ~ "non_vegetated",
    lc_system == "LAI" & value == "10" ~ "urban",
    ## BIOME - Biogeochemical Cycles (BGC)
    lc_system == "BGC" & value == "0" ~ "water_body",
    lc_system == "BGC" & value == "1" ~ "evergreen_needleleaf_vegetation",
    lc_system == "BGC" & value == "2" ~ "evergreen_broadleaf_vegetation",
    lc_system == "BGC" & value == "3" ~ "deciduous_needleleaf_vegetation",
    lc_system == "BGC" & value == "4" ~ "deciduous_broadleaf_vegetation",
    lc_system == "BGC" & value == "5" ~ "annual_broadleaf_vegetation",
    lc_system == "BGC" & value == "6" ~ "annual_grass_vegetation",
    lc_system == "BGC" & value == "7" ~ "non_vegetated",
    lc_system == "BGC" & value == "8" ~ "urban",
    ## Plant Functional Types (PFT)
    lc_system == "PFT" & value == "0" ~ "water_body",
    lc_system == "PFT" & value == "1" ~ "evergreen_needleleaf_trees",
    lc_system == "PFT" & value == "2" ~ "evergreen_broadleaf_trees",
    lc_system == "PFT" & value == "3" ~ "deciduous_needleleaf_trees",
    lc_system == "PFT" & value == "4" ~ "deciduous_broadleaf_trees",
    lc_system == "PFT" & value == "5" ~ "shrub",
    lc_system == "PFT" & value == "6" ~ "grass",
    lc_system == "PFT" & value == "7" ~ "cereal_cropland",
    lc_system == "PFT" & value == "8" ~ "broadleaf_cropland",
    lc_system == "PFT" & value == "9" ~ "urban",
    lc_system == "PFT" & value == "10" ~ "permanent_snow_and_ice",
    lc_system == "PFT" & value == "11" ~ "barren",
    T ~ 'x'), .before = value) %>% 
  # Calculate total pixels per LC system
  dplyr::group_by(usgs_site, area_m2, area_km2, time, lc_system) %>% 
  dplyr::mutate(total_pixels = sum(pixel_ct, na.rm = T)) %>% 
  dplyr::ungroup() %>% 
  # Express 'pixel count' as a percent
  dplyr::mutate(perc_cover = (pixel_ct / total_pixels) * 100) %>% 
  # Drop now-superseded columns
  dplyr::select(-type, -value, -pixel_ct, -total_pixels)

# Re-check structure
dplyr::glimpse(lc_v2)

## -------------------------------- ##
            # Export ----
## -------------------------------- ##

# Pick final object name
final_lc <- lc_v2

# Create folder to export to
dir.create(path = file.path(path, "extracted-data"), showWarnings = F)

# Define file path for CSV
lc_path <- file.path(path, "extracted-data", "fire-arid_land-cover.csv")

# Export the summarized data
write.csv(x = final_lc, na = '', row.names = F, file = lc_path)

# Upload to GoogleDrive
googledrive::drive_upload(media = lc_path, overwrite = T,
                          path = googledrive::as_id("https://drive.google.com/drive/u/0/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC"))

# End ----

