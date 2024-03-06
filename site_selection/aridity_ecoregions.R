#' @title Inventory aridity index, ecoregions, and a combination of the two to
#' inform site selection
#'
#' @description This is a workflow to identify regions of western north America
#' characterized by a particular aridity index, ecoregion, and/or combination
#' of the two to inform selection of study catchments for the analysis of the
#' effects of wildfire on aquatic ecosystems in aridlands.

#' @note A blog post by Luis D. Verde Arregoitia was helpful to developing the
#' workflow to calculate summary raster values within polygons:
#' https://luisdva.github.io/rstats/GIS-with-R/


# DATA: Global Aridity Index and Potential Evapotranspiration (ET0) Database:
# Version 6

# data in drive: https://drive.google.com/file/d/17qE6ecWVqJPEsnUN0Yyq7MOxIZfCzG0C/view?usp=drive_link
# data source: https://figshare.com/articles/dataset/Global_Aridity_Index_and_Potential_Evapotranspiration_ET0_Climate_Database_v2/7504448/6

global_ai <- terra::rast("~/Desktop/Global-AI_ET0_v3_annual/ai_v3_yr.tif")

## crop to north America
global_ai_crop <- terra::crop(global_ai, raster::extent(c(-125, -70, +20, 71.44167)))

## ignore very small values
global_ai_crop <- terra::clamp(global_ai_crop, lower = 0.001, values = FALSE)


# DATA: ecoregions (here using north America level III)

ecoregions <- sf::st_read(
  dsn   = "path-to-directory",
  layer = "NA_CEC_Eco_Level3"
)

ecoregions <- ecoregions |> 
  sf::st_make_valid(ecoregions) |> 
  sf::st_transform(crs = 4326)


# FILTER: identify ecoregions with a particular AI, here median < 2000

arid_ecos <- ecoregions |> 
  dplyr::mutate(
    ai_mode        = exactextractr::exact_extract(global_ai_crop, ecoregions, "mode"),
    ai_mean        = exactextractr::exact_extract(global_ai_crop, ecoregions, "mean"),
    ai_mean_narm   = exactextractr::exact_extract(global_ai_crop, ecoregions, function(values, coverage_fraction) mean(values, na.rm = "TRUE")),
    ai_median      = exactextractr::exact_extract(global_ai_crop, ecoregions, "median"),
    ai_median_narm = exactextractr::exact_extract(global_ai_crop, ecoregions, function(values, coverage_fraction) median(values, na.rm = "TRUE"))
  ) |> 
  dplyr::filter(ai_median <= 2000)


# PLOT

global_ai_df <- terra::as.data.frame(
  x  = global_ai_crop,
  xy = TRUE
) |>
  dplyr::rename(ai = awi_pm_sr_yr)

ggplot2::ggplot() +
  ggplot2::geom_raster(
    data    = global_ai_df,
    mapping = ggplot2::aes(
      x    = x,
      y    = y,
      fill = ai
    )
  ) +
  ggplot2::geom_sf(
    mapping = ggplot2::aes(colour = NA_L3NAME),
    data    = arid_ecos,
    # inherit.aes = FALSE
  )


# WRITE

## raster

terra::writeRaster(global_ai_crop, "~/Desktop/ai_crop.tif")

## json

#' @note The study will focus on ecoregions with a median aridity index of 2000
#' (#L50). However, this cutoff excluded the Arizona/New Mexico Mountains
#' (NA_L3NAME). Because of particular relevance of that ecoregion to this
#' study, it was also included in the list of ecoregions that define the area
#' of study (#L91-95).

aridland_ecoregions <- dplyr::bind_rows(
  arid_ecos,
  ecoregions |> 
    dplyr::filter(NA_L3CODE == "13.1.1")
)

sf::st_write(
  obj        = aridland_ecoregions,
  dsn        = "~/Desktop/aridland_ecoregions.geojson",
  driver     = "geojson",
  delete_dsn = TRUE
)
