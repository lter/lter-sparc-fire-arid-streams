# Initial covariate data inventory
# January 29, 2024
# Heili Lowman

# README: The following script was developed to look into the
# available covariate data for the stream sites included in this
# analysis.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)
library(patchwork)
library(calecopal)

# Load datasets provided by Nick and available on the Google drive here:
# https://drive.google.com/drive/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC
elev_dat <- read_csv("data/fire-arid_elevation.csv")
gpp_dat <- read_csv("data/fire-arid_gpp.csv")
lc_dat <- read_csv("data/fire-arid_land-cover.csv")
pdsi_dat <- read_csv("data/fire-arid_pdsi.csv")
precip_dat <- read_csv("data/fire-arid_precipitation.csv")
temp_dat <- read_csv("data/fire-arid_temperature.csv")

# Load dataset containing sites with chemistry data of interest
# and representative of the full hydrograph (n = 291 sites).
usgs_chem <- readRDS("data_working/usgs_chem_filtered_011924.rds")

# Get site list.
usgs_sites <- unique(usgs_chem$usgs_site)

#### Elevation ####

elev_trim <- elev_dat %>%
  filter(usgs_site %in% usgs_sites)

# What is the range in watershed size x elevation?
(fig1 <- ggplot(elev_trim, aes(x = area_km2, y = elev_median)) +
  # geom_errorbar(color = "#BD973D",
  #               alpha = 0.8,
  #               aes(ymin = elev_min, ymax = elev_max)) +
  geom_point(color = "#3B7D6E", alpha = 0.8) +
  scale_x_log10() +
  labs(x = "Watershed Size (km^2)",
       y = "Median Elevation (m)",
       title = "What is the variation in median elevation by watershed size?") +
  theme_bw())

(fig2 <- ggplot(elev_trim, aes(x = area_km2, y = elev_max)) +
    geom_point(color = "#5F5C29", alpha = 0.8) +
    scale_x_log10() +
    labs(x = "Watershed Size (km^2)",
         y = "Maximum Elevation (m)",
         title = "What is the variation in maximum elevation by watershed size?") +
    theme_bw())

(fig3 <- ggplot(elev_trim, aes(x = area_km2, y = elev_min)) +
    geom_point(color = "#BD973D", alpha = 0.8) +
    scale_x_log10() +
    labs(x = "Watershed Size (km^2)",
         y = "Minimum Elevation (m)",
         title = "What is the variation in minimum elevation by watershed size?") +
    theme_bw())

# Combine plots.
(fig_elev <- fig1 / fig2 / fig3)

# Export figure.
# ggsave(plot = fig_elev,
#        filename = "figures/elevation_wshedsize_012924.jpg",
#        width = 15,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### Land Cover ####

lc_trim <- lc_dat %>%
  filter(usgs_site %in% usgs_sites) %>%
  mutate(land_cover_group = case_when(
    land_cover %in% c("cropland", "cropland_natural_mosaic", "broadleaf_cropland", "cereal_cropland") ~ "agriculture",
    land_cover %in% c("evergreen_needleleaf_forest", "evergreen_needleleaf_vegetation", "evergreen_broadleaf_vegetation", "evergreen_needleleaf_trees", "evergreen_broadleaf_trees") ~ "evergreen",
    land_cover %in% c("deciduous_broadleaf_forest", "deciduous_needleleaf_forest", "deciduous_broadleaf_vegetation", "deciduous_needleleaf_vegetation", "deciduous_broadleaf_trees", "deciduous_needleleaf_trees") ~ "deciduous",
    land_cover %in% c("annual_grass_vegetation", "annual_broadleaf_vegetation" ) ~ "annual",
    land_cover %in% c("closed_shrubland", "open_shrubland", "shrubland", "shrub") ~ "shrubland",
    land_cover %in% c("permanent_wetland", "water_body") ~ "wetland/water",
    land_cover %in% c("woody_savanna", "savanna", "grassland", "grass") ~ "savanna/grassland",
    land_cover %in% c("urban") ~ "urban",
    land_cover %in% c("barren", "snow_and_ice", "non_vegetated", "permanent_snow_and_ice") ~ "barren/frozen",
    land_cover %in% c("mixed_forest") ~ "forested"))

# Make a palette large enough for all land cover types.
my_palette <- cal_palette("chaparral1", n = 10, type = "continuous")

# What is the range in representation of each land cover type?
(fig4 <- ggplot(lc_trim, aes(x = perc_cover, fill = land_cover_group)) +
    geom_histogram(alpha = 0.8) +
    scale_fill_manual(values = my_palette) +
    #scale_x_log10() +
    scale_y_log10() +
    labs(x = "Percent Cover", y = "Site Count",
         title = "What is the variation in land cover by site?") +
    theme_bw() +
    facet_wrap(.~land_cover_group, nrow = 2) +
    theme(legend.position = "none"))

(fig5 <- ggplot(lc_trim, aes(x = land_cover_group, y = perc_cover, 
                             fill = land_cover_group,
                             color = land_cover_group)) +
    geom_boxplot(alpha = 0.8) +
    scale_fill_manual(values = my_palette) +
    scale_color_manual(values = my_palette) +
    scale_y_log10() +
    labs(x = "Land Cover Type", y = "Percent Cover",
         title = "What is the variation in land cover % by site?") +
    theme_bw() +
    theme(legend.position = "none") +
    coord_flip())

# What is the dominant land cover type compared to watershed size?
lc_max <- lc_trim %>%
  group_by(usgs_site) %>%
  filter(perc_cover == max(perc_cover)) %>%
  ungroup()

(fig6 <- ggplot(lc_max, aes(x = area_km2, y = perc_cover, 
                             color = land_cover_group)) +
    geom_point() +
    scale_color_manual(values = cal_palette("chaparral1")) +
    scale_x_log10() +
    labs(x = "Watershed Size (km^2)", y = "Percent Cover",
         title = "What is the dominant land cover by watershed size?",
         color = "Land Cover Type") +
    theme_bw())

# Combine plots.
(fig_lc <- fig5 / fig6)

# Export figure.
# ggsave(plot = fig_lc,
#        filename = "figures/landcover_wshedsize_012924.jpg",
#        width = 18,
#        height = 20,
#        units = "cm",
#        dpi = 200)

#### GPP ####

gpp_trim <- gpp_dat %>%
  filter(usgs_site %in% usgs_sites) %>%
  mutate(date = ymd(time))

# Note, these are approximately weekly.

# What is the range in values through time?
# Data Source: MODIS/Terra Gross Primary Productivity 8-Day 
# L4 Global 500 m SIN Grid
# For more info see:
# https://lpdaac.usgs.gov/products/mod17a2hv061/
(fig7 <- ggplot(gpp_trim, aes(x = date, y = gpp_kg_C_m2,
                              group = date)) +
    geom_boxplot(color = "#607345",
                 fill = "#607345",
                 alpha = 0.8) +
    labs(x = "Date",
         y = "Weekly GPP (kg C/m^2)",
         title = "What is the variation in weekly GPP through time?") +
    theme_bw())

# What is the range in values across sites?
gpp_med <- gpp_trim %>%
  group_by(usgs_site) %>%
  summarize(gpp_median = median(gpp_kg_C_m2)) %>%
  ungroup()

gpp_trim <- left_join(gpp_trim, gpp_med)

(fig8 <- ggplot(gpp_trim %>%
                  mutate(usgs_site = fct_reorder(usgs_site, 
                                                 gpp_median)), 
                aes(x = usgs_site, y = gpp_kg_C_m2)) +
    geom_boxplot(color = "#7F8C72",
                 fill = "#7F8C72",
                 alpha = 0.8) +
    labs(x = "Site",
         y = "Weekly GPP (kg C/m^2)",
         title = "What is the variation in weekly GPP across sites?") +
    theme_bw() +
    theme(axis.text.x = element_blank(),
          axis.ticks.x = element_blank()))

# What is the range in values by watershed size?
(fig8.2 <- ggplot(gpp_trim, aes(x = area_km2, y = gpp_median)) +
    geom_point(color = "#7F8C72",
               alpha = 0.7) +
    scale_x_log10() +
    labs(x = "Watershed Size (km^2)",
         y = "Median Weekly GPP (kg C/m^2)",
         title = "What is the variation in weekly GPP by watershed size?") +
    theme_bw())

# Combine plots.
(fig_gpp <- fig7 / fig8 / fig8.2)

# Export figure.
# ggsave(plot = fig_gpp,
#        filename = "figures/gpp_013124.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### PDSI ####

pdsi_trim <- pdsi_dat %>%
  filter(usgs_site %in% usgs_sites) %>%
  mutate(date = ymd(time))

# What is the range in values through time?
# More positive values are wetter, more negative values are dried
# For more info see:
# https://climatedataguide.ucar.edu/climate-data/palmer-drought-severity-index-pdsi
(fig9 <- ggplot(pdsi_trim, aes(x = date, y = noaa_pdsi)) +
    geom_point(color = "#ECBD95") +
    labs(x = "Date",
         y = "PDSI (NOAA)",
         title = "What is the variation in PDSI through time?") +
    theme_bw())

# Export figure.
# ggsave(plot = fig9,
#        filename = "figures/pdsi_013124.jpg",
#        width = 20,
#        height = 10,
#        units = "cm",
#        dpi = 200)

#### Precip ####

precip_trim <- precip_dat %>%
  filter(usgs_site %in% usgs_sites) %>%
  # make date column based on data available
  mutate(date = parse_date_time(x = paste(year, day), 
                                orders = "yj"))

# Daily precipitation proved too challenging to meaningfully plot,
# so choosing to aggregate by year instead.

precip_trim_ann <- precip_trim %>%
  group_by(usgs_site, area_km2, year) %>%
  summarize(sum_ann_ppt = sum(precip_mm)) %>%
  ungroup()

# What is the range in values through time?
# Data Source: GridMET using watershed shapefiles
# For more info see: https://www.climatologylab.org/gridmet.html
(fig10 <- ggplot(precip_trim_ann, aes(x = year, 
                                      y = sum_ann_ppt,
                                      group = year)) +
    geom_boxplot(color = "#69B9FA",
                 fill = "#69B9FA",
                 alpha = 0.8) +
    labs(x = "Year",
         y = "Cumulative Annual Precipitation (mm)",
         title = "What is the variation in annual Ppt through time?") +
    theme_bw())

# What is the range in values across sites?
ppt_med <- precip_trim_ann %>%
  group_by(usgs_site) %>%
  summarize(ann_ppt_median = median(sum_ann_ppt)) %>%
  ungroup()

precip_trim_ann <- left_join(precip_trim_ann, ppt_med)

(fig11 <- ggplot(precip_trim_ann %>%
                   mutate(usgs_site = fct_reorder(usgs_site, 
                                                  ann_ppt_median)), 
                 aes(x = usgs_site, y = sum_ann_ppt)) +
    geom_boxplot(color = "#59A3F8",
                 fill = "#59A3F8",
                 alpha = 0.8) +
    labs(x = "Site",
         y = "Cumulative Annual Precipitation (mm)",
         title = "What is the variation in annual Ppt across sites?") +
    theme_bw() +
    theme(axis.text.x = element_blank(),
          axis.ticks.x = element_blank()))

# What is the range in values by watershed size?
(fig12 <- ggplot(precip_trim_ann, aes(x = area_km2, 
                                      y = ann_ppt_median)) +
    geom_point(color = "#4B8FF7",
                 alpha = 0.8) +
    scale_x_log10() +
    labs(x = "Watershed Size (km^2)",
         y = "Median Cumulative Annual Precipitation (mm)",
         title = "What is the variation in annual Ppt by watershed size?") +
    theme_bw() +
    theme(axis.text.x = element_blank(),
          axis.ticks.x = element_blank()))

# Combine plots.
(fig_ppt <- fig10 / fig11 / fig12)

# Export figure.
# ggsave(plot = fig_ppt,
#        filename = "figures/ppt_013124.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### Temperature ####

temp_trim <- temp_dat %>%
  filter(usgs_site %in% usgs_sites) %>%
  mutate(date = ymd(time)) %>%
  mutate(temp_C = temp_K - 273.15)

# Daily temperature proved too challenging to meaningfully plot,
# so choosing to again aggregate by year instead.

temp_trim_ann <- temp_trim %>%
  group_by(usgs_site, area_km2, year) %>%
  summarize(ann_mean_temp = mean(temp_C)) %>%
  ungroup()

# Hmmmm...something weird is happening here, it looks like some
# data was pulled in in Kelvin, and the rest in Celsius, so need
# to check with Nick.

"#E7A655", "#E59D7F", "#E38377", "#6D4847"

# End of script.
