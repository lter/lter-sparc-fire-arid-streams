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
npp_dat <- read_csv("data/fire-arid_npp.csv")
lc_dat <- read_csv("data/fire-arid_land-cover.csv")
pdsi_dat <- read_csv("data/fire-arid_pdsi.csv")
precip_dat <- read_csv("data/fire-arid_precipitation.csv")
pet_dat <- read_csv("data/fire-arid_pet.csv")
temp_dat <- read_csv("data/fire-arid_temperature.csv")

# Load dataset containing sites with chemistry data of interest
# and representative of the full hydrograph (n = 291 sites).
usgs_chem <- readRDS("data_working/usgs_chem_filtered_011924.rds")

# Get site list.
usgs_sites <- unique(usgs_chem$usgs_site)

# PLEASE NOTE - THIS IS NOT THE FULL CHEM DATASET THAT WE WILL
# EVENTUALLY BE WORKING WITH, SO PLOTTING ALL INPUT DATA FOR NOW.

#### Elevation ####

# What is the range in watershed size x elevation?
(fig1 <- ggplot(elev_dat, aes(x = elev_median_m)) +
  geom_histogram(fill = "#3B7D6E", alpha = 0.8) +
  labs(x = "Median Elevation (m)",
       title = "What is the variation in median elevation?") +
  theme_bw())

(fig2 <- ggplot(elev_dat, aes(x = slope_median_deg)) +
    geom_histogram(fill = "#5F5C29", alpha = 0.8) +
    scale_x_log10() + # note the log scale!!
    labs(x = "Median Slope (degrees)",
         title = "What is the variation in median slope?") +
    theme_bw())

# Combine plots.
(fig_elev_slope <- fig1 / fig2)

# Export figure.
# ggsave(plot = fig_elev_slope,
#        filename = "figures/elevation_slope_042224.jpg",
#        width = 15,
#        height = 15,
#        units = "cm",
#        dpi = 200)

#### Land Cover ####

# First, it seems like there are timeseries of land cover? Let's see when those
# measurements are actually recorded.
(fig4 <- ggplot(lc_dat %>%
                  filter(usgs_site == "USGS-06230190"), 
                aes(x = time, y = perc_cover, 
                    color = land_cover, shape = lc_system)) +
    geom_point(alpha = 0.8) +
    scale_y_log10() +
    labs(x = "Date", y = "Percent Cover",
         title = "What is the variation in land cover at site 06230190?") +
    theme_bw())

# Ok, so there are multiple different groups of land cover measurements. We
# should take care to only use one. But measurements are annual.

# Let's see what the categories are for each system.
systems <- lc_dat %>%
  group_by(lc_system, land_cover) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = lc_system, values_from = land_cover) %>%
  select(BGC, IGBP, LAI, PFT, UMD)

lc_grouped <- lc_dat %>%
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
    land_cover %in% c("mixed_forest") ~ "forested")) %>%
  # and need to actually sum and group else the histograms below are messed up
  group_by(usgs_site, lc_system, land_cover_group) %>%
  # made sure to group by system, else it'd be over-representing things
  summarize(mean_annual_perc_cover = mean(perc_cover, na.rm = TRUE)) %>%
  ungroup()

# Make a palette large enough for all land cover types.
my_palette <- cal_palette("chaparral1", n = 10, type = "continuous")

# What is the range in representation of each land cover type?
(fig5 <- ggplot(lc_grouped %>%
                  filter(lc_system == "IGBP"), # choosing one system randomly for now
                aes(x = mean_annual_perc_cover, fill = land_cover_group)) +
    geom_histogram(alpha = 0.8) +
    scale_fill_manual(values = my_palette) +
    #scale_x_log10() +
    #scale_y_log10() +
    labs(x = "Percent Cover", y = "Site Count",
         title = "What is the variation in land cover by site?") +
    theme_bw() +
    facet_wrap(.~land_cover_group, nrow = 2) +
    theme(legend.position = "none"))

(fig6 <- ggplot(lc_grouped %>%
                  filter(lc_system == "IGBP"), 
                aes(x = land_cover_group, y = mean_annual_perc_cover, 
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

# Combine plots.
(fig_lc <- fig5 | fig6)

# Export figure.
# ggsave(plot = fig_lc,
#        filename = "figures/landcover_igbp_042224.jpg",
#        width = 30,
#        height = 15,
#        units = "cm",
#        dpi = 200)

# !!! CAUTION !!!, the below code was made using a past version of this data 
# and may no longer work.
# And now to examine just the 17 sites that were used in model development.
lc_trimtrim <- lc_trim %>%
  filter(usgs_site %in% my17sites) %>%
  filter(lc_system == "IGBP") %>%
  dplyr::group_by(usgs_site, land_cover_group, time) %>%
  summarize(cum_cover = sum(perc_cover)) %>%
  ungroup() %>%
  group_by(usgs_site, land_cover_group) %>%
  summarize(mean_cover = mean(cum_cover)) %>%
  ungroup()

# Join with hastily made delta data from other script.
lc_joined_delta <- left_join(lc_trimtrim, stan_lm_data6_delta, 
                             by = c("usgs_site" = "site"))

(fig6.2 <- ggplot(lc_joined_delta, aes(x = mean_cover, y = delta, 
                             fill = land_cover_group,
                             color = land_cover_group)) +
    geom_point(alpha = 0.8) +
    #scale_y_log10() +
    labs(x = "Percent Cover", y = "Change in CQ Slope",
         title = "What is the variation in changes in slope by land cover?") +
    theme_bw() +
    theme(legend.position = "none") +
    facet_wrap(.~land_cover_group, scales = "free"))

#### GPP ####

# Again, let's take a look at what the frequency of this data is. 
(fig7 <- ggplot(gpp_dat %>%
                  filter(usgs_site == "USGS-08330600"), 
                aes(x = time, y = gpp_kg_C_m2)) +
   geom_point(alpha = 0.8) +
   #scale_y_log10() +
   labs(x = "Date", y = "GPP (Kg C/m2)",
        title = "What is the variation in GPP at site 08330600?") +
   theme_bw())
# Note, these are approximately weekly.

# What is the range in values through time?
# Data Source: MODIS/Terra Gross Primary Productivity 8-Day 
# L4 Global 500 m SIN Grid
# For more info see:
# https://lpdaac.usgs.gov/products/mod17a2hv061/
(fig8 <- ggplot(gpp_dat, aes(x = time, y = gpp_kg_C_m2, group = time)) +
    geom_boxplot(color = "#607345",
                 fill = "#607345",
                 alpha = 0.8) +
    labs(x = "Date",
         y = "Weekly GPP (kg C/m^2)",
         title = "What is the variation in weekly GPP through time?") +
    theme_bw())

# Combine plots.
(fig_gpp <- fig7 / fig8)

# Export figure.
# ggsave(plot = fig_gpp,
#        filename = "figures/gpp_042224.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### NPP ####

# Let's take a look at what the frequency of this data is. 
(fig9 <- ggplot(npp_dat %>%
                  filter(usgs_site == "USGS-09383400"), 
                aes(x = time, y = npp_kg_C_m2_yr)) +
   geom_point(alpha = 0.8) +
   #scale_y_log10() +
   labs(x = "Date", y = "NPP (Kg C/m2 yr)",
        title = "What is the variation in NPP at site 09383400?") +
   theme_bw())
# Note, these are now annual.

# What is the range in values through time?
(fig10 <- ggplot(npp_dat, aes(x = time, y = npp_kg_C_m2_yr, group = time)) +
    geom_boxplot(color = "#609500",
                 fill = "#609500",
                 alpha = 0.8) +
    labs(x = "Date",
         y = "Annual NPP (kg C/m^2 yr)",
         title = "What is the variation in annual NPP through time?") +
    theme_bw())

# Combine plots.
(fig_npp <- fig9 / fig10)

# Export figure.
# ggsave(plot = fig_npp,
#        filename = "figures/npp_042224.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### PDSI ####

# Adding a date column to the PDSI data.
pdsi_dat <- pdsi_dat %>%
  mutate(day = 1) %>%
  mutate(date = make_date(year, month, day))

# Let's take a look at what the frequency of this data is. 
(fig11 <- ggplot(pdsi_dat %>%
                  filter(usgs_site == "USGS-10172914"), 
                aes(x = date, y = ncar_pdsi)) +
   geom_point(alpha = 0.8) +
   #scale_y_log10() +
   labs(x = "Date", y = "PDSI (NCAR)",
        title = "What is the variation in PDSI at site 10172914?") +
   theme_bw())
# Note, these are now monthly.

# What is the range in values through time?
# More positive values are wetter, more negative values are drier
# For more info see:
# https://climatedataguide.ucar.edu/climate-data/palmer-drought-severity-index-pdsi
(fig12 <- ggplot(pdsi_dat, aes(x = date, y = ncar_pdsi, group = date)) +
    geom_boxplot(color = "#ECBD95",
               fill = "#ECBD95",
               alpha = 0.8) +
    labs(x = "Date",
         y = "PDSI (NCAR)",
         title = "What is the variation in PDSI through time?") +
    theme_bw())

# Combine plots.
(fig_pdsi <- fig11 / fig12)

# Export figure.
# ggsave(plot = fig_pdsi,
#        filename = "figures/pdsi_042224.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### PET ####

# Adding a date column to the PDSI data.
pet_dat <- pet_dat %>%
  mutate(day = 1) %>%
  mutate(date = make_date(year, month, day))

# Let's take a look at what the frequency of this data is. 
(fig13 <- ggplot(pet_dat %>%
                   filter(usgs_site == "USGS-13077650"), 
                 aes(x = date, y = pet_kg_m2_8day)) +
    geom_point(alpha = 0.8) +
    scale_y_log10() +
    labs(x = "Date", y = "PET (Kg/m2 8day)",
         title = "What is the variation in PET at site 13077650?") +
    theme_bw())
# Note, these are now roughly weekly (every 8 days).

# What is the range in values through time?
(fig14 <- ggplot(pet_dat, aes(x = date, y = pet_kg_m2_8day, group = date)) +
    geom_boxplot(color = "#6592D6",
                 fill = "#6592D6",
                 alpha = 0.8) +
    labs(x = "Date",
         y = "PET (Kg/m2 8day)",
         title = "What is the variation in PET through time?") +
    theme_bw())

# Combine plots.
(fig_pet <- fig13 / fig14)

# Export figure.
# ggsave(plot = fig_pet,
#        filename = "figures/pet_042224.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

#### Precip ####

precip_dat <- precip_dat %>%
  # make date column based on data available
  mutate(date = parse_date_time(x = paste(year, day), 
                                orders = "yj"))

# Let's take a look at what the frequency of this data is. 
(fig15 <- ggplot(precip_dat %>%
                   filter(usgs_site == "USGS-07311783"), 
                 aes(x = date, y = precip_mm)) +
    geom_point(alpha = 0.8) +
    #scale_y_log10() +
    labs(x = "Date", y = "Precipitation (mm)",
         title = "What is the variation in precip at site 07311783?") +
    theme_bw())
# Note, these are now roughly weekly (every 8 days).

# Daily precipitation proved too challenging to meaningfully plot,
# so choosing to aggregate by year instead.

precip_ann <- precip_dat %>%
  group_by(usgs_site, year) %>%
  summarize(sum_ann_ppt = sum(precip_mm)) %>%
  ungroup()

# What is the range in values through time?
# Data Source: GridMET using watershed shapefiles
# For more info see: https://www.climatologylab.org/gridmet.html
(fig16 <- ggplot(precip_ann, aes(x = year, y = sum_ann_ppt, group = year)) +
    geom_boxplot(color = "#69B9FA",
                 fill = "#69B9FA",
                 alpha = 0.8) +
    labs(x = "Year",
         y = "Cumulative Annual Precipitation (mm)",
         title = "What is the variation in annual Ppt through time?") +
    theme_bw())

# Combine plots.
(fig_ppt <- fig15 / fig16)

# Export figure.
# ggsave(plot = fig_ppt,
#        filename = "figures/ppt_042224.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

# !!! CAUTION !!!, the below code was made using a past version of this data 
# and may no longer work.
# Made annual precip for 17 sites only and joining with delta data.
precip_mean_ann <- precip_trim_ann %>%
  group_by(usgs_site) %>%
  summarize(mean_cum_ann_ppt = mean(sum_ann_ppt)) %>%
  ungroup()

precip_join_delta <- left_join(precip_mean_ann, stan_lm_data6_delta,
                               by = c("usgs_site" = "site"))

(fig12.2 <- ggplot(precip_join_delta, aes(x = mean_cum_ann_ppt, 
                                      y = delta)) +
    geom_point(color = "#4B8FF7",
               alpha = 0.8,
               size = 5) +
    scale_x_log10() +
    labs(x = "Median Cumulative Annual Precipitation (mm)",
         y = "Change in CQ Slope",
         title = "What is the variation in changes in slope by annual Ppt?") +
    theme_bw())

#### Temperature ####

temp_dat <- temp_dat %>%
  mutate(date = make_date(year, month, day)) %>%
  mutate(temp_C = temp_K - 273.15)

# Let's take a look at what the frequency of this data is. 
(fig17 <- ggplot(temp_dat %>%
                   filter(usgs_site == "USGS-06712000"), 
                 aes(x = date, y = temp_K)) +
    geom_point(alpha = 0.8) +
    #scale_y_log10() +
    labs(x = "Date", y = "Temperature (K)",
         title = "What is the variation in temperature at site 06712000?") +
    theme_bw())
# Note, these are now roughly daily.

# Also, we're still getting the weird Kelvin/Celsius stuff...

# Daily temperature proved too challenging to meaningfully plot,
# so choosing to again aggregate by year instead.

temp_ann <- temp_dat %>%
  filter(temp_K > 100) %>% # removing unrealistic values
  group_by(usgs_site, year) %>%
  summarize(ann_mean_temp = mean(temp_C)) %>%
  ungroup()

(fig18 <- ggplot(temp_ann, aes(x = year, y = ann_mean_temp, group = year)) +
    geom_boxplot(color = "#E38377",
                 fill = "#E38377",
                 alpha = 0.8) +
    labs(x = "Year",
         y = "Mean Annual Temperature (Celsius)",
         title = "What is the variation in annual Temp through time?") +
    theme_bw())

# Combine plots.
(fig_temp <- fig17 / fig18)

# Export figure.
# ggsave(plot = fig_temp,
#        filename = "figures/temp_042224.jpg",
#        width = 20,
#        height = 30,
#        units = "cm",
#        dpi = 200)

# Also, we're still getting the weird Kelvin/Celsius stuff...

# End of script.
