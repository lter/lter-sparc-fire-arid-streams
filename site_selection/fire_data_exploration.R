# Initial fire data inventory
# January 19, 2024
# Heili Lowman

# README: The following script was developed to look into the
# available MTSB fire data for the stream sites included in this
# analysis.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)
library(patchwork)
library(calecopal)

# Load datasets provided by Stevan and available on the Google drive here:
# https://drive.google.com/drive/folders/1waznrSWuL1DiZOvRgbFoDnGNNoqB9Ahv
fire_dat <- readxl::read_excel("data/dd_area_stats.xlsx") # 16,000 fires wow!!
usgs_sites <- readRDS("data_working/usgs_sites_filtered_011924.rds")

#### Filter Data ####

length(unique(fire_dat$usgs_site)) # 753 sites

# Filtering down the fire data for only sites of interest.
fire_trim <- fire_dat %>%
  filter(usgs_site %in% usgs_sites)  %>%
  # Ok, down to 7k fires now.
  mutate(Julian_date = yday(ignition_date)) 
  # and adding column for day of the year

#### Visualize Data ####

# When have fires taken place through time?
(fig1 <- ggplot(fire_trim, aes(x = ignition_date)) +
  geom_histogram(alpha = 0.8, fill = "#2C1B21") +
  labs(x = "Date",
       y = "Count",
       title = "When have fires taken place through time?") +
  theme_bw())

# When during the year do fires start?
(fig2 <- ggplot(fire_trim, aes(x = Julian_date)) +
    geom_histogram(alpha = 0.8, fill = "#E60505") +
    labs(x = "Ignition DOY",
         y = "Count",
         title = "When during the year do fires start?") +
    theme_bw())

# How large are fires?
(fig3 <- ggplot(fire_trim, aes(x = fire_area)) +
    geom_histogram(alpha = 0.8, fill = "#F66C09") +
    scale_x_log10() +
    labs(x = "Fire area (km^2)",
         y = "Count",
         title = "How large are fires?") +
    theme_bw())

# Are fires getting larger over time?
(fig4 <- ggplot(fire_trim, aes(y = fire_area, x = ignition_date)) +
    geom_point(alpha = 0.8, color = "#F66C09") +
    scale_y_log10() +
    labs(x = "Date",
         y = "Fire area (km^2)",
         title = "Are fires getting larger?") +
    theme_bw())

# What percentage of a given watershed burns typically?
(fig5 <- ggplot(fire_trim, aes(x = per_cent_burned)) +
    geom_histogram(alpha = 0.9, fill = "#FEEC44") +
    scale_x_log10() +
    labs(x = "% Watershed Burned",
         y = "Count",
         title = "What percentage of a watershed burns?") +
    theme_bw())

fire_occ <- fire_trim %>%
  group_by(usgs_site) %>%
  summarize(n_fire = n()) %>%
  ungroup()

# How many fires occur in a single watershed?
(fig6 <- ggplot(fire_occ, aes(x = n_fire)) +
    geom_histogram(alpha = 0.8, fill = "#B77B7B") +
    scale_x_log10() +
    labs(x = "# of Fires",
         y = "Count",
         title = "How many fires occur in a watershed?") +
    theme_bw())
# Gila River in AZ has had 436 fires! It's also an 18,000 sq mi watershed.

# Combine these figures.
(full_fig <- fig1 / fig2 / fig3 / fig4 / fig5 / fig6)

# Export figure.
# ggsave(plot = full_fig,
#        filename = "figures/mtbs_fires_011924.jpg",
#        width = 20,
#        height = 40,
#        units = "cm",
#        dpi = 200)

#### Examine Data ####

# How many instances are there of multiple fires happening in the same
# watershed on the same date (but different fires)?

fire_multiples <- fire_trim %>%
  group_by(usgs_site, ignition_date) %>%
  summarize(fires = n()) %>%
  ungroup()

ggplot(fire_multiples, aes(x = fires)) +
  geom_bar(stat = "count") +
  labs(x = "Unique Fires",
       title = "# of fires in a watershed beginning with same ignition date") +
  scale_y_log10() +
  theme_bw()

# What is the frequency of two fires that burn >10% of the watershed back to back?
fire_large <- fire_trim %>%
  filter(per_cent_burned >= 10) # only 37 instances of fires that large

length(unique(fire_large$usgs_site)) # 36 unique sites
# and the duplicate appears to be the same fire, so
# there are NO INSTANCES of large, back-to-back fires in this dataset.

# What does the data spread of fire size look like, if we select only for the largest
# fire in each catchment?
fire_large_each <- fire_trim %>%
  group_by(usgs_site) %>%
  slice_max(per_cent_burned) %>%
  ungroup()

ggplot(fire_large_each, aes(x = per_cent_burned)) +
  geom_histogram(fill = "coral", alpha = 0.8) +
  scale_x_log10() +
  theme_bw()
# ok, so little fires everywhere again
# but appears over half would burn >1%

# End of script.
