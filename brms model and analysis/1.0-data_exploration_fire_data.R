# Data exploration for fire models

library(tidyverse)
library(ggplot2)

# This sets a global theme for all my plots. 
theme_set(theme_bw() +
            theme(
              plot.background = element_blank()
              ,panel.grid.major = element_blank()
              ,panel.grid.minor = element_blank()
              ,panel.background = element_blank()
              ,panel.border = element_blank()
              ,axis.text.x  = element_text(angle=90, vjust=0.5, size=8)
              ,axis.ticks = element_blank()
              ,strip.background = element_rect()
            ))

# Working directory should be set using Session -> Set Working Directory. Not hard coded. 
# Better practices suggest your file structure look like this:
# .
# └── Project name/
#   ├── data/
#   │   ├── external
#   │   ├── interim
#   │   ├── processed
#   │   └── raw
#   ├── docs
#   ├── models
#   └── reports/
#       ├── images
#       └── graphs

# The data we are looking at has already been processed so it's in the processed folder.

ammonium <- read_csv("data/processed/ammonium_largest_pre_post_covariates.csv", na = c('.','-999','NA'))
problems(ammonium)
ammonium
summary(ammonium)

ammonium %>% ggplot(., aes(x=value_std)) + geom_histogram() +
  ggtitle("Ammonium standardized value")

ammonium %>% ggplot(., aes(x=flow)) + geom_histogram() +
  ggtitle("Ammonium data set flow values")

# Need to scale the data
scaled <- ammonium %>% filter(flow > 0 & value_std > 0) %>% 
  # --- Log-only transforms ---
  mutate(
    "Log flow" = log(flow),           # log Q
    "Log ammonium" = log(value_std)       # log N
  )

colnames(scaled)

scaled %>% ggplot(., aes(x=`Log flow`, y=`Log ammonium`)) + geom_point() +
  ggtitle("Ammonium and river flow data")

scaled %>% ggplot(., aes(x=cum_fire_area, y=`Log ammonium`)) + geom_point() +
  ggtitle("Ammonium and fire area")

scaled %>% ggplot(., aes(x=catch_area, y=`Log ammonium`)) + geom_point() +
  ggtitle("Ammonium and catchment area")

scaled %>% ggplot(., aes(x=cum_per_cent_burned, y=`Log ammonium`)) + geom_point() +
  ggtitle("Ammonium and cumulative percent burned")

scaled %>% ggplot(., aes(x=na_l3name, y=`Log ammonium`)) + geom_boxplot() +
  ggtitle("Ammonium boxplots by ecoregion")

scaled %>% ggplot(., aes(x=avg_median_post_evi, y=`Log ammonium`)) + geom_point() +
  ggtitle("Ammonium by avg median post evi")
