# Initial USGS query inventory
# January 8, 2024
# Heili Lowman

# README: The following script was developed to perform some initial data
# visualization and inventorying to inform (1) how many years of pre-/post-
# fire data is available at most USGS sites with which we can perform the
# planned CQ analyses and (2) how many sites might have sufficient data
# (duration + frequency) with which to expand the MARSS analysis framework.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)
library(patchwork)
library(calecopal)

# Load datasets provided by Stevan and available on the Google drive here:
# https://drive.google.com/drive/folders/1waznrSWuL1DiZOvRgbFoDnGNNoqB9Ahv
chem_dat <- readRDS("data/usgs_chemistry.rds")
q_dat <- readxl::read_excel("data/discharge_daily_statistics.xlsx")

#### Examine Chemistry Data ####

names(chem_dat) # Prints column names.
length(unique(chem_dat$usgs_site)) # 477 sites identified
length(unique(chem_dat$CharacteristicName)) # 34 analytes identified
length(unique(chem_dat$USGSPCode)) # 111 unique identifiers for said analytes

# How many records, grouping by both pcode and analyte.
chem_analyte_p <- chem_dat %>%
  group_by(CharacteristicName, USGSPCode) %>%
  summarize(count = n()) %>%
  ungroup()

# Make color palette
water_palette <- cal_palette(name = "sbchannel", n = 111, type = "continuous")

(fig1 <- ggplot(chem_analyte_p, aes(fill = USGSPCode,
                                    y = count,
                                    x = CharacteristicName)) +
    geom_bar(position = "stack", stat = "identity", alpha = 0.8) +
    scale_fill_manual(values = water_palette) +
    coord_flip() +
    labs(y = "Count", x = "Analyte") +
    theme_bw())

# Export figure.
# ggsave(plot = fig1, 
#        filename = "figures/usgs_sites_pcodes_010824.jpg",
#        width = 40,
#        height = 20,
#        units = "cm",
#        dpi = 300)

# How many records per site grouping by analyte.
chem_site_analyte <- chem_dat %>%
  group_by(usgs_site, CharacteristicName) %>%
  summarize(count = n()) %>%
  ungroup()

chem_avg_analyte_site <- chem_site_analyte %>%
  group_by(CharacteristicName) %>%
  summarize(mean_per_site = mean(count)) %>%
  ungroup()

(fig2 <- ggplot(chem_avg_analyte_site, aes(y = mean_per_site,
                                    x = CharacteristicName)) +
    geom_bar(stat = "identity") +
    coord_flip() +
    labs(y = "Mean Count per Site", x = "Analyte") +
    theme(legend.position = "none"))

# What annual frequency are these data at
chem_ann_analyte_site <- chem_dat %>%
  mutate(year = year(ActivityStartDate)) %>%
  group_by(usgs_site, year, CharacteristicName) %>%
  summarize(count = n()) %>%
  ungroup()

chem_avg_ann_analyte_site <- chem_ann_analyte_site %>%
  group_by(CharacteristicName) %>%
  summarize(mean_per_site = mean(count)) %>%
  ungroup()

(fig3 <- ggplot(chem_avg_ann_analyte_site, aes(y = mean_per_site,
                                           x = CharacteristicName)) +
    geom_bar(stat = "identity", fill = "black", alpha = 0.8) +
    coord_flip() +
    labs(y = "Mean Count per Site per Year", x = "Analyte") +
    geom_hline(yintercept = 12) +
    theme(legend.position = "none") +
    theme_bw())

# Export figure.
# ggsave(plot = fig3, 
#        filename = "figures/usgs_annual_chem_010824.jpg",
#        width = 30,
#        height = 20,
#        units = "cm",
#        dpi = 300)

#### Examine Discharge/Fire Data ####

names(q_dat) # Prints column names
length(unique(q_dat$usgs_site)) # 478 sites identified
length(unique(q_dat$event_id)) # 2,311 fires identified

# So that works out to be roughly 5 fires per site.

mean(q_dat$days_q_pre_fire) # avg of 7899 days/21 years pre-fire data
mean(q_dat$days_q_post_fire) # avg of 5690 days/16 years post-fire data

#### Join Chem and Q Data ####

# Need to join these datasets together to determine the quantity of
# pre- and post-fire chemistry data available for each site. Need
# to take care in joining since most sites experience multiple fires.

# Trimming only for columns pertaining to the fire events
q_dat_fires <- q_dat[,1:3]

# Trying this on a smaller dataset first to see if it works.
q_dat_fires1 <- q_dat_fires %>%
  filter(usgs_site == "USGS-06620000") # 6 fires

chem_dat1 <- chem_dat %>%
  filter(usgs_site == "USGS-06620000") # 1,247 records

# Join with chemistry dataset
# This will lengthen every chemistry record the # of times there are fires
chem_dat_fires1 <- full_join(chem_dat1, q_dat_fires1,
                             relationship = "many-to-many") %>%
  # and create a new column to delineate relationship to fire
  mutate(rel_to_fire = factor(case_when(ActivityStartDate > ignition_date ~ "After",
                                      TRUE ~ "Before"),
                            levels = c("Before", "After"))) %>%
  # and then count the records for each site and analyte pre- and post-fire
  group_by(usgs_site, event_id, ignition_date, CharacteristicName, rel_to_fire) %>%
  summarize(records_rel_fire = n()) %>%
  ungroup() %>%
  # And need to pivot wider so that before and after values are in separate columns,
  # similar to how the discharge data is displayed
  pivot_wider(names_from = "rel_to_fire", values_from = "records_rel_fire") %>%
  rename(records_pre_fire = Before,
         records_post_fire = After)

# And let's quickly plot alkalinity, to verify the dates of the fires and chem data
# make sense.
(fig4 <- ggplot(chem_dat1, aes(x = ActivityStartDate, y = ResultMeasureValue)) +
    geom_point(size = 1, color = "black", alpha = 0.8) +
    geom_vline(xintercept = as_date("2020-10-14"), color = "red") + # fire #1
    geom_vline(xintercept = as_date("2020-09-06"), color = "red") + # fire #2
    geom_vline(xintercept = as_date("2002-08-12"), color = "red") + # fire #3
    geom_vline(xintercept = as_date("1988-09-07"), color = "red") + # fire #4
    geom_vline(xintercept = as_date("2016-06-19"), color = "red") + # fire #5
    geom_vline(xintercept = as_date("2020-09-17"), color = "red") + # fire #6
    labs(x = "Date", y = "Alkalinity") +
    xlim(as_date("1980-01-01"), as_date("2023-12-31")))

# Ok, now run for full chemistry dataset..
# This will lengthen every chemistry record the # of times there are fires
chem_dat_fires <- full_join(chem_dat, q_dat_fires,
                             relationship = "many-to-many") %>%
  # and create a new column to delineate relationship to fire
  mutate(rel_to_fire = factor(case_when(ActivityStartDate > ignition_date ~ "After",
                                        TRUE ~ "Before"),
                              levels = c("Before", "After"))) %>%
  # and then count the records for each site and analyte pre- and post-fire
  group_by(usgs_site, event_id, ignition_date, CharacteristicName, rel_to_fire) %>%
  summarize(records_rel_fire = n()) %>%
  ungroup() %>%
  # And need to pivot wider so that before and after values are in separate columns,
  # similar to how the discharge data is displayed
  pivot_wider(names_from = "rel_to_fire", values_from = "records_rel_fire") %>%
  rename(records_pre_fire = Before,
         records_post_fire = After)

# Export so as not to lose progress.
# saveRDS(chem_dat_fires, "data_working/usgs_chemistry_daily_statistics_010824.rds")

#### Examine Pre-/Post-Fire Data ####

# Replace all NAs with zeros.
chem_dat_fires$records_pre_fire <- chem_dat_fires$records_pre_fire %>% replace_na(0)
chem_dat_fires$records_post_fire <- chem_dat_fires$records_post_fire %>% replace_na(0)

# How much pre and post fire data is there on average by analyte per fire event?
chem_avg_dat_fires <- chem_dat_fires %>%
  group_by(CharacteristicName) %>%
  summarize(mean_records_pre_fire = mean(records_pre_fire),
            mean_records_post_fire = mean(records_post_fire)) %>%
  ungroup()

(fig5 <- ggplot(chem_avg_dat_fires, aes(y = mean_records_pre_fire,
                                        x = CharacteristicName)) +
    geom_bar(stat = "identity") +
    ylim(0, 325) +
    coord_flip() +
    labs(x = "Mean Pre-Fire Records per Event", y = "Analyte") +
    theme(legend.position = "none"))

(fig6 <- ggplot(chem_avg_dat_fires, aes(y = mean_records_post_fire,
                                        x = CharacteristicName)) +
    geom_bar(stat = "identity") +
    ylim(0, 325) +
    coord_flip() +
    labs(x = "Mean Post-Fire Records per Event", y = "Analyte") +
    theme(legend.position = "none", axis.text.y = element_blank()))

(fig56 <- fig5 + fig6)

# Say we were to want roughly three years of monthly data (n ~ 36) on either side of an event.
# How many would that encompass in our dataset.
chem_dat_fires_n36 <- chem_dat_fires %>%
  filter(records_pre_fire >= 36) %>% # removes 87,366 records
  filter(records_post_fire >= 36) # removes another 83542 records

length(unique(chem_dat_fires_n36$usgs_site)) # 266 sites remaining
length(unique(chem_dat_fires_n36$event_id)) # 1,669 fires remaining

# Now, to examine more closely how these records appear for individual analytes.
chem_list <- c("Ammonia and ammonium", "Nitrate", "Oxygen",
               "Specific conductance", "Total dissolved solids",
               "Potassium", "Organic Carbon")

# And in the figures below, I will filter out events for which there is either
# no pre-fore or no post-fire data.
(fig7.1 <- ggplot(chem_dat_fires %>% 
                    filter(records_pre_fire > 0) %>%
                    filter(records_post_fire > 0) %>%
                    filter(CharacteristicName %in% chem_list),
                  aes(records_pre_fire)) +
    geom_histogram(fill = "#7E8C69", alpha = 0.8) +
    scale_x_log10() + 
    labs(x = "Records pre-fire",
         y = "Count") +
    facet_grid(CharacteristicName~.) +
    theme_bw())

(fig7.2 <- ggplot(chem_dat_fires %>% 
                    filter(records_pre_fire > 0) %>%
                    filter(records_post_fire > 0) %>% 
                    filter(CharacteristicName %in% chem_list),
                  aes(records_post_fire)) +
    geom_histogram(fill = "#F28023", alpha = 0.8) +
    scale_x_log10() + 
    labs(x = "Records post-fire",
         y = "Count") +
    facet_grid(CharacteristicName~.) +
    theme_bw())

(fig7 <- fig7.1 + fig7.2)

# Export figure.
# ggsave(plot = fig7, 
#        filename = "figures/usgs_pre_post_chem_select_010824.jpg",
#        width = 40,
#        height = 25,
#        units = "cm",
#        dpi = 300)
  
# End of script.
