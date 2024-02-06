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
library(readxl)

# Load datasets provided by Stevan and available on the Google drive here:
# https://drive.google.com/drive/folders/1waznrSWuL1DiZOvRgbFoDnGNNoqB9Ahv
chem_dat <- readRDS("data/usgs_chemistry.rds")
q_dat <- readxl::read_excel("data/discharge_daily_statistics.xlsx")

# Also load Erin's data available on the Google drive here:
# https://drive.google.com/drive/folders/1uVfjWees-RrFPP8rK5aeY2mTOoMdAZmB
bell3_dat <- read_csv("data/bell3_streamflow_nitrate_obs_20220127.csv")
bell3_flow <- read_csv("data/bell3_streamflow_obs.csv")
bell4_dat <- read_csv("data/bell4_streamflow_nitrate_obs_20220127.csv")
bell4_flow <- read_csv("data/bell4_streamflow_obs.csv")
bell4_dat_raw <- read_excel("data/Bell4NitrateConcentrations.xlsx", sheet = 1,
                            skip = 3, col_names = c("Watershed", "Year", "Date", "Time", "HydrologicDay", "NO3_mgL"))
bell4_flow_raw <- read_excel("data/SDEFStreamflowDataBell4Estimated.xlsx", sheet = 1,
                            skip = 2, col_names = c("Watershed", "Year", "Date", "Time", "HydrologicDay", "StageHeight_cm", "Discharge_Ls", "Note"))

#### Examine USGS Chemistry Data ####

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

#### Additional Pcode Investigation ####

# Do organic C sites correspond with sites that have UV 254 data?
chem_ocuv <- chem_dat %>%
  filter(CharacteristicName %in% c("Organic carbon", "UV 254")) %>%
  group_by(usgs_site, CharacteristicName) %>%
  summarize(count = n()) %>%
  ungroup()
# So, only 15 of a total of 181 sites have BOTH OC and UV measurements.

# Which sites have Nitrate, Nitrite, and Inorganic Nitrogen?
chem_nox_3 <- chem_dat %>%
  filter(CharacteristicName %in% c("Nitrate", "Nitrite", "Inorganic nitrogen (nitrate and nitrite)")) %>%
  group_by(usgs_site, CharacteristicName) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = CharacteristicName, values_from = count) %>%
  drop_na()
# So, 234 of a total of 308 sites have all three - NO3, NO2, and IN

#### High Frequency Datasets ####

# I will first start by trimming the dataset down to analytes that might likely be high
# frequency data collected by in situ instruments rather than grab samples.
chem_hf <- chem_dat %>%
  filter(CharacteristicName %in% c("Specific conductance", "Oxygen", "Nitrate", "Turbidity")) %>%
  # And I will count the number of observations by day (first pass at a h.f. measurement) 
  group_by(usgs_site, CharacteristicName, ActivityStartDate) %>%
  summarize(count = n()) %>%
  ungroup()

# Roughly hourly measurements are likely needed for high-frequency analyses
sites_hf <- chem_hf %>%
  filter(count > 20) # in case a few hours' observation is missing

length(unique(sites_hf$usgs_site)) # 30 sites with potentially high-frequency data
# but none are super long - usually only a few days here and there

# Only exception seems to be Rio Grande

# Trying this another way.
chem_hf_sc <- chem_hf %>%
  filter(CharacteristicName == "Specific conductance")

ggplot(chem_hf_sc, aes(x = ActivityStartDate, y = usgs_site)) + 
  geom_point() + 
  theme_bw()

#### Examine Erin's Data ####

# This includes only NO3 data so far from the San Dimas watersheds.

bell3_dat <- bell3_dat %>%
  mutate(date = mdy(date))

(fig_b3 <- ggplot(bell3_dat, aes(x = date, y = nitrate)) +
    geom_point(alpha = 0.8) +
    labs(x = "Date", y = "Nitrate", title = "Bell 3") +
    theme_bw())

(fig_b3_flow <- ggplot(bell3_dat, aes(x = date, y = streamflow_mm)) +
    geom_point(color = "cornflowerblue") +
    labs(x = "Date", y = "Flow") +
    theme_bw())

bell4_dat <- bell4_dat %>%
  mutate(date = mdy(date))

(fig_b4 <- ggplot(bell4_dat, aes(x = date, y = nitrate)) +
    geom_point(alpha = 0.8) +
    labs(x = "Date", y = "Nitrate", title = "Bell 4") +
    theme_bw())

(fig_b4_flow <- ggplot(bell4_dat, aes(x = date, y = streamflow_mm)) +
    geom_point(color = "cornflowerblue") +
    labs(x = "Date", y = "Flow") +
    theme_bw())

(fig_bell_all <- (fig_b3 + fig_b4) / (fig_b3_flow + fig_b4_flow))

# Examine the raw data for Bell 4 NO3 as well.

bell4_dat_raw_trim <- bell4_dat_raw %>%
  # Format date properly.
  mutate(month = month(Date),
         day = day(Date)) %>%
  mutate(date = make_date(year = Year, month = month, day = day)) %>%
  # and remove N/As (argh!)
  filter(NO3_mgL != "N/A") %>%
  mutate(NO3_mgl = as.numeric(NO3_mgL))

(fig_b4_raw <- ggplot(bell4_dat_raw_trim, 
                      aes(x = date, y = NO3_mgl)) +
    geom_point(alpha = 0.8) +
    labs(x = "Date", y = "Nitrate (mg/L)", title = "Bell 4") +
    theme_bw())

bell4_flow_raw_trim <- bell4_flow_raw %>%
  # Format date properly.
  mutate(month = month(Date),
         day = day(Date)) %>%
  mutate(date = make_date(year = Year, 
                          month = month, 
                          day = day))

(fig_b4_flow_raw <- ggplot(bell4_flow_raw_trim, aes(x = date, y = Discharge_Ls)) +
    geom_point(color = "cornflowerblue") +
    labs(x = "Date", y = "Q (L/sec)") +
    theme_bw())

# ohhhhh wait, there's 30 different sheets in this file :(
  
# End of script.
