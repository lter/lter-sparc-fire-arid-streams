# Initial discharge inventory
# January 16, 2024
# Heili Lowman

# README: The following script was developed to perform some initial data
# inventorying to determine the approximate discharge distributions at the 
# sites where we might be interested in using data. In short, we want to know
# if the grab samples we are using roughly span the full discharge spectrum
# and are representative of different flow conditions in each stream site.
# Note, due to data volume, here we are using daily mean discharge values.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)
library(stats)

# Load datasets provided by Stevan and available on the Google drive here:
# https://drive.google.com/drive/folders/1waznrSWuL1DiZOvRgbFoDnGNNoqB9Ahv
chem_dat <- readRDS("data/usgs_chemistry.rds")
Q_dat <- readRDS("data/discharge_daily.rds")

#### Data Joining ####

# First, we need to trim the chemistry data down to the analytes of interest.
chem_trim <- chem_dat %>%
  filter(CharacteristicName %in% c("Specific conductance", "Nitrate", "Ammonia and ammonium",
                                   "Orthophosphate", "Potassium", "Calcium",
                                   "Total dissolved solids", "Total suspended solids", "Oxygen"))
# 52% of records remaining

# Now, we need a list of sites to use to trim the discharge data down.
chem_trim_sites <- unique(chem_trim$usgs_site)

# Next, trim the discharge data to only include these sites (just for quicker processing).
Q_trim <- Q_dat %>%
  filter(usgs_site %in% chem_trim_sites)
# Great, removed 7 million records.

# Now, calculate the discharge distribution (quantiles) for each site.
Q_quantiles <- Q_trim %>%
  # keeping only "accepted" measurements
  filter(Flow_cd == "A") %>%
  group_by(usgs_site) %>%
  summarize(quantile_2.5 = quantile(Flow, probs = 0.025, na.rm = TRUE),
            quantile_25 = quantile(Flow, probs = 0.25, na.rm = TRUE),
            quantile_50 = quantile(Flow, probs = 0.5, na.rm = TRUE),
            quantile_75 = quantile(Flow, probs = 0.75, na.rm = TRUE),
            quantile_97.5 = quantile(Flow, probs = 0.975, na.rm = TRUE)) %>%
  ungroup()

# Match available chemistry data with flow data for that day.
chem_trim <- left_join(chem_trim, Q_trim, 
                       # join by site and date only on sample dates that exist in the chem record
                       by = c("usgs_site", "ActivityStartDate" = "Date"))

# And add site quantiles to the dataset for easier comparison.
chem_trim <- left_join(chem_trim, Q_quantiles,
                       by = c("usgs_site"))

# So, a quick look at the data here does show some sites where quantiles are all 0s
# until the top one, so need to discuss that with Tamara and Stevan.

# Add a column explicitly identifying where on a hydrograph each chem sample falls.
chem_trim <- chem_trim %>%
  mutate(quantile_sample = factor(case_when(Flow <= quantile_2.5 ~ "below 2.5th",
                                     Flow > quantile_2.5 & Flow <= quantile_25 ~ "2.5 - 25th",
                                     Flow > quantile_25 & Flow <= quantile_50 ~ "25 - 50th",
                                     Flow > quantile_50 & Flow <= quantile_75 ~ "50 - 75th",
                                     Flow > quantile_75 & Flow <= quantile_97.5 ~ "75 - 97.5th",
                                     Flow > quantile_97.5 ~ "above 97.5th"),
                                  levels = c("below 2.5th", "2.5 - 25th", "25 - 50th",
                                             "50 - 75th", "75 - 97.5th", "above 97.5th")))

# Spot checked a few to make sure this populated correctly, and it seems to have.

#### Examine Q Quantiles ####

# Count discharge quantiles for each analyte.
chem_Q_summarized <- chem_trim %>%
  group_by(usgs_site, CharacteristicName, quantile_sample) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = quantile_sample, values_from = n) %>%
  # reorder just so it's easier to look at
  select(usgs_site, CharacteristicName, 'below 2.5th', '2.5 - 25th', '25 - 50th',
         '50 - 75th', '75 - 97.5th', 'above 97.5th')

# Look at a single analyte, K, alone.
chem_Q_summarized_K <- chem_Q_summarized %>%
  filter(CharacteristicName == "Potassium")

# Hmmmm, ok so it might make sense to impose some criteria re: a minimum of 
# 4 of the 6 "bins" need to be represented.

# If the full spectrum of discharge is to be represented...
chem_Q_summarized_full <- chem_Q_summarized %>%
  drop_na()
# Then 297 of the original 477 sites remain.

# End of script.
