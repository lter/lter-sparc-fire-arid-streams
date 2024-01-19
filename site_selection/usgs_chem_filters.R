# USGS Chemistry Filtering
# January 19, 2024
# Heili Lowman

# README: The following script will perform the necessary filtering
# steps based on analytes of interest and the hydrograph spanned to
# select the sites with which we will proceed with gathering additional
# data for.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)

# Load datasets provided by Stevan and available on the Google drive here:
# https://drive.google.com/drive/folders/1waznrSWuL1DiZOvRgbFoDnGNNoqB9Ahv
chem_dat <- readRDS("data/usgs_chemistry.rds")
Q_dat <- readRDS("data/discharge_daily.rds")

#### Filter by chem ####

# First, we need to select for the analytes of interest.
chem_list <- c("Ammonia and ammonium", "Nitrate", "Nitrite", "Orthophosphate",
               "Specific conductance", "Oxygen", "Total suspended solids",
               "Potassium", "Calcium", "Organic carbon")

chem_trim <- chem_dat %>%
  # filter for desired analytes above
  filter(CharacteristicName %in% chem_list)
  # removes 438,031 observations, no sites (n = 477)

# Next, we need to determine what each of these codes are and if they are even appropriate for our analysis.
unique(chem_trim$ActivityMediaName) # Ok, all are "Water" which is what we want
unique(chem_trim$USGSPCode) # See definitions below.

# Code look-ups available via the following sites:
# https://help.waterdata.usgs.gov/parameter_cd?group_cd=INM
# https://help.waterdata.usgs.gov/parameter_cd?group_cd=NUT
# https://help.waterdata.usgs.gov/parameter_cd?group_cd=PHY
# https://help.waterdata.usgs.gov/parameter_cd?group_cd=INN
# https://help.waterdata.usgs.gov/parameter_cd?group_cd=%

# "00915" - Calcium, water, filtered, milligrams per liter (dissolved)
# "00671" - Orthophosphate, water, filtered, milligrams per liter as phosphorus (dissolved)
# "00680" - Organic carbon, water, unfiltered, milligrams per liter
# "00095" - Specific conductance, water, unfiltered, microsiemens per centimeter at 25 degrees Celsius (total)
# "00935" - Potassium, water, filtered, milligrams per liter (dissolved)
# "00681" - Organic carbon, water, filtered, milligrams per liter (dissolved)
# "00620" - Nitrate, water, unfiltered, milligrams per liter as nitrogen (total)
# "00618" - Nitrate, water, filtered, milligrams per liter as nitrogen (dissolved)
# "00615" - Nitrite, water, unfiltered, milligrams per liter as nitrogen (total)
# "00660" - Orthophosphate, water, filtered, milligrams per liter as PO4 (dissolved)
# "70507" - Orthophosphate, water, unfiltered, milligrams per liter as phosphorus (total)
# "00300" - Dissolved oxygen, water, unfiltered, milligrams per liter (dissolved)
# "00301" - Dissolved oxygen, water, unfiltered, percent of saturation (dissolved)
# "90095" - Specific conductance, water, unfiltered, laboratory, microsiemens per centimeter at 25 degrees Celsius (total)
# "71845" - Ammonia (NH3 + NH4+), water, unfiltered, milligrams per liter as NH4 (total)
# "71846" - Ammonia (NH3 + NH4+), water, filtered, milligrams per liter as NH4 (dissolved)
# "71851" - Nitrate, water, filtered, milligrams per liter as nitrate (dissolved)
# "00610" - Ammonia (NH3 + NH4+), water, unfiltered, milligrams per liter as nitrogen (total)
# "00613" - Nitrite, water, filtered, milligrams per liter as nitrogen (dissolved)
# "71856" - Nitrite, water, filtered, milligrams per liter as nitrite (dissolved)
# "00608" - Ammonia (NH3 + NH4+), water, filtered, milligrams per liter as nitrogen (dissolved)
# "00530" - Suspended solids, water, unfiltered, milligrams per liter (non-filterable)
# "70299" - Suspended solids dried at 110 degrees Celsius, water, unfiltered, milligrams per liter (suspended)
# "00916" - Calcium, water, unfiltered, recoverable, milligrams per liter (recoverable)
# "00937" - Potassium, water, unfiltered, recoverable, milligrams per liter (recoverable)
# "81357" - Calcium, suspended sediment, milligrams per liter (suspended)
# "00910" - Calcium, water, unfiltered, milligrams per liter as calcium carbonate (total)
# "00094" - Specific conductance, water, unfiltered, field, microsiemens per centimeter at 25 degrees Celsius (total)
# "91003" - Nitrate, water, filtered, micrograms per liter as nitrate (dissolved)
# "91004" - Orthophosphate, water, filtered, micrograms per liter as phosphorus (dissolved)
# "82938" - Calcium, wet atmospheric deposition, unfiltered, recoverable, milligrams per liter (recoverable)

# Removing unfiltered dissolved analytes and "total" measures for most.
chem_desired <- c("00915", # Calcium
                  "00671", "00660", "91004", # Orthophosphate
                  "00095", "90095", "00094", # Specific Conductance
                  "00935", # Potassium
                  "00681", # Organic Carbon
                  "00618", "71851", "00613", "71856", "91003", # Nitrate or Nitrite
                  "00300", "00301", # Dissolved Oxygen
                  "71846", "00608", # Ammonia (NH4+NH3) 
                  "00530", "70299") # Suspended Solids

chem_trim_codes <- chem_trim %>%
  filter(USGSPCode %in% chem_desired)
  # removes 21,042 observations, no sites (n = 477)

#### Filter by Q ####

# List of sites to use to trim the discharge data down.
chem_trim_sites <- unique(chem_trim_codes$usgs_site)

# Trim the discharge data to only include these sites.
Q_trim <- Q_dat %>%
  filter(usgs_site %in% chem_trim_sites)
# Great, removed ~7 million records.

# Calculate the discharge distributions for each site.
Q_quantiles <- Q_trim %>%
  # keeping only "accepted" measurements
  filter(Flow_cd %in% c("A", "A e")) %>%
  group_by(usgs_site) %>%
  # default is 7 digits for quantile()
  summarize(quantile_2.5 = quantile(Flow, probs = 0.025, na.rm = TRUE),
            quantile_25 = quantile(Flow, probs = 0.25, na.rm = TRUE),
            quantile_50 = quantile(Flow, probs = 0.5, na.rm = TRUE),
            quantile_75 = quantile(Flow, probs = 0.75, na.rm = TRUE),
            quantile_97.5 = quantile(Flow, probs = 0.975, na.rm = TRUE)) %>%
  ungroup()

# Match available chemistry data with flow data for that day.
chem_trim_codes <- left_join(chem_trim_codes, Q_trim, 
                       # join by site and date only on sample dates that exist in the chem record
                       by = c("usgs_site", "ActivityStartDate" = "Date"))

# Examine individual samples.
chem_ind <- chem_trim_codes %>%
  select(usgs_site, ActivityStartDate, CharacteristicName, USGSPCode, 
         ResultMeasureValue, Flow, Flow_cd)

# There appear to be a lot of NAs, so need to examine those.
chem_Q_NA <- chem_ind %>%
  filter(is.na(Flow)) 
# Appears to be about 5% of records (n = 15,223)

length(unique(chem_Q_NA$usgs_site)) # gah, this happens at 146 sites so this is fairly prevalent
# So, I'll just need to remove the NAs a.k.a. days on which we have chem but no Q data
chem_trim_codes_noQna <- chem_trim_codes %>%
  filter(!is.na(Flow))

# Add site quantiles to the dataset.
chem_trim_codes_quants <- left_join(chem_trim_codes_noQna, Q_quantiles,
                       by = c("usgs_site")) %>%
  # and add column explicitly identifying where on hydrograph that sample falls
  mutate(quantile_sample = factor(case_when(Flow <= quantile_2.5 ~ "below 2.5th",
                                      Flow > quantile_2.5 & Flow <= quantile_25 ~ "2.5 - 25th",
                                      Flow > quantile_25 & Flow <= quantile_50 ~ "25 - 50th",
                                      Flow > quantile_50 & Flow <= quantile_75 ~ "50 - 75th",
                                      Flow > quantile_75 & Flow <= quantile_97.5 ~ "75 - 97.5th",
                                      Flow > quantile_97.5 ~ "above 97.5th"),
                                  levels = c("below 2.5th", "2.5 - 25th", "25 - 50th",
                                             "50 - 75th", "75 - 97.5th", "above 97.5th")))

# Count discharge quantiles for each analyte.
chem_Q_summarized <- chem_trim_codes_quants %>%
  group_by(usgs_site, CharacteristicName, quantile_sample) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = quantile_sample, values_from = n) %>%
  # reorder just so it's easier to look at
  select(usgs_site, CharacteristicName, 'below 2.5th', '2.5 - 25th', '25 - 50th',
         '50 - 75th', '75 - 97.5th', 'above 97.5th')

# If the full spectrum of discharge is to be represented...
chem_Q_summarized_full <- chem_Q_summarized %>%
  drop_na()
# 291 of the original 477 sites remain.

full_sites <- chem_Q_summarized_full$usgs_site
# saveRDS(full_sites, "data_working/usgs_sites_filtered_011924.rds")

# Create a new column that can be merged into the larger dataset and used as a filter.
chem_Q_summarized <- chem_Q_summarized %>%
  mutate(Pass_Hydro = case_when(!is.na(`below 2.5th`) &
                                  !is.na(`2.5 - 25th`) &
                                  !is.na(`25 - 50th`) &
                                  !is.na(`50 - 75th`) &
                                  !is.na(`75 - 97.5th`) &
                                  !is.na(`above 97.5th`) ~ "YES",
                                 TRUE ~ "NO"))

# Select for columns of interest.
chem_Q_select <- chem_Q_summarized %>%
  select(usgs_site, CharacteristicName, Pass_Hydro)

# Combine with original dataset.
chem_trim_codes_quants <- full_join(chem_trim_codes_quants, chem_Q_select,
                                    by = c("usgs_site", "CharacteristicName"))

# And impose final filter.
chem_trim_codes_trim_quants <- chem_trim_codes_quants %>%
  filter(Pass_Hydro == "YES")
  # removes 72,624 records and 186 sites (n = 291 remaining)

# Export for use in models.
# saveRDS(chem_trim_codes_trim_quants, "data_working/usgs_chem_filtered_011924.rds")

# End of script.