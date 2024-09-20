# USGS Chemistry Filtering
# August 9, 2024
# Heili Lowman

# README: The following script will create a workflow for accomplishing
# the following tasks so as to generate a dataset to be used to fit the
# CQ and environmental covariate models.

# (1) Match fire data with chemistry data to determine which sites
# experienced a fire.
# (2) Select the largest fire in each watershed for which we have 4
# years of pre- and post-fire data.
# (3) Match with discharge data to ensure these data span a minimum
# of the 25th to 75th percentiles of discharge.
# (4) Export the joined fire-chem-discharge dataset.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)

# Load necessary datasets.
chem_dat <- readRDS("data/usgs_chemistry.rds")
no3_dat <- readRDS("data/nitrate_chemistry.rds")
Q_dat <- readRDS("data/discharge_daily.rds")
fire_dat <- readxl::read_excel("data/dd_area_stats.xlsx") 

#### Workflow #1 ####

##### Fire data curation #####

# First, how many unique fires do we have?
length(unique(fire_dat$event_id)) # 4,261

# And how many sites are represented?
length(unique(fire_dat$usgs_site)) # 753

# How does this correspond with the sites for which we have chemistry?
length(unique(chem_dat$usgs_site)) # 477
# Ok, so right off the bat, the fire dataset does not appear
# to be limiting.

# Before over-complicating the workflow, I will first simply
# select the largest fire in each watershed and examine how
# many of them appear to have sufficient pre-/post-fire chem data.
fire_max <- fire_dat %>%
  group_by(usgs_site) %>%
  slice_max(per_cent_burned) %>%
  ungroup()

# What does this distribution look like?
ggplot(fire_max, aes(x = per_cent_burned)) +
  geom_histogram() +
  scale_x_log10() +
  theme_bw()
# Largest majority appears to be between 1-10% burned

# There are a few duplicates where there were multiple max values,
# so I've reviewed these manually and I am choosing the ones that
# showed higher fire within the catchment area.
fire_max <- fire_max[-c(71, 119, 138, 198, 350),]

##### Chem data curation #####

# Now, join with chemistry data.
fire_max_trim <- fire_max %>%
  select(usgs_site, event_id, ignition_date, fire_area:per_cent_burned)

chem_fire_dat <- left_join(chem_dat, fire_max_trim,
                           by = c("usgs_site"))

# And select analyte of interest.
no3_fire_dat <- chem_fire_dat %>%
  filter(CharacteristicName %in% c("Nitrate", "Inorganic nitrogen (nitrate and nitrite)"))

# Which pcodes remain?
unique(no3_fire_dat$USGSPCode)
# "00620" = Nitrate, water, unfiltered, milligrams per liter as nitrogen (total)
# "00618" = Nitrate, water, filtered, milligrams per liter as nitrogen (dissolved)
# "00630" = Nitrate plus nitrite, water, unfiltered, milligrams per liter as nitrogen (total)
# "00631" = Nitrate plus nitrite, water, filtered, milligrams per liter as nitrogen (dissolved)
# "71851" = Nitrate, water, filtered, milligrams per liter as nitrate (dissolved)
# "91003" = Nitrate, water, filtered, micrograms per liter as nitrate (dissolved)

# Now, add a column to denote whether the chem data point falls within the correct
# date range.
no3_fire_dat <- no3_fire_dat %>%
  mutate(ignition_plus3 = ignition_date %m+% years(3)) %>%
  mutate(ignition_minus3 = ignition_date %m-% years(3)) %>%
  mutate(within_tf = case_when(ymd(ActivityStartDate) > ymd(ignition_minus3) &
                                 ymd(ActivityStartDate) < ymd(ignition_plus3) ~ "Yes",
                               TRUE ~ "No"))

# Whats the relative distribution of yes vs. no?
no3_counts <- no3_fire_dat %>%
  group_by(usgs_site, within_tf) %>%
  summarize(count = n()) %>%
  ungroup() # 140 sites with Yes, but drops to 66 with 20+ measurements

# Out of curiosity, how many more sites would we gain if
# we expanded the window to 4 years pre/post fire?
no3_fire_dat <- no3_fire_dat %>%
  mutate(ignition_plus4 = ignition_date %m+% years(4)) %>%
  mutate(ignition_minus4 = ignition_date %m-% years(4)) %>%
  mutate(within_tf4 = case_when(ymd(ActivityStartDate) > ymd(ignition_minus4) &
                                 ymd(ActivityStartDate) < ymd(ignition_plus4) ~ "Yes",
                               TRUE ~ "No"))

no3_counts4 <- no3_fire_dat %>%
  group_by(usgs_site, within_tf4) %>%
  summarize(count = n()) %>%
  ungroup() # 154 sites with Yes, 81 sites with 20+ measurements

# Ok, going to use that for now.

# Filter chemistry dataset based on pre-post data.
no3_firetf_dat <- no3_fire_dat %>%
  filter(within_tf4 == "Yes")

# And now, to examine for sufficient pre- and post- fire data.
no3_firetf_dat <- no3_firetf_dat %>%
  mutate(prepost = case_when(ymd(ActivityStartDate) >= ymd(ignition_date) ~ "post",
                             ymd(ActivityStartDate) < ymd(ignition_date) ~ "pre"))

no3_prepost_counts <- no3_firetf_dat %>%
  group_by(usgs_site, prepost) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = prepost,
              values_from = count) # 154 sites in total

no3_prepost_filtered <- no3_prepost_counts %>%
  filter(pre >= 10) %>%
  filter(post >= 10) # 49 sites remaining >_<

# Finally, filter the dataset by these sites with enough
# pre and post fire data.
no3_sufficient <- unique(no3_prepost_filtered$usgs_site)

no3_firepp_dat <- no3_firetf_dat %>%
  filter(usgs_site %in% no3_sufficient)

##### Discharge data evaluation #####

# List of sites to use to trim the discharge data down.
chem_trim_sites <- unique(no3_firepp_dat$usgs_site)

# Trim the discharge data to only include these sites.
Q_trim <- Q_dat %>%
  filter(usgs_site %in% chem_trim_sites)
# Great, removed ~11 million records.

# Calculate the discharge distributions for each site,
# using ALL available data from those sites regardless of
# fire effect window.
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
no3_fire_q_dat <- left_join(no3_firepp_dat, Q_trim, 
                             # join by site and date only on sample dates 
                             # that exist in the chem record
                             by = c("usgs_site", "ActivityStartDate" = "Date"))

# So, I'll just need to remove the NAs a.k.a. days on which we have chem but no Q data
no3_fire_q_dat_noQna <- no3_fire_q_dat %>%
  filter(!is.na(Flow)) # only ~100 records

# Add site quantiles to the dataset.
no3_fire_qquants_dat <- left_join(no3_fire_q_dat_noQna, Q_quantiles,
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
no3_Q_summarized <- no3_fire_qquants_dat %>%
  # interested in distributions both pre and post fire
  group_by(usgs_site, prepost, quantile_sample) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = quantile_sample, values_from = n) %>%
  # reorder just so it's easier to look at
  select(usgs_site, prepost, 'below 2.5th', '2.5 - 25th', '25 - 50th',
         '50 - 75th', '75 - 97.5th', 'above 97.5th')

# If 25th-75th percentiles of discharge are to be represented...
no3_Q_25_75 <- no3_Q_summarized %>%
  drop_na('2.5 - 25th', '25 - 50th', '50 - 75th', '75 - 97.5th')

# And keep only those sites with both pre and post data.
no3_Q_25_75pp <- no3_Q_25_75 %>%
  pivot_wider(names_from = prepost,
              values_from = c('below 2.5th', '2.5 - 25th', '25 - 50th',
                              '50 - 75th', '75 - 97.5th', 'above 97.5th')) %>%
  drop_na('2.5 - 25th_pre', '2.5 - 25th_post', 
          '25 - 50th_pre', '25 - 50th_post', 
          '50 - 75th_pre', '50 - 75th_post', 
          '75 - 97.5th_pre', '75 - 97.5th_post') # 23 sites left

final_sites <- no3_Q_25_75pp$usgs_site

# Filter sites' data so that it:
# (1) represents the largest fire at that site
# (2) has 4 years of corresponding pre/post-fire data
# with a minimum of 10 observations on either side
# (3) spans the 25th to 75th percentiles in discharge

no3_dat_filtered <- no3_fire_qquants_dat %>%
  filter(usgs_site %in% final_sites)

# Export data.
#saveRDS(no3_dat_filtered, "data_working/usgs_no3_filtered_080924.rds")

#### Workflow #2 ####

# Since we dropped an order of magnitude of sites available with this filtering,
# I'll see if I can develop another workflow to maintain more data with less
# strict filters.

##### Fire event selection #####

# Is it possible to iterate through fires instead of trying to
# use only the largest, in an attempt to harvest more data?

# Ok, now for the more complicated workflow.
# First, make the chemistry dataset with only one
# measurement per date.

# NOTE - FIX LATER. Wrote to Stevan to see what the 
# multiple nitrate values are, so I'm averaging them for
# now.
no3_uq <- no3_dat %>%
  group_by(usgs_site, date, analyte) %>%
  summarize(mean_value_std = mean(value_std, na.rm = TRUE)) %>%
  # gah these names are also a mess, so standardizing those
  ungroup() %>%
  mutate(analyte_std = case_when(analyte %in% c("Inorganic nitrogen (nitrate and nitrite)",
                                                "NO3+NO2 - N") ~ "NO3 + NO2",
                                 analyte %in% c("Nitrate", "nitrate") ~ "NO3",
                                 TRUE ~ NA)) %>%
  select(-analyte) %>%
  pivot_wider(names_from = analyte_std, values_from = mean_value_std) %>%
  # and finally select only a single value at each site
  # need to create indices for logical choosing of values
  mutate(no3_no2_ind = case_when(is.na(`NO3 + NO2`) ~ "NO",
                                 TRUE ~ "YES"),
         no3_ind = case_when(is.na(`NO3`) ~ "NO",
                                   TRUE ~ "YES")) %>%
  mutate(all_ind = case_when(no3_no2_ind == "YES" & no3_ind == "YES" ~ 1,
                             no3_no2_ind == "NO" ~ 2,
                             no3_ind == "NO" ~ 3,
                             TRUE ~ NA)) %>%
  # and then make the decision of which value to keep
  # erring on the side of the combined metric since this
  # is the most frequently available
  mutate(mean_value_std_unique = case_when(all_ind %in% c(1,3) ~ `NO3 + NO2`,
                                           all_ind == 2 ~ `NO3`,
                                           TRUE ~ NA),
         analyte_unique = case_when(all_ind %in% c(1,3) ~ "NO3_NO2",
                                    all_ind == 2 ~ "NO3",
                                    TRUE ~ NA)) %>%
  select(usgs_site, date, analyte_unique, mean_value_std_unique)

# Great! Ok so how many sites does that leave us with in total?
length(unique(no3_uq$usgs_site)) # 299

# Trying out on a single site before making iterative.

# Filter fires at a given site.
f <- fire_dat %>%
  filter(usgs_site == "USGS-06713500") # 3 possible

# Filter chemistry data at a given site.
no3 <- no3_uq %>%
  filter(usgs_site == "USGS-06713500") # 382 observations

# Join the chemistry and fire data.
# This matches each chemistry record to all possible
# fires, so you'll get multiple resulting rows per record.
no3_f <- full_join(no3, f)

# Filter by data available within the proper window.
no3_tf <- no3_f %>%
  mutate(ignition_plus4 = ignition_date %m+% years(4)) %>%
  mutate(ignition_minus4 = ignition_date %m-% years(4)) %>%
  mutate(window = case_when(ymd(date) > ymd(ignition_minus4) &
                            ymd(date) < ymd(ignition_plus4) ~ "YES",
                            TRUE ~ "NO"))

summary0 <- no3_tf %>%
  group_by(event_id, window) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = window, values_from = count)

# Filter for fires with data during both pre and post-fire periods.
summary0_filtered <- summary0 %>%
  filter(YES > 0)

fires_wdata <- unique(summary0_filtered$event_id)

no3_tf_pp <- no3_tf %>%
  filter(event_id %in% fires_wdata) %>%
  mutate(prepost = factor(case_when(ymd(date) > ymd(ignition_minus4) &
                                  ymd(date) <= ymd(ignition_date) ~ "pre",
                             ymd(date) > ymd(ignition_date) &
                                  ymd(date) <= ymd(ignition_plus4) ~ "post",
                                TRUE ~ NA),
                          levels = c("pre", "post", NA)))

summary1 <- no3_tf_pp %>%
  count(event_id, prepost, .drop = FALSE) %>%
  ungroup() %>%
  pivot_wider(names_from = prepost, values_from = n)

# Filter for fires with enough (n = 10) data during both
# pre and post-fire periods.
summary1_filtered <- summary1 %>%
  mutate(enough = case_when(post >= 10 & pre >= 10 ~ "Yes",
                            TRUE ~ "No")) %>%
  filter(enough == "Yes")

if (length(summary1_filtered$event_id) == 0){
  
  df <- tibble(`event_id` = 0)
  
  return(df)
  
  } else {

    fires <- unique(summary1_filtered$event_id)
  
    }

# ENSURES A MINIMUM OF TEN OBSERVATIONS PRE AND POST FIRE!!!
no3_tf_pp <- no3_tf_pp %>%
  filter(event_id %in% fires)

##### Discharge Filter #####

# Filter discharge data at a given site.
q <- Q_dat %>%
  filter(usgs_site == "USGS-06713500") %>%
  # keeping only "accepted" measurements
  filter(Flow_cd %in% c("A", "A e"))

q_25 <- as.numeric(quantile(q$Flow, probs = 0.25, na.rm = TRUE))
q_75 <- as.numeric(quantile(q$Flow, probs = 0.75, na.rm = TRUE))

# Join discharge data on to calculate what intervals are represented.
no3_f_q <- left_join(no3_tf_pp, q,
                     by = c("date" = "Date")) %>%
  mutate(quartile = case_when(Flow < q_25 ~ "below25",
                              Flow >= q_25 & Flow <= q_75 ~ "25_75",
                              Flow > q_75 ~ "above75"))

# Create summary dataset to select by.
summary2 <- no3_f_q %>%
  count(event_id, per_cent_burned, prepost, quartile,
        .drop = FALSE) %>%
  ungroup() %>%
  pivot_wider(names_from = prepost, values_from = n)

# Filter for fires with enough discharge data.
summary2_filtered <- summary2 %>%
  # remove NA columns/rows
  select(-`NA`) %>%
  drop_na(quartile) %>%
  pivot_longer(cols = c(pre,post)) %>%
  mutate(quantile_pp = paste(quartile, name, sep = "_")) %>%
  select(-c(quartile, name)) %>%
  pivot_wider(names_from = quantile_pp, values_from = value) %>%
  mutate(enough = case_when(below25_pre > 0 & 
                            `25_75_pre` > 0 &
                            above75_pre > 0 &
                            below25_post > 0 &
                            `25_75_post` > 0 &
                            above75_post > 0 ~ "Yes",
                            TRUE ~ "No")) %>%
  filter(enough == "Yes") %>%
  # And then select the largest fire.
  slice_max(per_cent_burned) %>%
  # And, in the case of identical % watershed burned or
  # same fire with multiple IDs, let's select just the first.
  slice_head()

if (length(summary2_filtered$event_id) == 0) {
  
  df2 <- tibble(`event_id` = 0)
  
  return(df2)
  
  } else {

  the_fire <- unique(summary2_filtered$event_id)
  
  }

no3_tf_q <- no3_f_q %>%
  filter(event_id == the_fire) %>%
  filter(window == "YES")

# Quick plot to see how this maps.
ggplot(no3_tf_q, aes(x = log10(Flow), y = log10(mean_value_std_unique),
                     color = prepost, group = prepost)) +
  geom_point(alpha = 0.8) +
  geom_smooth(method = "lm") +
  scale_color_manual(values = c("grey50", "black")) +
  labs(x = "log(Discharge)", y = "log(NO3 Concentration)") +
  theme_bw()

##### Function #####

# Ok, now to make this a function that I'll apply across the sites.
# Make lists to iterate over.
no3_f <- inner_join(no3_uq, fire_dat)

no3_f_list <- split(no3_f, no3_f$usgs_site) # 144 sites w both NO3 & fire data

sites144 <- unique(no3_f$usgs_site)

# Filter for appropriate discharge data
q_filtered <- Q_dat %>%
  filter(usgs_site %in% sites144)

q_list <- split(q_filtered, q_filtered$usgs_site)

fireDischargeFilter <- function(x, q) {

  # Filter by data available within the proper window/timeframe.
  x_tf <- x %>%
    mutate(ignition_plus5 = ignition_date %m+% years(5)) %>%
    mutate(ignition_minus5 = ignition_date %m-% years(5)) %>%
    mutate(prepost = factor(case_when(ymd(date) > ymd(ignition_minus5) &
                               ymd(date) <= ymd(ignition_date) ~ "pre",
                               ymd(date) > ymd(ignition_date) &
                               ymd(date) <= ymd(ignition_plus5) ~ "post",
                             TRUE ~ NA),
                            levels = c("pre", "post", "NA"))) %>%
    filter(prepost %in% c("pre", "post", "NA"))

  summary1 <- x_tf %>%
    count(event_id, prepost, .drop = FALSE) %>%
    ungroup() %>%
    pivot_wider(names_from = prepost, values_from = n)
  
  # Filter for fires with enough (n = 10) data during both
  # pre and post-fire periods.
  summary1_filtered <- summary1 %>%
    mutate(enough = case_when(post >= 10 & pre >= 10 ~ "Yes",
                              TRUE ~ "No")) %>%
    filter(enough == "Yes")
  
  # First point at which there might be an empty dataframe,
  # so I need to account for that.
  if (length(summary1_filtered$event_id) == 0) {
    
    df <- tibble(`event_id` = 0)
    
    return(df)
    
  } else {
    
  fires <- unique(summary1_filtered$event_id)

  # ENSURES A MINIMUM OF TEN OBSERVATIONS PRE AND POST FIRE!!!
  x_tf10 <- x_tf %>%
    filter(event_id %in% fires)
  
  q <- q %>%
    # keeping only "accepted" discharge measurements
    filter(Flow_cd %in% c("A", "A e"))

  # Calculate discharge quantiles.
  q_25 <- as.numeric(quantile(q$Flow, probs = 0.25, na.rm = TRUE))
  q_75 <- as.numeric(quantile(q$Flow, probs = 0.75, na.rm = TRUE))

  # Join discharge data on to calculate what intervals are represented.
  x_tf10_q <- left_join(x_tf10, q,
                      by = c("date" = "Date")) %>%
              mutate(quartile = factor(case_when(Flow < q_25 ~ "below25",
                                                 Flow >= q_25 & Flow <= q_75 ~ "25_75",
                                                 Flow > q_75 ~ "above75"),
                                       levels = c("below25", "25_75", "above75")))

  # Create summary dataset to select by.
  summary2 <- x_tf10_q %>%
    count(event_id, per_cent_burned, prepost, quartile,
          .drop = FALSE) %>%
    ungroup() %>%
    pivot_wider(names_from = prepost, values_from = n)

  # Filter for fires with enough discharge data.
  summary2_filtered <- summary2 %>%
    # remove NA columns/rows
    select(-`NA`) %>%
    drop_na(quartile) %>%
    # pivot and rename columns for easier pivoting below
    pivot_longer(cols = c(pre,post)) %>%
    mutate(quantile_pp = paste(quartile, name, sep = "_")) %>%
    select(-c(quartile, name)) %>%
    pivot_wider(names_from = quantile_pp, values_from = value) %>%
    # filter sites that span the full discharge spectrum
    mutate(enough = case_when(below25_pre > 0 & 
                                `25_75_pre` > 0 &
                                above75_pre > 0 &
                                below25_post > 0 &
                                `25_75_post` > 0 &
                                above75_post > 0 ~ "Yes",
                              TRUE ~ "No")) %>%
    filter(enough == "Yes") %>%
    # and then select the largest fire
    slice_max(per_cent_burned) %>%
    # and, in the case of identical % watershed burned or
    # same fire with multiple IDs, let's select just the first.
    slice_head()
  
    # Second point at which there might be an empty dataframe.
    if (length(summary2_filtered$event_id) == 0) {
    
      df2 <- tibble(`event_id` = 0)
    
      return(df2)
    
    } else {

      the_fire <- unique(summary2_filtered$event_id)

      x_df <- x_tf10_q %>%
        filter(event_id == the_fire) %>%
        filter(prepost %in% c("pre", "post"))

      return(x_df)
      
    }
  
  }

}

# And now to apply across all sites.
no3_dat_filtered <- mapply(fireDischargeFilter,
                           x = no3_f_list,
                           q = q_list) # PHEW!!

no3_dat_metcriteria <- purrr::keep(no3_dat_filtered, ~ unique(.x$event_id) != 0)

# Export data.
# saveRDS(no3_dat_metcriteria, "data_working/usgs_no3_filtered_lax_082324.rds")

##### Plots #####

# Let's take a look at what these data look like.

no3_df_metcriteria <- do.call(rbind.data.frame, no3_dat_metcriteria)

(fig_ts <- ggplot(no3_df_metcriteria) +
    geom_point(aes(x = date,
                   y = mean_value_std_unique,
                   color = prepost)) +
    scale_color_manual(values = c("grey50", "black")) +
    labs(x = "Date", y = "NO3 mg/L-N",
         color = "Relative to fire") +
    facet_wrap(.~usgs_site.x, scales = "free") +
    theme_bw())

# ggsave(plot = fig_ts,
#        filename = "figures/no3_16site_timeseries_082324.jpg",
#        width = 25,
#        height = 15,
#        units = "cm")
  
(fig_lm <- ggplot(no3_df_metcriteria, 
                  aes(x = log10(Flow), 
                      y = log10(mean_value_std_unique),
              color = prepost, group = prepost)) +
  geom_point(alpha = 0.8) +
  geom_smooth(method = "lm") +
  scale_color_manual(values = c("grey50", "black")) +
  labs(x = "log(Discharge)", y = "log(NO3 Concentration)") +
    facet_wrap(.~usgs_site.x, scales = "free") +
  theme_bw())

# ggsave(plot = fig_lm,
#        filename = "figures/no3_16site_CQlms_082324.jpg",
#        width = 25,
#        height = 15,
#        units = "cm")

##### Sensitivity Analysis #####

# Since we see a HUUUUUGE drop in sites that meet all the criteria,
# I'm going to modify the function above to see how many sites we 
# get at each of the filtering steps.

# Already, we know that we have:
length(unique(no3_uq$usgs_site)) # 299 sites with NO3 data
length(unique(no3_f$usgs_site)) # 155 sites w/o fire (n = 144 remaining)

filter_step1 <- function(x, q) {
  
  # Filter by data available within the proper window/timeframe.
  x_tf <- x %>%
    mutate(ignition_plus4 = ignition_date %m+% years(4)) %>%
    mutate(ignition_minus4 = ignition_date %m-% years(4)) %>%
    mutate(prepost = factor(case_when(ymd(date) > ymd(ignition_minus4) &
                                        ymd(date) <= ymd(ignition_date) ~ "pre",
                                      ymd(date) > ymd(ignition_date) &
                                        ymd(date) <= ymd(ignition_plus4) ~ "post",
                                      TRUE ~ NA),
                            levels = c("pre", "post", "NA"))) %>%
    filter(prepost %in% c("pre", "post", "NA"))
  
  summary1 <- x_tf %>%
    count(event_id, prepost, .drop = FALSE) %>%
    ungroup() %>%
    pivot_wider(names_from = prepost, values_from = n)
  
  # Filter for fires with enough (n = 10) data during both
  # pre and post-fire periods.
  summary1_filtered <- summary1 %>%
    mutate(enough = case_when(post >= 10 & pre >= 10 ~ "Yes",
                              TRUE ~ "No")) %>%
    filter(enough == "Yes")
  
  # First point at which there might be an empty dataframe,
  # so I need to account for that.
  if (length(summary1_filtered$event_id) == 0) {
    
    df <- tibble(`event_id` = 0)
    
    return(df)
    
  } else {
    
    fires <- unique(summary1_filtered$event_id)
    
    # ENSURES A MINIMUM OF TEN OBSERVATIONS PRE AND POST FIRE!!!
    x_tf10 <- x_tf %>%
      filter(event_id %in% fires)
    
    return (x_tf10)
  }
    
}

no3_dat_step1filter <- mapply(filter_step1,
                           x = no3_f_list,
                           q = q_list)

no3_dat_metstep1 <- purrr::keep(no3_dat_step1filter, 
                                ~ .x$event_id[1] != "0")

length(unique(no3_dat_metstep1$usgs_site)) # 120 sites w/o data (n = 24 remaining)

# And another 8 sites w/o discharge span (n = 16 remaining)

#### Workflow #3 ####

# Since we dropped an order of magnitude of sites available with this filtering,
# I'll see if I can develop yet another workflow to maintain more data with less
# strict filters.

##### Fire event selection #####

# Still need to investigate:
# Is it possible to iterate through fires instead of trying to
# use only the largest, in an attempt to harvest more data?

# First, make the chemistry dataset with only one
# measurement per date.

# NOTE - FIX LATER. Wrote to Stevan to see what the 
# multiple nitrate values are, so I'm averaging them for
# now.
no3_uq <- no3_dat %>%
  group_by(usgs_site, date, analyte) %>%
  summarize(mean_value_std = mean(value_std, na.rm = TRUE)) %>%
  # gah these names are also a mess, so standardizing those
  ungroup() %>%
  mutate(analyte_std = case_when(analyte %in% c("Inorganic nitrogen (nitrate and nitrite)",
                                                "NO3+NO2 - N") ~ "NO3 + NO2",
                                 analyte %in% c("Nitrate", "nitrate") ~ "NO3",
                                 TRUE ~ NA)) %>%
  select(-analyte) %>%
  pivot_wider(names_from = analyte_std, values_from = mean_value_std) %>%
  # and finally select only a single value at each site
  # need to create indices for logical choosing of values
  mutate(no3_no2_ind = case_when(is.na(`NO3 + NO2`) ~ "NO",
                                 TRUE ~ "YES"),
         no3_ind = case_when(is.na(`NO3`) ~ "NO",
                             TRUE ~ "YES")) %>%
  mutate(all_ind = case_when(no3_no2_ind == "YES" & no3_ind == "YES" ~ 1,
                             no3_no2_ind == "NO" ~ 2,
                             no3_ind == "NO" ~ 3,
                             TRUE ~ NA)) %>%
  # and then make the decision of which value to keep
  # erring on the side of the combined metric since this
  # is the most frequently available
  mutate(mean_value_std_unique = case_when(all_ind %in% c(1,3) ~ `NO3 + NO2`,
                                           all_ind == 2 ~ `NO3`,
                                           TRUE ~ NA),
         analyte_unique = case_when(all_ind %in% c(1,3) ~ "NO3_NO2",
                                    all_ind == 2 ~ "NO3",
                                    TRUE ~ NA)) %>%
  select(usgs_site, date, analyte_unique, mean_value_std_unique)

# Great! Ok so how many sites does that leave us with in total?
length(unique(no3_uq$usgs_site)) # 299

# Trying out on a single site before making iterative.

# Filter fires at a given site.
f <- fire_dat %>%
  filter(usgs_site == "USGS-06713500") # 3 possible

# Filter chemistry data at a given site.
no3 <- no3_uq %>%
  filter(usgs_site == "USGS-06713500") # 382 observations

# Join the chemistry and fire data.
# This matches each chemistry record to all possible
# fires, so you'll get multiple resulting rows per record.
no3_f <- full_join(no3, f)

# Filter by data available within the proper window.
no3_tf <- no3_f %>%
  mutate(ignition_plus4 = ignition_date %m+% years(4)) %>%
  mutate(ignition_minus4 = ignition_date %m-% years(4)) %>%
  mutate(window = case_when(ymd(date) > ymd(ignition_minus4) &
                              ymd(date) < ymd(ignition_plus4) ~ "YES",
                            TRUE ~ "NO"))

summary0 <- no3_tf %>%
  group_by(event_id, window) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  pivot_wider(names_from = window, values_from = count)

# Filter for fires with data during both pre and post-fire periods.
summary0_filtered <- summary0 %>%
  filter(YES > 0)

fires_wdata <- unique(summary0_filtered$event_id)

no3_tf_pp <- no3_tf %>%
  filter(event_id %in% fires_wdata) %>%
  mutate(prepost = factor(case_when(ymd(date) > ymd(ignition_minus4) &
                                      ymd(date) <= ymd(ignition_date) ~ "pre",
                                    ymd(date) > ymd(ignition_date) &
                                      ymd(date) <= ymd(ignition_plus4) ~ "post",
                                    TRUE ~ NA),
                          levels = c("pre", "post", NA)))

summary1 <- no3_tf_pp %>%
  count(event_id, prepost, .drop = FALSE) %>%
  ungroup() %>%
  pivot_wider(names_from = prepost, values_from = n)

# Filter for fires with enough (n = 10) data during both
# pre and post-fire periods.
summary1_filtered <- summary1 %>%
  mutate(enough = case_when(post >= 10 & pre >= 10 ~ "Yes",
                            TRUE ~ "No")) %>%
  filter(enough == "Yes")

if (length(summary1_filtered$event_id) == 0){
  
  df <- tibble(`event_id` = 0)
  
  return(df)
  
} else {
  
  fires <- unique(summary1_filtered$event_id)
  
}

# ENSURES A MINIMUM OF TEN OBSERVATIONS PRE AND POST FIRE!!!
no3_tf_pp <- no3_tf_pp %>%
  filter(event_id %in% fires)

##### Discharge Filter #####

# Filter discharge data at a given site.
q <- Q_dat %>%
  filter(usgs_site == "USGS-06713500")
  # instead of filtering only for accepted values
  # I'm going to filter by date of fire

no3_f_quartiles <- left_join(no3_tf_pp, q,
                     by = c("date" = "Date")) %>%
  filter(window == "YES") %>%
  group_by(event_id) %>%
  summarize(q_25 = as.numeric(quantile(Flow, probs = 0.25, na.rm = TRUE)),
         q_75 = as.numeric(quantile(Flow, probs = 0.75, na.rm = TRUE))) %>%
  ungroup()


# Join discharge data on to calculate what intervals are represented.
no3_f_q <- left_join(no3_tf_pp, q,
                     by = c("date" = "Date")) %>%
  left_join(no3_f_quartiles, by = c("event_id")) %>%
  mutate(quartile = case_when(Flow < q_25 ~ "below25",
                              Flow >= q_25 & Flow <= q_75 ~ "25_75",
                              Flow > q_75 ~ "above75"))

# Create summary dataset to select by.
summary2 <- no3_f_q %>%
  count(event_id, per_cent_burned, prepost, quartile,
        .drop = FALSE) %>%
  ungroup() %>%
  pivot_wider(names_from = prepost, values_from = n)

# Filter for fires with enough discharge data.
summary2_filtered <- summary2 %>%
  # remove NA columns/rows
  select(-`NA`) %>%
  drop_na(quartile) %>%
  pivot_longer(cols = c(pre,post)) %>%
  mutate(quantile_pp = paste(quartile, name, sep = "_")) %>%
  select(-c(quartile, name)) %>%
  pivot_wider(names_from = quantile_pp, values_from = value) %>%
  mutate(enough = case_when(below25_pre > 0 & 
                              `25_75_pre` > 0 &
                              above75_pre > 0 &
                              below25_post > 0 &
                              `25_75_post` > 0 &
                              above75_post > 0 ~ "Yes",
                            TRUE ~ "No")) %>%
  filter(enough == "Yes") %>%
  # And then select the largest fire.
  slice_max(per_cent_burned) %>%
  # And, in the case of identical % watershed burned or
  # same fire with multiple IDs, let's select just the first.
  slice_head()

if (length(summary2_filtered$event_id) == 0) {
  
  df2 <- tibble(`event_id` = 0)
  
  return(df2)
  
} else {
  
  the_fire <- unique(summary2_filtered$event_id)
  
}

no3_tf_q <- no3_f_q %>%
  filter(event_id == the_fire) %>%
  filter(window == "YES")

# Quick plot to see how this maps.
ggplot(no3_tf_q, aes(x = log10(Flow), y = log10(mean_value_std_unique),
                     color = prepost, group = prepost)) +
  geom_point(alpha = 0.8) +
  geom_smooth(method = "lm") +
  scale_color_manual(values = c("grey50", "black")) +
  labs(x = "log(Discharge)", y = "log(NO3 Concentration)") +
  theme_bw()

##### Function #####

# Ok, now to make this a function that I'll apply across the sites.
# Make lists to iterate over.
no3_f <- inner_join(no3_uq, fire_dat)

no3_f_list <- split(no3_f, no3_f$usgs_site) # 144 sites w both NO3 & fire data

sites144 <- unique(no3_f$usgs_site)

# Filter for appropriate discharge data
q_filtered <- Q_dat %>%
  filter(usgs_site %in% sites144)

q_list <- split(q_filtered, q_filtered$usgs_site)

fireDischargeFilter <- function(x, q) {
  
  # Filter by data available within the proper window/timeframe.
  x_tf <- x %>%
    mutate(ignition_plus5 = ignition_date %m+% years(5)) %>%
    mutate(ignition_minus5 = ignition_date %m-% years(5)) %>%
    mutate(prepost = factor(case_when(ymd(date) > ymd(ignition_minus5) &
                                        ymd(date) <= ymd(ignition_date) ~ "pre",
                                      ymd(date) > ymd(ignition_date) &
                                        ymd(date) <= ymd(ignition_plus5) ~ "post",
                                      TRUE ~ NA),
                            levels = c("pre", "post", "NA"))) %>%
    filter(prepost %in% c("pre", "post", "NA"))
  
  summary1 <- x_tf %>%
    count(event_id, prepost, .drop = FALSE) %>%
    ungroup() %>%
    pivot_wider(names_from = prepost, values_from = n)
  
  # Filter for fires with enough (n = 10) data during both
  # pre and post-fire periods.
  summary1_filtered <- summary1 %>%
    mutate(enough = case_when(post >= 10 & pre >= 10 ~ "Yes",
                              TRUE ~ "No")) %>%
    filter(enough == "Yes")
  
  # First point at which there might be an empty dataframe,
  # so I need to account for that.
  if (length(summary1_filtered$event_id) == 0) {
    
    df <- tibble(`event_id` = 0)
    
    return(df)
    
  } else {
    
    fires <- unique(summary1_filtered$event_id)
    
    # ENSURES A MINIMUM OF TEN OBSERVATIONS PRE AND POST FIRE!!!
    x_tf10 <- x_tf %>%
      filter(event_id %in% fires)
    
    x_tf10_quartiles <- left_join(x_tf10, q,
                                 by = c("date" = "Date")) %>%
      filter(prepost %in% c("pre", "post")) %>%
      group_by(event_id) %>%
      summarize(q_25 = as.numeric(quantile(Flow, 
                                           probs = 0.25, 
                                           na.rm = TRUE)),
                q_75 = as.numeric(quantile(Flow, 
                                           probs = 0.75, 
                                           na.rm = TRUE))) %>%
      ungroup()
    
    # Join discharge data on to calculate what intervals are represented.
    x_tf10_q <- left_join(x_tf10, q,
                          by = c("date" = "Date")) %>%
      left_join(x_tf10_quartiles, by = c("event_id")) %>%
      mutate(quartile = factor(case_when(Flow < q_25 ~ "below25",
                                         Flow >= q_25 & Flow <= q_75 ~ "25_75",
                                         Flow > q_75 ~ "above75"),
                               levels = c("below25", "25_75", "above75")))
    
    # Create summary dataset to select by.
    summary2 <- x_tf10_q %>%
      count(event_id, per_cent_burned, prepost, quartile,
            .drop = FALSE) %>%
      ungroup() %>%
      pivot_wider(names_from = prepost, values_from = n)
    
    # Filter for fires with enough discharge data.
    summary2_filtered <- summary2 %>%
      # remove NA columns/rows
      select(-`NA`) %>%
      drop_na(quartile) %>%
      # pivot and rename columns for easier pivoting below
      pivot_longer(cols = c(pre,post)) %>%
      mutate(quantile_pp = paste(quartile, name, sep = "_")) %>%
      select(-c(quartile, name)) %>%
      pivot_wider(names_from = quantile_pp, values_from = value) %>%
      # filter sites that span the full discharge spectrum
      mutate(enough = case_when(below25_pre > 0 & 
                                  `25_75_pre` > 0 &
                                  above75_pre > 0 &
                                  below25_post > 0 &
                                  `25_75_post` > 0 &
                                  above75_post > 0 ~ "Yes",
                                TRUE ~ "No")) %>%
      filter(enough == "Yes") %>%
      # and then select the largest fire
      slice_max(per_cent_burned) %>%
      # and, in the case of identical % watershed burned or
      # same fire with multiple IDs, let's select just the first.
      slice_head()
    
    # Third point at which there might be an empty dataframe.
    if (length(summary2_filtered$event_id) == 0) {
      
      df2 <- tibble(`event_id` = 0)
      
      return(df2)
      
    } else {
      
      the_fire <- unique(summary2_filtered$event_id)
      
      x_df <- x_tf10_q %>%
        filter(event_id == the_fire) %>%
        filter(prepost %in% c("pre", "post"))
      
      return(x_df)
      
    }
    
  }
  
}

# And now to apply across all sites.
no3_dat_filtered <- mapply(fireDischargeFilter,
                           x = no3_f_list,
                           q = q_list) # PHEW!!

no3_dat_metcriteria <- purrr::keep(no3_dat_filtered, ~ unique(.x$event_id) != 0) # 25 sites instead.

# Export data.
saveRDS(no3_dat_metcriteria, "data_working/usgs_no3_filtered_lax_windowquart_092024.rds")

##### Plots #####

# Let's take a look at what these data look like.

no3_df_metcriteria <- do.call(rbind.data.frame, no3_dat_metcriteria)

(fig_ts <- ggplot(no3_df_metcriteria) +
    geom_point(aes(x = date,
                   y = mean_value_std_unique,
                   color = prepost)) +
    scale_color_manual(values = c("grey50", "black")) +
    labs(x = "Date", y = "NO3 mg/L-N",
         color = "Relative to fire") +
    facet_wrap(.~usgs_site.x, scales = "free") +
    theme_bw())

# ggsave(plot = fig_ts,
#        filename = "figures/no3_25site_timeseries_092024.jpg",
#        width = 25,
#        height = 15,
#        units = "cm")

(fig_lm <- ggplot(no3_df_metcriteria, 
                  aes(x = log10(Flow), 
                      y = log10(mean_value_std_unique),
                      color = prepost, group = prepost)) +
    geom_point(alpha = 0.8) +
    geom_smooth(method = "lm") +
    scale_color_manual(values = c("grey50", "black")) +
    labs(x = "log(Discharge)", y = "log(NO3 Concentration)") +
    facet_wrap(.~usgs_site.x, scales = "free") +
    theme_bw())

# ggsave(plot = fig_lm,
#        filename = "figures/no3_25site_CQlms_092024.jpg",
#        width = 25,
#        height = 15,
#        units = "cm")

# End of script.
