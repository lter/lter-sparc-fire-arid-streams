# GPP data exploration
# August 21, 2024
# Heili Lowman

# README: The following script was developed to look into the
# GPP data for the stream sites included in this analysis.

#### Setup ####

# Load necessary packages.
library(here)
library(tidyverse)
library(lubridate)
library(patchwork)

# Load GPP data provided by Nick and available on the Google drive here:
# https://drive.google.com/drive/folders/1XxvY56h1cMmaYatF7WhVrbYbaOgdRBGC
gpp_dat <- read_csv("data/fire-arid_gpp.csv")

# And load in the list of sites Heili is using in her model development.
sites <- read_csv("data/fires_test_sites_ed.csv")
no3_sites <- readRDS("data_working/usgs_no3_filtered_lax_080924.rds")

#### Tidy ####

# Make vector of sites of interest.
my_sites <- sites$usgs_site

# And filter the available GPP data by those sites.
gpp_test_sites <- gpp_dat %>%
  filter(usgs_site %in% my_sites)
# Ok, so these don't exist because the 17 sites were chosen prior
# to the imposition of our aridity filters.

# Just as a check, though, let's see if the sites
# for which I want to use NO3 data are included.

# Make the list of NO3 data into a dataframe.
no3_df <- do.call(rbind.data.frame, no3_sites)

# And trim to unique site list.
my_no3_sites <- unique(no3_df$usgs_site.x)

# And now let's see if the GPP data exists for those sites.
gpp_no3_sites <- gpp_dat %>%
  filter(usgs_site %in% my_no3_sites)
# Indeed it does.

# End of script.
