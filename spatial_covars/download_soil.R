## ------------------------------------------------------- ##
    # SPARC Fire & Aridlands - Download Soil Covariate
## ------------------------------------------------------- ##
# Written by: Nick J Lyon

# Purpose:
## Attempt download of the USGS Polaris soil dataset

# Data Source
## POLARIS: A 30-meter probabilistic soil series map of the contiguous United States
## https://pubs.usgs.gov/publication/70170912

## -------------------------------- ##
          # Housekeeping ----
## -------------------------------- ##

# Read needed libraries
# install.packages("librarian")
librarian::shelf(tidyverse, scicomptools)

# Silence `dplyr::summarize` preemptively
options(dplyr.summarise.inform = FALSE)

# Clear environment / collect garbage
rm(list = ls()); gc()

# Identify path to location of shared data
(path <- scicomptools::wd_loc(local = F, 
                              remote_path = file.path('/', "home", "shares",
                                                      "lter-sparc-fire-arid"),
                              local_path = getwd()))

## -------------------------------- ##
# pH Download ----
## -------------------------------- ##



# End ----

