# Prepping data and running STAN model
# Heili Lowman
# January 19, 2024

# README: The following script will fit an initial, linear
# STAN model to the concentration and discharge data.

#### Setup ####

# Load packages.
library(here)
library(tidyverse)
library(rstan)
library(shinystan)
library(bayesplot)
library(data.table)

# Load dataset.
data <- readRDS("data_working/usgs_chem_filtered_011924.rds")

# Run each time you load in "rstan"
rstan_options(auto_write=TRUE)
# auto-caches model results in the same directory
options(mc.cores=parallel::detectCores())
# during runs, each chain needs a dedicated core

#### Data Prep ####

# To first get the models working, I'm going to pare down
# which data I'll use as a test dataset. For now, I will
# filter out for only organic carbon, since there is only
# one code that does not require conversion.

oc_dat <- data %>%
  filter(USGSPCode == "00681")

hist(oc_dat$ResultMeasureValue)

# To help with convergence, I'm also going
# to scale back to a single site.
oc_dat_summ <- oc_dat %>%
  group_by(usgs_site) %>%
  summarize(n = n()) %>%
  ungroup()

oc_dat1 <- oc_dat %>%
  # filter for site with most data
  filter(usgs_site == "USGS-09352900") %>%
  # need to log transform (and scale) concentrations
  mutate(scaleOC = scale(log(ResultMeasureValue))) %>%
  # and scale discharges to one another
  mutate(scaleQ = scale(Flow))

# Ensure no NAs are present in the data,
# because STAN does not allow this.
sum(is.na(oc_dat1$scaleOC)) # 0
sum(is.na(oc_dat1$scaleQ)) # 0
sum(is.nan(oc_dat1$scaleOC)) # 0
sum(is.nan(oc_dat1$scaleQ)) # 0

#### Model Fit ####

# Prep data for STAN as a list
data_stan <- list(
  N = nrow(oc_dat1),
  C = oc_dat1$scaleOC[1:402],
  Q = oc_dat1$scaleQ[1:402]
)

# Added in the indexing because otherwise the dimensions 
# appeared as [402,1] for some reason, and it was throwing
# off STAN.

# Fit model
stan_lm_run <- stan(file = "models/STAN_lm_template.stan",
                      data = data_stan,
                      chains = 3,
                      iter = 5000,
                      control = list(max_treedepth = 12))

# Examine model convergence
shinystan::launch_shinystan(stan_lm_run)
# No divergent transitions.

# Examine summaries of the estimates.
stan_lm_data <- summary(stan_lm_run,
                          pars = c("a", "b", "sigma"),
                          probs = c(0.025, 0.5, 0.975))$summary

# Plot parameter estimates
color_scheme_set("teal")
mcmc_areas(stan_lm_run,
           pars = c("a", "b"),
           point_est = "median",
           prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  )

# End of script.
