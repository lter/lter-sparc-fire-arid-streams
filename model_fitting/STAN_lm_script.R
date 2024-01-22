# Prepping data and running STAN model
# Heili Lowman
# January 19, 2024

# README: The following script will fit an initial, linear
# STAN model to the concentration and discharge data.
# WARNING -- DO NOT PUSH ANY '.rds' FILES THAT ARE
# CREATED BY STAN TO GITHUB. They are not necessary to store
# as part of this repository.

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

#### Formula 1 - Basic LM ####

##### Data Prep #####

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

##### Model Fit #####

# Prep data for STAN as a list
data_stan <- list(
  N = nrow(oc_dat1),
  C = oc_dat1$scaleOC[1:402],
  Q = oc_dat1$scaleQ[1:402]
)

# Added in the indexing because otherwise the dimensions 
# appeared as [402,1] for some reason, and it was throwing
# off STAN.

# Fit model - should run in <1 minute
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
# And Rhat values all look good (Rhat < 1.05)

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

#### Formula 2 - Pre/Post Fire ####

# Now, I'm going to add in the extra step of calculating pre- and
# post-fire CQ slope values.

##### Data Prep #####

# Create dataset for modeling (similar to above).
oc_dat2 <- data %>%
  # filter only for organic carbon
  filter(USGSPCode == "00681") %>%
  # filter for site with most data
  filter(usgs_site == "USGS-09352900") %>%
  # need to log transform (and scale) concentrations
  mutate(scaleOC = scale(log(ResultMeasureValue))) %>%
  # and to log transform (and scale) discharge
  mutate(scaleQ = scale(log(Flow))) %>%
  # add pre-/post-fire variable to develop model with
  mutate(fire = as.integer(case_when(ActivityStartDate < "2003-07-06" ~ 0,
                          TRUE ~ 1)))

# Remember - log() is ln() in R

# I am now log-transforming the discharge as well since the model
# is taking the form : log(C) = log(a) + b * log(Q)

# Quick plot of the data.
ggplot(oc_dat2, aes(x = scaleQ, y = scaleOC, color = factor(fire))) +
  geom_point(alpha = 0.8) +
  theme_bw()

# Ensure no NAs or NaNs are present in the data,
# because STAN does not allow this.
sum(is.na(oc_dat2$scaleOC)) # 0
sum(is.na(oc_dat2$scaleQ)) # 0
sum(is.na(oc_dat2$fire)) # 0
sum(is.nan(oc_dat2$scaleOC)) # 0
sum(is.nan(oc_dat2$scaleQ)) # 0
sum(is.nan(oc_dat2$fire)) # 0

##### Model Fit #####

# Prep data for STAN as a list
data_stan2 <- list(
  N = nrow(oc_dat2), # number of observations
  C = oc_dat2$scaleOC[1:402], # concentration
  Q = oc_dat2$scaleQ[1:402], # discharge
  f = oc_dat2$fire[1:402] # fire delineation
)

# Added in the indexing because otherwise the dimensions 
# appeared as [402,1] for some reason, and it was throwing
# off STAN.

# Fit model - should run in <1 minute
stan_lm_run2 <- stan(file = "models/STAN_lm_prepost_template.stan",
                    data = data_stan2,
                    chains = 3,
                    iter = 5000,
                    control = list(max_treedepth = 12))
# YIPEE!!

# Examine model convergence
shinystan::launch_shinystan(stan_lm_run2)
# No divergent transitions.

# Examine summaries of the estimates.
stan_lm_data2 <- summary(stan_lm_run2,
                        pars = c("A_pre", "A_post",
                                 "b_pre", "b_post",
                                 "sigma_pre", "sigma_post"),
                        probs = c(0.025, 0.5, 0.975))$summary
# And Rhat values all look good (Rhat < 1.05)

# Plot parameter estimates
color_scheme_set("brightblue")
mcmc_intervals(stan_lm_run2,
           pars = c("A_pre", "A_post", "b_pre", "b_post"),
           point_est = "median",
           prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  )

#### Formula 3 - Delta Slope ####

# Next, I'm going to estimate the change in slopes between
# pre- and post-fire periods.

##### Data Prep #####

# Create dataset for modeling (similar to above).
oc_dat3 <- data %>%
  # filter only for organic carbon
  filter(USGSPCode == "00681") %>%
  # filter for site with most data
  filter(usgs_site == "USGS-09352900") %>%
  # need to log transform (and scale) concentrations
  mutate(scaleOC = scale(log(ResultMeasureValue))) %>%
  # and to log transform (and scale) discharge
  mutate(scaleQ = scale(log(Flow))) %>%
  # add pre-/post-fire variable to develop model with
  mutate(fire = as.integer(case_when(ActivityStartDate < "2003-07-06" ~ 0,
                                     TRUE ~ 1)))

# Quick plot of the data with rough lm()s added.
ggplot(oc_dat3, aes(x = scaleQ, y = scaleOC, color = factor(fire))) +
  geom_point(alpha = 0.8) +
  geom_smooth(method = "lm", fill = NA) +
  theme_bw()
# At first glance, they look very similar, so delta is likely ~0

# Ensure no NAs or NaNs are present in the data,
# because STAN does not allow this.
sum(is.na(oc_dat3$scaleOC)) # 0
sum(is.na(oc_dat3$scaleQ)) # 0
sum(is.na(oc_dat3$fire)) # 0
sum(is.nan(oc_dat3$scaleOC)) # 0
sum(is.nan(oc_dat3$scaleQ)) # 0
sum(is.nan(oc_dat3$fire)) # 0

##### Model Fit #####

# Prep data for STAN as a list
data_stan3 <- list(
  N = nrow(oc_dat2), # number of observations
  C = oc_dat2$scaleOC[1:402], # concentration
  Q = oc_dat2$scaleQ[1:402], # discharge
  f = oc_dat2$fire[1:402] # fire delineation
)

# Added in the indexing because otherwise the dimensions 
# appeared as [402,1] for some reason, and it was throwing
# off STAN.

# Fit model - should run in ~2 minutes
stan_lm_run3 <- stan(file = "models/STAN_lm_delta_template.stan",
                     data = data_stan3,
                     chains = 3,
                     iter = 5000,
                     control = list(max_treedepth = 12))

# Examine model convergence
shinystan::launch_shinystan(stan_lm_run3)
# eek ok so this is crashing a bit, 4000+ divergent transitions

# Examine summaries of the estimates.
stan_lm_data3 <- summary(stan_lm_run3,
                         pars = c("A_pre", "A_post",
                                  "b_pre", "b_post",
                                  "delta"),
                         probs = c(0.025, 0.5, 0.975))$summary
# Rhat values all > 1.05

# Plot parameter estimates
color_scheme_set("orange")
mcmc_intervals(stan_lm_run3,
               pars = c("A_pre", "A_post", "b_pre", "b_post", "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  )

# End of script.
