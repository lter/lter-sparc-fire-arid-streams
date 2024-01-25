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

##### Data Prep OC #####

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

##### Model Fit OC #####

# Prep data for STAN as a list
data_stan3 <- list(
  N = nrow(oc_dat3), # number of observations
  C = oc_dat3$scaleOC[1:402], # concentration
  Q = oc_dat3$scaleQ[1:402], # discharge
  f = oc_dat3$fire[1:402] # fire delineation
)

# Added in the indexing because otherwise the dimensions 
# appeared as [402,1] for some reason, and it was throwing
# off STAN.

# sets initial values to help chains converge
# init_stan3 <- function(...) {
#   list(A_post = 0, 
#        A_pre = 0, 
#        b_post = 0, 
#        b_pre = 0,
#        delta = 0) # values to match priors
# }

# Fit model - should run in 1-3 minutes
stan_lm_run3 <- stan(file = "models/STAN_lm_delta_template.stan",
                     data = data_stan3,
                     chains = 3,
                     iter = 5000,
                     control = list(max_treedepth = 12))

# Examine model convergence
shinystan::launch_shinystan(stan_lm_run3)
# eek ok so this is crashing a bit; notes on iterative troubleshooting below:
# 4600 divergent transitions at first
# 4900 divergent transitions with init values added (eek!)
# 4039 divergent transitions with b priors changed to (0, 1) (still blegh)
# Would not run when moved ifelse statement to transformed param block
# 1929 divergent transitions with delta priors removed (better...)
# 5098 divergent transitions with delta priors changed to (0,1) (yuck)
# 3068 divergent transitions with sigma_delta prior added (0,1) (ran more quickly but hmmm)
# 2218 divergent transitions with sigma_post/pre priors added (0,1)
# 2!!! divergent transitions with delta and sigma_delta priors removed
# 2507 divergent transitions with delta/sigma_delta priors set to (0,1E-1)

# Examine summaries of the estimates.
stan_lm_data3 <- summary(stan_lm_run3,
                         pars = c("A_pre", "A_post",
                                  "b_pre", "b_post",
                                  "sigma_post", "sigma_pre",
                                  "delta", "sigma_delta"),
                         probs = c(0.025, 0.5, 0.975))$summary
# Rhat values all < 1.05 EXCEPT delta and sigma_delta

# Plot parameter estimates
color_scheme_set("orange")
mcmc_intervals(stan_lm_run3,
               pars = c("A_pre", "A_post", "b_pre", "b_post", "delta",
                        "sigma_pre", "sigma_post", "sigma_delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  )

##### Data Prep SC #####

# Ok, so part of the issue here may be that there is too little "delta"
# for the model to glom onto here, so let's look for another site at which
# there *is* a demonstrated change.

# Also, I've edited the model script to store "delta" as a
# "generated quantity" rather than an estimated parameter.

# Create dataset for modeling (similar to above).
sc_dat3 <- data %>%
  # filter only for specific conductance
  filter(USGSPCode %in% c("00095", "90095", "00094")) %>%
  # filter for specific site
  filter(usgs_site == "USGS-07227100") %>%
  # remove days on which flow = 0 %>%
  filter(Flow > 0) %>%
  # need to log transform (and scale) concentrations
  mutate(scaleSC = scale(log(ResultMeasureValue))) %>%
  # and to log transform (and scale) remaining discharge
  mutate(scaleQ = scale(log(Flow))) %>%
  # add pre-/post-fire variable to develop model with
  mutate(fire = as.integer(case_when(ActivityStartDate < "1998-07-14" ~ 0,
                                     TRUE ~ 1)))

# Quick plot of the data with rough lm()s added.
ggplot(sc_dat3, aes(x = scaleQ, y = scaleSC, color = factor(fire))) +
  geom_point(alpha = 0.8) +
  geom_smooth(method = "lm", fill = NA) +
  theme_bw()
# At first glance, both negative, but slope definitely changes post-fire

# Ensure no NAs or NaNs are present in the data,
# because STAN does not allow this.
sum(is.na(sc_dat3$scaleSC)) # 0
sum(is.na(sc_dat3$scaleQ)) # 0
sum(is.na(sc_dat3$fire)) # 0
sum(is.nan(sc_dat3$scaleSC)) # 0
sum(is.nan(sc_dat3$scaleQ)) # 0
sum(is.nan(sc_dat3$fire)) # 0

##### Model Fit SC #####

# Prep data for STAN as a list
data_stan3.1 <- list(
  N = nrow(sc_dat3), # number of observations
  C = sc_dat3$scaleSC[1:419], # concentration
  Q = sc_dat3$scaleQ[1:419], # discharge
  f = sc_dat3$fire[1:419] # fire delineation
)

# Fit model - should run in <1 minute
stan_lm_run3.1 <- stan(file = "models/STAN_lm_delta_template.stan",
                     data = data_stan3.1,
                     chains = 3,
                     iter = 5000,
                     control = list(max_treedepth = 12))

# Examine summaries of the estimates.
stan_lm_data3.1 <- summary(stan_lm_run3.1,
                         pars = c("A_pre", "A_post",
                                  "b_pre", "b_post",
                                  "sigma_post", "sigma_pre",
                                  "delta"),
                         probs = c(0.025, 0.5, 0.975))$summary
# Rhat values all < 1.05 YIPEEE!!!

# Plot parameter estimates
color_scheme_set("pink")
mcmc_intervals(stan_lm_run3.1,
               pars = c("A_pre", "A_post", "b_pre",
                        "b_post","delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  )

#### Formula 4 - Multiple Sites ####

##### Data Prep #####

# Now, I need to make the model iterate over multiple sites,
# so I need to first prepare a list of 10 sites with pre-
# and post-fire specific conductivity data.

agg_data <- data %>%
  # filter only for specific conductance
  filter(USGSPCode %in% c("00095", "90095", "00094")) %>%
  # group by site
  group_by(usgs_site) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  filter(count > 1000)

my17sites <- agg_data$usgs_site

dat4 <- data %>%
  # filter only for specific conductance
  filter(USGSPCode %in% c("00095", "90095", "00094")) %>%
  # filter for sites of interest
  filter(usgs_site %in% my17sites) %>%
  # remove days on which flow = 0 or SC = 0 %>%
  filter(Flow > 0) %>%
  filter(ResultMeasureValue > 0) %>%
  # need to log transform (and scale) concentrations
  mutate(scaleSC = scale(log(ResultMeasureValue))) %>%
  # and to log transform (and scale) remaining discharge
  mutate(scaleQ = scale(log(Flow))) %>%
  # add pre-/post-fire variable to develop model with
  mutate(fire = as.integer(case_when(
    usgs_site == "USGS-07103700" & ActivityStartDate < "2012-06-23" ~ 0,
    usgs_site == "USGS-07105500" & ActivityStartDate < "2012-06-23" ~ 0,
    usgs_site == "USGS-07105800" & ActivityStartDate < "2012-06-23" ~ 0,
    usgs_site == "USGS-07106300" & ActivityStartDate < "2012-06-23" ~ 0,
    usgs_site == "USGS-07106500" & ActivityStartDate < "2012-06-23" ~ 0,
    usgs_site == "USGS-07109500" & ActivityStartDate < "2012-06-23" ~ 0,
    usgs_site == "USGS-08313000" & ActivityStartDate < "2013-06-05" ~ 0,
    usgs_site == "USGS-08330000" & ActivityStartDate < "2011-06-26" ~ 0,
    usgs_site == "USGS-08354900" & ActivityStartDate < "2011-06-26" ~ 0,
    usgs_site == "USGS-08355490" & ActivityStartDate < "2011-06-26" ~ 0,
    usgs_site == "USGS-08358400" & ActivityStartDate < "2011-06-26" ~ 0,
    usgs_site == "USGS-09095500" & ActivityStartDate < "2020-07-31" ~ 0,
    usgs_site == "USGS-09152500" & ActivityStartDate < "1994-07-04" ~ 0,
    usgs_site == "USGS-09163500" & ActivityStartDate < "2020-07-31" ~ 0,
    usgs_site == "USGS-09261000" & ActivityStartDate < "2012-06-24" ~ 0,
    usgs_site == "USGS-09367540" & ActivityStartDate < "2018-06-01" ~ 0,
    usgs_site == "USGS-09367580" & ActivityStartDate < "2018-06-01" ~ 0,
                                       TRUE ~ 1)))

# Quick plot of the data with rough lm()s added.
ggplot(dat4, aes(x = scaleQ, y = scaleSC, color = factor(fire))) +
  geom_point(alpha = 0.5) +
  geom_smooth(method = "lm", fill = NA) +
  theme_bw() +
  facet_wrap(vars(usgs_site), nrow = 4, scales = "free")
# At first glance, both negative, but slope definitely changes post-fire

# Ensure no NAs or NaNs are present in the data,
# because STAN does not allow this.
sum(is.na(dat4$scaleSC)) # 0
sum(is.na(dat4$scaleQ)) # 0
sum(is.na(dat4$fire)) # 0
sum(is.nan(dat4$scaleSC)) # 0
sum(is.nan(dat4$scaleQ)) # 0
sum(is.nan(dat4$fire)) # 0

##### Model Fit #####

# Split list by site
dat4_l <- split(dat4, dat4$usgs_site)

# Function to compile necessary data
stan_data_compile <- function(x){
  
  data <- list(
    N = nrow(x), # number of observations
    C = x$scaleSC[1:nrow(x)], # concentration
    Q = x$scaleQ[1:nrow(x)], # discharge
    f = x$fire[1:nrow(x)] # fire delineation
  )
  
  return(data)
  
}

# Apply function to dataset
data_stan4 <- lapply(dat4_l, function(x) stan_data_compile(x))

# Fit model - should run in 5 minutes
stan_lm_run4 <- lapply(data_stan4,
                       function(x) stan(file = "models/STAN_lm_delta_template.stan",
                         data = x,
                         chains = 3,
                         iter = 5000,
                         control = list(max_treedepth = 12))
                       )

# Examine summaries of the estimates.
stan_lm_data4 <- map(stan_lm_run4,
                          function(x) summary(x,
                           pars = c("A_pre", "A_post",
                                    "b_pre", "b_post",
                                    "sigma_post", "sigma_pre",
                                    "delta"),
                           probs = c(0.025, 0.5, 0.975))$summary )

# Turn back into a dataframe for easier summary viewing.
stan_lm_data4_df <- plyr::ldply(stan_lm_data4, 
                        function(x) data.frame(names = row.names(x), x)) %>%
  rename("parameter" = "names",
         usgs_site = `.id`)
# Rhat values all < 1.05 YESSS!!!

# Plot parameter estimates
color_scheme_set("purple")

# Trying to list through the stanfit object was
# proving tricky, so here are all the plots

mcmc_intervals(stan_lm_run4$`USGS-07103700`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # no change

mcmc_intervals(stan_lm_run4$`USGS-07105500`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.1 change

mcmc_intervals(stan_lm_run4$`USGS-07105800`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.1

mcmc_intervals(stan_lm_run4$`USGS-07106300`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.2

mcmc_intervals(stan_lm_run4$`USGS-07106500`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.1

mcmc_intervals(stan_lm_run4$`USGS-07109500`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.4 change but WIDE variation

mcmc_intervals(stan_lm_run4$`USGS-08313000`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.1

mcmc_intervals(stan_lm_run4$`USGS-08330000`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.25

mcmc_intervals(stan_lm_run4$`USGS-08354900`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.1

mcmc_intervals(stan_lm_run4$`USGS-08355490`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.2

mcmc_intervals(stan_lm_run4$`USGS-08358400`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.1

mcmc_intervals(stan_lm_run4$`USGS-09095500`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.5 but again HUGE variation

mcmc_intervals(stan_lm_run4$`USGS-09152500`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # -0.25

mcmc_intervals(stan_lm_run4$`USGS-09163500`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.5

mcmc_intervals(stan_lm_run4$`USGS-09261000`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # no change but wide variation

mcmc_intervals(stan_lm_run4$`USGS-09367540`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.4

mcmc_intervals(stan_lm_run4$`USGS-09367580`,
               pars = c("A_pre", "A_post", 
                        "b_pre", "b_post",
                        "delta"),
               point_est = "median",
               prob = 0.95) +
  labs(
    title = "Posterior distributions",
    subtitle = "with medians and 95% intervals"
  ) # +0.3

# End of script.
