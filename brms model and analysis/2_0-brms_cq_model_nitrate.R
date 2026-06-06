# CQ fire model — brms implementation
# Converted from STAN_lm_multilevel_nc_template_revised2_covs.stan
#
# Model structure:
#
#   log_N_sc[i] = baseline_intercept[group[i]] + baseline_cq_slope[group[i]] * log_Q_sc[i]
#               + (fire_intercept_shift + pct_burn_on_intercept * pct_burned[i]) * post[i]
#               + (fire_cq_slope_shift  + pct_burn_on_slope     * pct_burned[i]) * post[i] * log_Q_sc[i]
#
# Plain English parameter names (brms coefficient → label used in this script):
#   b_Intercept                  → baseline_intercept       (population pre-fire intercept)
#   b_log_Q_sc                   → baseline_cq_slope        (population pre-fire CQ slope)
#   b_post                       → fire_intercept_shift     (global intercept shift post-fire, at average burn)
#   b_log_Q_sc:post              → fire_cq_slope_shift      (global CQ slope shift post-fire, at average burn)
#   b_post:pct_burned            → pct_burn_on_intercept    (burn % intercept shift)
#   b_log_Q_sc:post:pct_burned   → pct_burn_on_slope        (burn % CQ slope shift)
#   r_na_l3name[, Intercept / log_Q_sc] → re_intercept / re_slope (ecoregion random effects)
#
# NOTE: z2 (fire area) was removed due to collinearity with percent burned.
# See supplementary Stan script for the original full parameterisation.

# NOTE: brms models correlated random intercept + slope via (log_Q_sc | na_l3name).

library(tidyverse)
library(ggplot2)
library(brms)
library(tidybayes)
library(ggrepel)
library(here)
library(viridis)

# Working directory should be set using Session -> Set Working Directory. Not hard coded.
# Better practices suggest your file structure look like this:
# .
# └── Project name/
#   ├── data/
#   │   ├── external
#   │   ├── interim
#   │   ├── processed
#   │   └── raw
#   ├── docs
#   ├── models
#   └── reports/
#       ├── images
#       └── graphs

theme_set(theme_bw() +
            theme(
              plot.background  = element_blank()
              ,panel.grid.major = element_blank()
              ,panel.grid.minor = element_blank()
              ,panel.background = element_blank()
              ,panel.border     = element_blank()
              ,axis.text.x      = element_text(angle = 90, vjust = 0.5, size = 8)
              ,axis.ticks       = element_blank()
              ,strip.background = element_rect()
            ))

# Data Loading 

#nitrate <- read_csv("data/processed/nitrate_largest_pre_post_covariates.csv",
#                    na = c(".", "-999", "NA"))

nitrate <- read_csv(here("brms model and analysis", "data", "processed", "nitrate_largest_pre_post_covariates.csv"),
                    na = c(".", "-999", "NA"))

problems(nitrate)
nitrate
summary(nitrate)

# Data Preparation

# Log-transform, create fire indicator, and standardize covariates.
# pct_burned = standardized cumulative percent burned (the key fire covariate).
scaled <- nitrate %>%
  filter(flow > 0, value_std > 0) %>%
  mutate(
    log_N        = log(value_std),
    log_Q        = log(flow),
    # Fire indicator: 0 = pre-fire, 1 = post-fire (matches Stan's f vector)
    post         = as.integer(segment == "after"),
    pct_burned_raw = cum_per_cent_burned
  ) %>%
  filter(!is.na(pct_burned_raw))

# Save scale constants so predictions can be back-transformed to natural units:
#   log_N  = log_N_sc  * y_sd + y_mean
#   log_Q  = log_Q_sc  * x_sd + x_mean
#   pct_burned (raw) = pct_burned * pct_burned_sd + pct_burned_mean
x_mean          <- mean(scaled$log_Q);        x_sd          <- sd(scaled$log_Q)
y_mean          <- mean(scaled$log_N);        y_sd          <- sd(scaled$log_N)
pct_burned_mean <- mean(scaled$pct_burned_raw); pct_burned_sd <- sd(scaled$pct_burned_raw)

scaled <- scaled %>%
  mutate(
    log_Q_sc  = (log_Q        - x_mean)          / x_sd,
    log_N_sc  = (log_N        - y_mean)           / y_sd,
    pct_burned = (pct_burned_raw - pct_burned_mean) / pct_burned_sd
  )

colnames(scaled)

# Sanity check: segment breakdown per ecoregion
scaled %>% count(segment, post)
scaled %>% count(na_l3name, post)

#Exploratory Data Analysis

scaled %>%
  ggplot(aes(x = log_N_sc)) + geom_histogram() +
  ggtitle("Nitrate (standardized log)")

scaled %>%
  ggplot(aes(x = log_Q_sc)) + geom_histogram() +
  ggtitle("Flow (standardized log)")

# Pre vs post CQ relationship
scaled %>%
  ggplot(aes(x = log_Q_sc, y = log_N_sc, color = segment)) +
  geom_point(alpha = 0.4) +
  scale_color_manual(values = c("before" = "#073642", "after" = "#839496")) +
  ggtitle("CQ data: pre vs post fire")

# Faceted by ecoregion
scaled %>%
  ggplot(aes(x = log_Q_sc, y = log_N_sc, color = segment)) +
  geom_point(alpha = 0.4) +
  scale_color_manual(values = c("before" = "#073642", "after" = "#839496")) +
  facet_wrap(~na_l3name) +
  ggtitle("CQ by ecoregion: pre vs post fire")

# Faceted by site
scaled %>%
  ggplot(aes(x = log_Q_sc, y = log_N_sc, color = segment)) +
  geom_point(alpha = 0.4) +
  scale_color_manual(values = c("before" = "lightblue", "after" = "darkred")) +
  facet_wrap(~usgs_site, scales = "free") +
  ggtitle("CQ by site: pre vs post fire")

# Percent burned vs nitrate
scaled %>%
  ggplot(aes(x = pct_burned, y = log_N_sc)) + geom_point() +
  ggtitle("Nitrate vs standardized cumulative percent burned")

scaled %>%
  ggplot(aes(x = na_l3name, y = log_N_sc)) + geom_boxplot() +
  ggtitle("Nitrate by ecoregion")

#Model Specification

# Run get_prior() first to confirm brms coefficient names before setting priors.
# If any coef= strings below cause a warning, adjust them to match fixef(fit1) output.
brms_formula <- bf(
  log_N_sc ~ log_Q_sc + post + post:pct_burned +
    log_Q_sc:post + log_Q_sc:post:pct_burned +
    (log_Q_sc | na_l3name)
)

get_prior(brms_formula, data = scaled)

# Priors matched to the Stan model.
# Stan used T[0,] half-normal for SDs; brms truncates sd/sigma at 0 automatically.
priors <- c(
  prior(normal(0, 1.5), class = Intercept),                                    # baseline intercept
  prior(normal(0, 1.5), class = b, coef = log_Q_sc),                           # baseline CQ slope
  prior(normal(0, 0.5), class = b, coef = post),                               # fire intercept shift
  prior(normal(0, 0.3), class = b, coef = "post:pct_burned"),                  # burn % effect on intercept shift
  prior(normal(0, 0.3), class = b, coef = "log_Q_sc:post"),                    # fire CQ slope shift
  prior(normal(0, 0.2), class = b, coef = "log_Q_sc:post:pct_burned"),         # burn % effect on CQ slope shift
  prior(normal(0, 1),   class = sigma),                                        # observation SD
  prior(normal(0, 1),   class = sd),                                           # ecoregion random effect SDs
  prior(lkj(2),         class = cor)                                           # RE intercept-slope correlation
)

#Model Fitting

fit1 <- brm(
  brms_formula,
  data    = scaled,
  family  = gaussian(),
  prior   = priors,
  chains  = 4,
  cores   = 8,
  iter    = 2000,
  warmup  = 500,
  backend = "cmdstanr",
  control = list(adapt_delta = 0.99, max_treedepth = 14)
)

fit1
summary(fit1, prob = 0.5)

#Diagnostics
pp_check(fit1, ndraws = 50)
pp_check(fit1, type = "scatter_avg")

posterior_summary(fit1, probs = c(0.25, 0.75))

# Confirm coefficient names — use these to verify the prior coef= strings above
fixef(fit1, probs = c(0.25, 0.75))

bayes_R2(fit1, probs = c(0.25, 0.75))

#Population-Level Pre / Post Summaries
# Evaluated at average covariates (pct_burned = 0 after standardization).

pop_50 <- fit1 %>%
  spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`) %>%
  rename(
    baseline_intercept   = b_Intercept,
    baseline_cq_slope    = b_log_Q_sc,
    fire_intercept_shift = b_post,
    fire_cq_slope_shift  = `b_log_Q_sc:post`
  ) %>%
  mutate(
    intercept_pre  = baseline_intercept,
    intercept_post = baseline_intercept + fire_intercept_shift,
    slope_pre      = baseline_cq_slope,
    slope_post     = baseline_cq_slope  + fire_cq_slope_shift
  ) %>%
  pivot_longer(c(intercept_pre, intercept_post, slope_pre, slope_post),
               names_to = c("param", "period"), names_sep = "_",
               values_to = "value") %>%
  group_by(param, period) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

pop_50

# Global fire intercept and slope shifts at average burn (pct_burned = 0)
deltas_50 <- fit1 %>%
  spread_draws(b_post, `b_log_Q_sc:post`) %>%
  rename(
    fire_intercept_shift = b_post,
    fire_cq_slope_shift  = `b_log_Q_sc:post`
  ) %>%
  pivot_longer(c(fire_intercept_shift, fire_cq_slope_shift),
               names_to = "param", values_to = "value") %>%
  mutate(param = recode(param,
                        fire_intercept_shift = "intercept shift (post \u2212 pre)",
                        fire_cq_slope_shift  = "CQ slope shift (post \u2212 pre)")) %>%
  group_by(param) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

deltas_50

ggplot(deltas_50, aes(x = median, y = param)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "Shift (median & 50% CrI)", y = NULL,
       title = "Population-level post-fire shifts",
       caption = "Evaluated at average burn extent (pct_burned = 0)") +
  theme_bw()

# Covariate Effects on the Post-Fire Shifts
# How percent burned modulates the intercept and CQ slope shifts.

cov_50 <- fit1 %>%
  spread_draws(`b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
  rename(
    pct_burn_on_intercept = `b_post:pct_burned`,
    pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  ) %>%
  pivot_longer(c(pct_burn_on_intercept, pct_burn_on_slope),
               names_to = "param", values_to = "value") %>%
  mutate(param = recode(param,
                        pct_burn_on_intercept = "burn % \u2192 intercept shift",
                        pct_burn_on_slope     = "burn % \u2192 CQ slope shift")) %>%
  group_by(param) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

cov_50

ggplot(cov_50, aes(x = median, y = param)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "Coefficient (median & 50% CrI)", y = NULL,
       title = "How burn extent modulates post-fire shifts") +
  theme_bw()

# Group-Level Pre / Post CQ Slopes
# Computes total (fixed + random) slopes and intercepts per ecoregion,
# scaled by each group's actual mean fire exposure.

# Use mean pct_burned across post-fire observations as representative fire
# exposure for each ecoregion.
burn_by_group <- scaled %>%
  filter(post == 1) %>%
  group_by(na_l3name) %>%
  summarise(pct_burned = mean(pct_burned), .groups = "drop")

# Full joint posterior draws: FE + RE combined via as_draws_df().
# Using as_draws_df() + regex avoids spread_draws name-parsing failures on
# ecoregion names containing commas and slashes, and correctly propagates
# random-effect uncertainty into all group-level summaries.
all_draws_df <- posterior::as_draws_df(fit1)

# Fixed-effect columns
fe_draws_full <- all_draws_df %>%
  as_tibble() %>%
  select(
    .draw,
    baseline_intercept    = b_Intercept,
    baseline_cq_slope     = b_log_Q_sc,
    fire_intercept_shift  = b_post,
    fire_cq_slope_shift   = `b_log_Q_sc:post`,
    pct_burn_on_intercept = `b_post:pct_burned`,
    pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  )

# Random intercept draws (long format, ecoregion name recovered from parameter string)
re_int_cols <- grep("r_na_l3name\\[.*,Intercept\\]", names(all_draws_df), value = TRUE)
re_int_long <- all_draws_df %>%
  as_tibble() %>%
  select(.draw, all_of(re_int_cols)) %>%
  pivot_longer(cols      = all_of(re_int_cols),
               names_to  = "param",
               values_to = "re_intercept") %>%
  mutate(na_l3name = str_extract(param, "(?<=\\[).*(?=,Intercept\\])") %>%
                     str_replace_all("\\.", " ")) %>%
  select(.draw, na_l3name, re_intercept)

# Random slope draws
re_slope_cols <- grep("r_na_l3name\\[.*,log_Q_sc\\]", names(all_draws_df), value = TRUE)
re_slope_long <- all_draws_df %>%
  as_tibble() %>%
  select(.draw, all_of(re_slope_cols)) %>%
  pivot_longer(cols      = all_of(re_slope_cols),
               names_to  = "param",
               values_to = "re_slope") %>%
  mutate(na_l3name = str_extract(param, "(?<=\\[).*(?=,log_Q_sc\\])") %>%
                     str_replace_all("\\.", " ")) %>%
  select(.draw, na_l3name, re_slope)

# One row per draw × ecoregion with FE + both REs + burn exposure
group_draws <- re_int_long %>%
  left_join(re_slope_long, by = c(".draw", "na_l3name")) %>%
  left_join(fe_draws_full, by = ".draw") %>%
  left_join(burn_by_group, by = "na_l3name")

prepost_groups_50 <- group_draws %>%
  mutate(
    intercept_pre  = baseline_intercept + re_intercept,
    intercept_post = baseline_intercept + re_intercept +
                     fire_intercept_shift + pct_burn_on_intercept * pct_burned,
    slope_pre      = baseline_cq_slope  + re_slope,
    slope_post     = baseline_cq_slope  + re_slope +
                     fire_cq_slope_shift + pct_burn_on_slope * pct_burned
  ) %>%
  pivot_longer(c(intercept_pre, intercept_post, slope_pre, slope_post),
               names_to = c("param", "period"), names_sep = "_",
               values_to = "value") %>%
  group_by(na_l3name, param, period) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

prepost_groups_50

# CQ slopes pre vs post, faceted by period
prepost_groups_50 %>%
  filter(param == "slope") %>%
  ggplot(aes(x = median, y = fct_reorder(na_l3name, median))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_grid(period ~ .) +
  labs(x = "CQ slope (median & 50% CrI)", y = NULL,
       title = "Pre vs post fire CQ slopes by ecoregion") +
  theme_bw()

# Scatter: per-ecoregion slopes pre vs post (points above 1:1 line = increased slope post-fire)
grp_slopes <- prepost_groups_50 %>%
  filter(param == "slope") %>%
  select(na_l3name, period, median, lo50, hi50) %>%
  pivot_wider(names_from = period,
              values_from = c(median, lo50, hi50))

ggplot(grp_slopes, aes(x = median_pre, y = median_post, label = na_l3name)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_point() +
  geom_text_repel(size = 3) +
  labs(x = "Pre-fire slope (median)", y = "Post-fire slope (median)",
       title = "Ecoregion CQ slopes: pre vs post fire",
       caption = "Points above dashed line = increased slope post-fire") +
  theme_bw()

# Scatter: per-ecoregion slopes pre vs post (points above 1:1 line = increased slope post-fire)
# mean error bars on both axes — horizontal for the pre-fire interval, vertical for post-fire.
grp_slopes <- prepost_groups_50 %>%
  filter(param == "slope") %>%
  select(na_l3name, period, median, lo50, hi50) %>%
  pivot_wider(names_from = period,
              values_from = c(median, lo50, hi50))

ggplot(grp_slopes, aes(x = median_pre, y = median_post, label = na_l3name)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_errorbar(aes(ymin = lo50_post, ymax = hi50_post),
                color = "grey60", linewidth = 0.5, width = 0) +
  geom_errorbarh(aes(xmin = lo50_pre, xmax = hi50_pre),
                 color = "grey60", linewidth = 0.5, height = 0) +
  geom_point() +
  geom_text_repel(size = 3) +
  labs(x = "Pre-fire slope (median)", y = "Post-fire slope (median)",
       title = "Ecoregion CQ slopes: pre vs post fire",
       caption = "Points above dashed line = increased slope post-fire; lines = 50% CrI") +
  theme_bw()

# Delta Mu: Burn Effect at Different Flow Conditions
# Population-level: expected shift in log_N_sc going from pre to post fire,
# evaluated at average burn (pct_burned = 0) and low / mean / high flow.
# Since log_Q_sc is standardized, ±1 SD = ±1.

x_star <- c(-1, 0, 1)

delta_mu_50 <- fit1 %>%
  spread_draws(b_post, `b_log_Q_sc:post`) %>%
  rename(
    fire_intercept_shift = b_post,
    fire_cq_slope_shift  = `b_log_Q_sc:post`
  ) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    delta_mu = fire_intercept_shift + fire_cq_slope_shift * x
  ) %>%
  group_by(flow_condition, x) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(x)

delta_mu_50

delta_mu_50 %>%
  mutate(flow_condition = fct_reorder(flow_condition, x)) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "\u0394 log-N (post \u2212 pre, median & 50% CrI)", y = NULL,
       title = "Burn effect on nitrate varies with flow conditions",
       caption = "Population-level; evaluated at average burn extent (pct_burned = 0)") +
  theme_bw()

# Ecoregion-level delta mu: shift scaled by each group's actual burn exposure.
delta_mu_site_50 <- fit1 %>%
  spread_draws(b_post, `b_log_Q_sc:post`,
               `b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
  rename(
    fire_intercept_shift  = b_post,
    fire_cq_slope_shift   = `b_log_Q_sc:post`,
    pct_burn_on_intercept = `b_post:pct_burned`,
    pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  ) %>%
  crossing(burn_by_group) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    # Total post-pre shift at this ecoregion's actual burn exposure and flow level
    delta_mu = (fire_intercept_shift + pct_burn_on_intercept * pct_burned) +
      (fire_cq_slope_shift  + pct_burn_on_slope     * pct_burned) * x
  ) %>%
  group_by(na_l3name, flow_condition, x, pct_burned) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(na_l3name, x)

delta_mu_site_50 %>%
  mutate(
    flow_condition = fct_reorder(flow_condition, x),
    na_l3name      = fct_reorder(na_l3name, pct_burned)  # order by burn extent
  ) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~na_l3name, ncol = 3) +
  labs(x = "\u0394 log-N (post \u2212 pre, median & 50% CrI)", y = NULL,
       title = "Burn effect on nitrate by ecoregion and flow condition",
       caption = "Ecoregions ordered by standardized percent burned") +
  theme_bw() +
  theme(strip.text = element_text(size = 7))

# Predicted Post-Fire CQ Curves Across Burn Extent
# Population-level predictions varying burn extent with flow. 
# All values are on the standardized log scale; to back-transform:
#   log_N = log_N_sc * y_sd + y_mean  (natural log scale)

pct_burned_seq <- seq(min(scaled$pct_burned), max(scaled$pct_burned), length.out = 5)

fit1 %>%
  spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`,
               `b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
  rename(
    baseline_intercept    = b_Intercept,
    baseline_cq_slope     = b_log_Q_sc,
    fire_intercept_shift  = b_post,
    fire_cq_slope_shift   = `b_log_Q_sc:post`,
    pct_burn_on_intercept = `b_post:pct_burned`,
    pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  ) %>%
  crossing(
    pct_burned = pct_burned_seq,
    log_Q_sc   = seq(min(scaled$log_Q_sc), max(scaled$log_Q_sc), length.out = 50)
  ) %>%
  mutate(
    # Post-fire predicted mean (post = 1)
    log_N_sc = baseline_intercept + baseline_cq_slope * log_Q_sc +
      (fire_intercept_shift + pct_burn_on_intercept * pct_burned) +
      (fire_cq_slope_shift  + pct_burn_on_slope     * pct_burned) * log_Q_sc,
    # Back-transform pct_burned label for display (raw percent)
    pct_label = round(pct_burned * pct_burned_sd + pct_burned_mean, 1)
  ) %>%
  group_by(pct_label, log_Q_sc) %>%
  median_qi(log_N_sc, .width = 0.50) %>%
  ggplot(aes(log_Q_sc, log_N_sc,
             color = factor(pct_label),
             fill  = factor(pct_label))) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.8) +
  scale_color_viridis_d(name = "Cum. % burned", option = "plasma") +
  scale_fill_viridis_d(name = "Cum. % burned", option = "plasma") +
  labs(x       = "log flow (standardized)",
       y       = "log nitrate (standardized)",
       title   = "Post-fire CQ relationships across burn extent",
       caption = "Population-level predictions; ribbons = 50% CrI") +
  theme_bw()

#Slope-Intercept Correlation Diagnostics
# In the CQ literature a negative rho is expected: sites with higher baseline
# concentrations (high intercept) tend toward chemostasis (slope near 0),
# while low-intercept sites show stronger mobilization or dilution signals.

# Posterior summary of the correlation paramete
rho_draws <- fit1 %>%
  spread_draws(cor_na_l3name__Intercept__log_Q_sc) %>%
  rename(rho = cor_na_l3name__Intercept__log_Q_sc)

rho_summary <- rho_draws %>%
  median_qi(rho, .width = c(0.50, 0.90)) %>%
  select(median = rho, lo50 = .lower, hi50 = .upper, .width)

rho_summary

# Directional probabilities
rho_draws %>%
  summarise(
    median_rho = median(rho),
    p_negative = mean(rho < 0),
    p_positive = mean(rho > 0)
  )

# Posterior density of rho
rho_draws %>%
  ggplot(aes(x = rho)) +
  geom_density(fill = "steelblue", alpha = 0.35, color = NA) +
  stat_pointinterval(aes(y = 0), .width = c(0.50, 0.90),
                     point_interval = median_qi,
                     point_size = 2) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  scale_x_continuous(limits = c(-1, 1)) +
  labs(
    x       = "Correlation: pre-fire intercept ~ CQ slope",
    y       = "Density",
    title   = "Posterior distribution of slope-intercept correlation",
    caption = "Point = median; thick/thin lines = 50% and 90% CrI; dashed = zero"
  ) +
  theme_bw()

# Ecoregion scatter: intercept vs slope with cross-hair intervals.
# Total group effect = population mean + random deviation, summarised from
# the full joint posterior. Trend line is not a formal estimate of ρ.
grp_params <- group_draws %>%
  mutate(
    total_intercept = baseline_intercept + re_intercept,
    total_slope     = baseline_cq_slope  + re_slope
  ) %>%
  group_by(na_l3name) %>%
  summarise(
    int_est   = median(total_intercept),
    int_lo    = quantile(total_intercept, 0.25),
    int_hi    = quantile(total_intercept, 0.75),
    slope_est = median(total_slope),
    slope_lo  = quantile(total_slope, 0.25),
    slope_hi  = quantile(total_slope, 0.75),
    .groups   = "drop"
  )

ggplot(grp_params, aes(x = int_est, y = slope_est, label = na_l3name)) +
  geom_errorbar(aes(ymin = slope_lo, ymax = slope_hi),
                color = "grey60", linewidth = 0.5, width = 0) +
  geom_errorbarh(aes(xmin = int_lo, xmax = int_hi),
                 color = "grey60", linewidth = 0.5, height = 0) +
  geom_smooth(method = "lm", se = FALSE,
              linetype = "dashed", color = "steelblue", linewidth = 0.7) +
  geom_point() +
  geom_text_repel(size = 3) +
  geom_hline(yintercept = 0, linetype = "dotted", color = "red") +
  labs(
    x       = "Pre-fire intercept (median)",
    y       = "Pre-fire CQ slope (median)",
    title   = "Slope-intercept relationship across ecoregions",
    subtitle = paste0("Population-level \u03c1 = ",
                      round(median(rho_draws$rho), 2),
                      "  [P(rho<0) = ",
                      round(mean(rho_draws$rho < 0), 2), "]"),
    caption = "Lines = 50% CrI; dashed trend = OLS; dotted = chemostatic boundary"
  ) +
  theme_bw()

################################################
### TKH attempt to fit delta-CQ @ site level ###
################################################
# Run get_prior() first to confirm brms coefficient names before setting priors.
# If any coef= strings below cause a warning, adjust them to match fixef(fit1) output.
brms_formula_site <- bf(log_N_sc ~ log_Q_sc + post + post:pct_burned +
                        log_Q_sc:post + log_Q_sc:post:pct_burned +
                        (log_Q_sc | usgs_site)
)

get_prior(brms_formula_site, data = scaled)

# Priors matched to the Stan model.
# Stan used T[0,] half-normal for SDs; brms truncates sd/sigma at 0 automatically.
priors <- c(
  prior(normal(0, 1.5), class = Intercept),                                    # baseline intercept
  prior(normal(0, 1.5), class = b, coef = log_Q_sc),                           # baseline CQ slope
  prior(normal(0, 0.5), class = b, coef = post),                               # fire intercept shift
  prior(normal(0, 0.3), class = b, coef = "post:pct_burned"),                  # burn % effect on intercept shift
  prior(normal(0, 0.3), class = b, coef = "log_Q_sc:post"),                    # fire CQ slope shift
  prior(normal(0, 0.2), class = b, coef = "log_Q_sc:post:pct_burned"),         # burn % effect on CQ slope shift
  prior(normal(0, 1),   class = sigma),                                        # observation SD
  prior(normal(0, 1),   class = sd),                                           # usgs_site random effect SDs
  prior(lkj(2),         class = cor)                                           # RE intercept-slope correlation
)

#Model Fitting

fit2 <- brm(
  brms_formula_site,
  data    = scaled,
  family  = gaussian(),
  prior   = priors,
  chains  = 4,
  cores   = 8,
  iter    = 2000,
  warmup  = 500,
  backend = "cmdstanr",
  control = list(adapt_delta = 0.99, max_treedepth = 14)
)

fit2
summary(fit2, prob = 0.5)

#Diagnostics
pp_check(fit2, ndraws = 50)
pp_check(fit2, type = "scatter_avg")

posterior_summary(fit2, probs = c(0.25, 0.75))

# Confirm coefficient names — use these to verify the prior coef= strings above
fixef(fit2, probs = c(0.25, 0.75))

bayes_R2(fit2, probs = c(0.25, 0.75))

#Population-Level Pre / Post Summaries
# Evaluated at average covariates (pct_burned = 0 after standardization).

pop_50_site <- fit2 %>% spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`) %>%
                        rename(baseline_intercept   = b_Intercept,
                               baseline_cq_slope    = b_log_Q_sc,
                               fire_intercept_shift = b_post,
                               fire_cq_slope_shift  = `b_log_Q_sc:post`
  ) %>%
                        mutate(intercept_pre  = baseline_intercept,
                               intercept_post = baseline_intercept + fire_intercept_shift,
                               slope_pre      = baseline_cq_slope,
                               slope_post     = baseline_cq_slope  + fire_cq_slope_shift
  ) %>%
                        pivot_longer(c(intercept_pre, intercept_post, slope_pre, slope_post), names_to = c("param", "period"), names_sep = "_", values_to = "value") %>%
                        group_by(param, period) %>%
                        median_qi(value, .width = 0.50) %>%
                        ungroup() %>%
                        rename(median = value, lo50 = .lower, hi50 = .upper)

pop_50_site

# Global fire intercept and slope shifts at average burn (pct_burned = 0)
deltas_50_site <- fit2 %>% spread_draws(b_post, `b_log_Q_sc:post`) %>%
                           rename(fire_intercept_shift = b_post,
                           fire_cq_slope_shift  = `b_log_Q_sc:post`
  ) %>%
                           pivot_longer(c(fire_intercept_shift, fire_cq_slope_shift), names_to = "param", values_to = "value") %>%
                           mutate(param = recode(param, fire_intercept_shift = "intercept shift (post \u2212 pre)", fire_cq_slope_shift  = "CQ slope shift (post \u2212 pre)")) %>%
                           group_by(param) %>%
                           median_qi(value, .width = 0.50) %>%
                           ungroup() %>%
                           rename(median = value, lo50 = .lower, hi50 = .upper)

deltas_50_site

sl.int.labs <- c("slope", "intercept")

ggplot(deltas_50_site, aes(x = median, y = param)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "Shift (median & 50% CrI)", y = NULL,
       title = "Population-level post-fire shifts",
       caption = "Evaluated at average burn extent (pct_burned = 0)") +
  theme_bw()

## presentation fig: Global slope & intercept shifts
globalshift.pl <- ggplot(deltas_50_site, aes(x = median, y = param)) +
                    geom_point(size = 5) +
                    geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0, linewidth = 2) +
                    geom_vline(xintercept = 0, linetype = "dashed") +
                    scale_y_discrete(labels = sl.int.labs) +
                    labs(x = "post-fire shift",
                         y = NULL) +
                    theme_bw() +
                    theme(legend.position = "none",
                          panel.grid.major = element_blank(),
                          panel.grid.minor = element_blank(),
                          panel.background = element_blank(),
                          panel.border = element_rect(colour = "black", fill = NA, linewidth = 2),
                          axis.text = element_text(size = 15),
                          axis.title = element_text(size = 15))

ggsave(globalshift.pl, path = here("brms model and analysis", "plots"), file = "globalshift_no3_site.pdf", height = 3, width = 3.5, units = "in")

# Covariate Effects on the Post-Fire Shifts
# How percent burned modulates the intercept and CQ slope shifts.

cov_50_site <- fit2 %>% spread_draws(`b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
                        rename(pct_burn_on_intercept = `b_post:pct_burned`,
                               pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  ) %>%
                        pivot_longer(c(pct_burn_on_intercept, pct_burn_on_slope),
                                     names_to = "param", values_to = "value") %>%
                        mutate(param = recode(param,
                        pct_burn_on_intercept = "burn % \u2192 intercept shift",
                        pct_burn_on_slope     = "burn % \u2192 CQ slope shift")) %>%
                        group_by(param) %>%
                        median_qi(value, .width = 0.50) %>%
                        ungroup() %>%
                        rename(median = value, lo50 = .lower, hi50 = .upper)

cov_50_site

ggplot(cov_50_site, aes(x = median, y = param)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "Coefficient (median & 50% CrI)", y = NULL,
       title = "How burn extent modulates post-fire shifts") +
  theme_bw()

## presentation figure: global % burn effect
pctburn_site.pl <- ggplot(cov_50_site, aes(x = median, y = param)) +
                      geom_point(size = 5) +
                      geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0, linewidth = 2) +
                      geom_vline(xintercept = 0, linetype = "dashed") +
                      scale_y_discrete(labels = sl.int.labs) +
                      labs(x = "effect of % catchment burned", 
                           y = NULL) +
                      theme_bw() +
                      theme(legend.position = "none",
                            plot.margin= margin(t = 5, r = 10, b = 5, l = 5),
                            panel.grid.major = element_blank(),
                            panel.grid.minor = element_blank(),
                            panel.background = element_blank(),
                            panel.border = element_rect(colour = "black", fill = NA, linewidth = 2),
                            axis.text = element_text(size = 15),
                            axis.title = element_text(size = 15))

ggsave(pctburn_site.pl, path = here("brms model and analysis", "plots"), file = "pctburn_no3_site.pdf", height = 3, width = 3.75, units = "in")

# Group-Level Pre / Post CQ Slopes
# Computes total (fixed + random) slopes and intercepts per ecoregion,
# scaled by each group's actual mean fire exposure.

# Use mean pct_burned across post-fire observations as representative fire
# exposure for each ecoregion.
burn_by_site <- scaled %>% filter(post == 1) %>%
                           group_by(usgs_site) %>%
                           summarise(pct_burned = mean(pct_burned), .groups = "drop")

# Full joint posterior draws: FE + RE combined via as_draws_df().
# Using as_draws_df() + regex avoids spread_draws name-parsing failures on
# ecoregion names containing commas and slashes, and correctly propagates
# random-effect uncertainty into all group-level summaries.
all_draws_site_df <- posterior::as_draws_df(fit2)

# Fixed-effect columns
fe_draws_site_full <- all_draws_site_df %>% as_tibble() %>%
                                            select(.draw,
                                                   baseline_intercept    = b_Intercept,
                                                   baseline_cq_slope     = b_log_Q_sc,
                                                   fire_intercept_shift  = b_post,
                                                   fire_cq_slope_shift   = `b_log_Q_sc:post`,
                                                   pct_burn_on_intercept = `b_post:pct_burned`,
                                                   pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  )

# Random intercept draws (long format, ecoregion name recovered from parameter string)
re_int_site_cols <- grep("r_usgs_site\\[.*,Intercept\\]", names(all_draws_site_df), value = TRUE)
re_int_site_long <- all_draws_site_df %>%
  as_tibble() %>%
  select(.draw, all_of(re_int_site_cols)) %>%
  pivot_longer(cols      = all_of(re_int_site_cols),
               names_to  = "param",
               values_to = "re_intercept") %>%
  mutate(usgs_site = str_extract(param, "(?<=\\[).*(?=,Intercept\\])") %>%
           str_replace_all("\\.", " ")) %>%
  select(.draw, usgs_site, re_intercept)

# Random slope draws
re_slope_site_cols <- grep("r_usgs_site\\[.*,log_Q_sc\\]", names(all_draws_site_df), value = TRUE)
re_slope_site_long <- all_draws_site_df %>%
  as_tibble() %>%
  select(.draw, all_of(re_slope_site_cols)) %>%
  pivot_longer(cols      = all_of(re_slope_site_cols),
               names_to  = "param",
               values_to = "re_slope") %>%
  mutate(usgs_site = str_extract(param, "(?<=\\[).*(?=,log_Q_sc\\])") %>%
           str_replace_all("\\.", " ")) %>%
  select(.draw, usgs_site, re_slope)

# One row per draw × ecoregion with FE + both REs + burn exposure
group_draws_site <- re_int_site_long %>%
  left_join(re_slope_site_long, by = c(".draw", "usgs_site")) %>%
  left_join(fe_draws_site_full, by = ".draw") %>%
  left_join(burn_by_site, by = "usgs_site")

prepost_site_50 <- group_draws_site %>% mutate(intercept_pre  = baseline_intercept + re_intercept,
                                               intercept_post = baseline_intercept + re_intercept +
                                               fire_intercept_shift + pct_burn_on_intercept * pct_burned,
                                               slope_pre      = baseline_cq_slope  + re_slope,
                                               slope_post     = baseline_cq_slope  + re_slope +
                                               fire_cq_slope_shift + pct_burn_on_slope * pct_burned
  ) %>%
                                       pivot_longer(c(intercept_pre, intercept_post, slope_pre, slope_post),
               names_to = c("param", "period"), names_sep = "_",
               values_to = "value") %>%
  group_by(usgs_site, param, period) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

prepost_site_50

# CQ slopes pre vs post, faceted by period
prepost_site_50 %>%
  filter(param == "slope") %>%
  ggplot(aes(x = median, y = fct_reorder(usgs_site, median))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_grid(period ~ .) +
  labs(x = "CQ slope (median & 50% CrI)", y = NULL,
       title = "Pre vs post fire CQ slopes by ecoregion") +
  theme_bw()

# Scatter: per-site slopes pre vs post (points above 1:1 line = increased slope post-fire)
site_slopes <- prepost_site_50 %>%
  filter(param == "slope") %>%
  select(usgs_site, period, median, lo50, hi50) %>%
  pivot_wider(names_from = period,
              values_from = c(median, lo50, hi50))

ggplot(site_slopes, aes(x = median_pre, y = median_post, label = usgs_site)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_point() +
  geom_text_repel(size = 3) +
  labs(x = "Pre-fire slope (median)", y = "Post-fire slope (median)",
       title = "Site CQ slopes: pre vs post fire",
       caption = "Points above dashed line = increased slope post-fire") +
  theme_bw()

# Scatter: per-ecoregion slopes pre vs post (points above 1:1 line = increased slope post-fire)
# mean error bars on both axes — horizontal for the pre-fire interval, vertical for post-fire.
ggplot(site_slopes, aes(x = median_pre, y = median_post, label = usgs_site)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_errorbar(aes(ymin = lo50_post, ymax = hi50_post),
                color = "grey60", linewidth = 0.5, width = 0) +
  geom_errorbarh(aes(xmin = lo50_pre, xmax = hi50_pre),
                 color = "grey60", linewidth = 0.5, height = 0) +
  geom_point() +
  geom_text_repel(size = 3) +
  labs(x = "Pre-fire slope (median)", y = "Post-fire slope (median)",
       title = "Site CQ slopes: pre vs post fire",
       caption = "Points above dashed line = increased slope post-fire; lines = 50% CrI") +
  theme_bw()

## presentation figure
site.prepost.sl.pl <- ggplot(site_slopes, aes(x = median_pre, y = median_post)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_errorbar(aes(ymin = lo50_post, ymax = hi50_post),
                color = "grey60", linewidth = 0.5, width = 0) +
  geom_errorbarh(aes(xmin = lo50_pre, xmax = hi50_pre),
                 color = "grey60", linewidth = 0.5, height = 0) +
  geom_point(aes(color = usgs_site)) +
  scale_color_viridis(option = "magma", discrete = TRUE) +
  labs(x = "pre-fire slope", y = "post-fire slope") +
  theme_bw() +
  theme(legend.position = "none",
        plot.margin= margin(t = 5, r = 10, b = 5, l = 5),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        panel.border = element_rect(colour = "black", fill = NA, linewidth = 2),
        axis.text = element_text(size = 15),
        axis.title = element_text(size = 15))

ggsave(site.prepost.sl.pl, path = here("brms model and analysis", "plots"), file = "site.slopes.prepost.pdf", height = 6, width = 6, units = "in")

# Delta Mu: Burn Effect at Different Flow Conditions
# Population-level: expected shift in log_N_sc going from pre to post fire,
# evaluated at average burn (pct_burned = 0) and low / mean / high flow.
# Since log_Q_sc is standardized, ±1 SD = ±1.

x_star <- c(-1, 0, 1)

delta_mu_50 <- fit2 %>%
  spread_draws(b_post, `b_log_Q_sc:post`) %>%
  rename(
    fire_intercept_shift = b_post,
    fire_cq_slope_shift  = `b_log_Q_sc:post`
  ) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    delta_mu = fire_intercept_shift + fire_cq_slope_shift * x
  ) %>%
  group_by(flow_condition, x) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(x)

delta_mu_50

delta_mu_50 %>%
  mutate(flow_condition = fct_reorder(flow_condition, x)) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "\u0394 log-N (post \u2212 pre, median & 50% CrI)", y = NULL,
       title = "Burn effect on nitrate varies with flow conditions",
       caption = "Population-level; evaluated at average burn extent (pct_burned = 0)") +
  theme_bw()

# Site-level delta mu: shift scaled by each group's actual burn exposure.
delta_mu_site_50 <- fit2 %>%
  spread_draws(b_post, `b_log_Q_sc:post`,
               `b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
  rename(
    fire_intercept_shift  = b_post,
    fire_cq_slope_shift   = `b_log_Q_sc:post`,
    pct_burn_on_intercept = `b_post:pct_burned`,
    pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  ) %>%
  crossing(burn_by_site) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    # Total post-pre shift at this ecoregion's actual burn exposure and flow level
    delta_mu = (fire_intercept_shift + pct_burn_on_intercept * pct_burned) +
      (fire_cq_slope_shift  + pct_burn_on_slope     * pct_burned) * x
  ) %>%
  group_by(usgs_site, flow_condition, x, pct_burned) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(usgs_site, x)

delta_mu_site_50 %>%
  mutate(
    flow_condition = fct_reorder(flow_condition, x),
    na_l3name      = fct_reorder(usgs_site, pct_burned)  # order by burn extent
  ) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~usgs_site, ncol = 3) +
  labs(x = "\u0394 log-N (post \u2212 pre, median & 50% CrI)", y = NULL,
       title = "Burn effect on nitrate by site and flow condition",
       caption = "Ecoregions ordered by standardized percent burned") +
  theme_bw() +
  theme(strip.text = element_text(size = 7))

# Predicted Post-Fire CQ Curves Across Burn Extent
# Population-level predictions varying burn extent with flow. 
# All values are on the standardized log scale; to back-transform:
#   log_N = log_N_sc * y_sd + y_mean  (natural log scale)

pct_burned_seq <- seq(min(scaled$pct_burned), max(scaled$pct_burned), length.out = 5)

fit2 %>%
  spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`,
               `b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
  rename(
    baseline_intercept    = b_Intercept,
    baseline_cq_slope     = b_log_Q_sc,
    fire_intercept_shift  = b_post,
    fire_cq_slope_shift   = `b_log_Q_sc:post`,
    pct_burn_on_intercept = `b_post:pct_burned`,
    pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`
  ) %>%
  crossing(
    pct_burned = pct_burned_seq,
    log_Q_sc   = seq(min(scaled$log_Q_sc), max(scaled$log_Q_sc), length.out = 50)
  ) %>%
  mutate(
    # Post-fire predicted mean (post = 1)
    log_N_sc = baseline_intercept + baseline_cq_slope * log_Q_sc +
      (fire_intercept_shift + pct_burn_on_intercept * pct_burned) +
      (fire_cq_slope_shift  + pct_burn_on_slope     * pct_burned) * log_Q_sc,
    # Back-transform pct_burned label for display (raw percent)
    pct_label = round(pct_burned * pct_burned_sd + pct_burned_mean, 1)
  ) %>%
  group_by(pct_label, log_Q_sc) %>%
  median_qi(log_N_sc, .width = 0.50) %>%
  ggplot(aes(log_Q_sc, log_N_sc,
             color = factor(pct_label),
             fill  = factor(pct_label))) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.8) +
  scale_color_viridis_d(name = "Cum. % burned", option = "plasma") +
  scale_fill_viridis_d(name = "Cum. % burned", option = "plasma") +
  labs(x       = "log flow (standardized)",
       y       = "log nitrate (standardized)",
       title   = "Post-fire CQ relationships across burn extent",
       caption = "Population-level predictions; ribbons = 50% CrI") +
  theme_bw()

# presentation figure
burn_site_eff.pl <- fit2 %>% spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`,
               `b_post:pct_burned`, `b_log_Q_sc:post:pct_burned`) %>%
                              rename(baseline_intercept    = b_Intercept,
                                     baseline_cq_slope     = b_log_Q_sc,
                                     fire_intercept_shift  = b_post,
                                     fire_cq_slope_shift   = `b_log_Q_sc:post`,
                                     pct_burn_on_intercept = `b_post:pct_burned`,
                                     pct_burn_on_slope     = `b_log_Q_sc:post:pct_burned`) %>%
                              crossing(pct_burned = pct_burned_seq,
                                       log_Q_sc   = seq(min(scaled$log_Q_sc), max(scaled$log_Q_sc), length.out = 50)) %>%
                             mutate(# Post-fire predicted mean (post = 1)
                                    log_N_sc = baseline_intercept + baseline_cq_slope * log_Q_sc +
      (fire_intercept_shift + pct_burn_on_intercept * pct_burned) +
      (fire_cq_slope_shift  + pct_burn_on_slope     * pct_burned) * log_Q_sc,
    # Back-transform pct_burned label for display (raw percent)
    pct_label = round(pct_burned * pct_burned_sd + pct_burned_mean, 1)
  ) %>%
  group_by(pct_label, log_Q_sc) %>%
  median_qi(log_N_sc, .width = 0.50) %>%
  ggplot(aes(log_Q_sc, log_N_sc,
             color = factor(pct_label),
             fill  = factor(pct_label))) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.8) +
  scale_color_viridis_d(option = "plasma", name = "% burned") +
  scale_fill_viridis_d(option = "plasma", name = "% burned") +
  labs(x       = "log-discharge (standardized)",
       y       = "log-nitrate (standardized)") +
  theme_bw() +
  theme(legend.position = c(0.85, 0.175),
        legend.text = element_text(size = 20),
        legend.title = element_text(size = 20),
        plot.margin= margin(t = 5, r = 10, b = 5, l = 5),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.background = element_blank(),
        panel.border = element_rect(colour = "black", fill = NA, linewidth = 2),
        axis.text = element_text(size = 20),
        axis.title = element_text(size = 20))

ggsave(burn_site_eff.pl, path = here("brms model and analysis", "plots"), file = "site.pctburn.sl.pdf", height = 6, width = 6.5, units = "in")


#Slope-Intercept Correlation Diagnostics
# In the CQ literature a negative rho is expected: sites with higher baseline
# concentrations (high intercept) tend toward chemostasis (slope near 0),
# while low-intercept sites show stronger mobilization or dilution signals.

# Posterior summary of the correlation paramete
rho_site_draws <- fit2 %>%
  spread_draws(cor_usgs_site__Intercept__log_Q_sc) %>%
  rename(rho = cor_usgs_site__Intercept__log_Q_sc)

rho_site_summary <- rho_site_draws %>%
  median_qi(rho, .width = c(0.50, 0.90)) %>%
  select(median = rho, lo50 = .lower, hi50 = .upper, .width)

rho_site_summary

# Directional probabilities
rho_site_draws %>%
  summarise(
    median_rho = median(rho),
    p_negative = mean(rho < 0),
    p_positive = mean(rho > 0)
  )

# Posterior density of rho
rho_site_draws %>%
  ggplot(aes(x = rho)) +
  geom_density(fill = "steelblue", alpha = 0.35, color = NA) +
  stat_pointinterval(aes(y = 0), .width = c(0.50, 0.90),
                     point_interval = median_qi,
                     point_size = 2) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
  scale_x_continuous(limits = c(-1, 1)) +
  labs(
    x       = "Correlation: pre-fire intercept ~ CQ slope",
    y       = "Density",
    title   = "Posterior distribution of slope-intercept correlation",
    caption = "Point = median; thick/thin lines = 50% and 90% CrI; dashed = zero"
  ) +
  theme_bw()

# Site scatter: intercept vs slope with cross-hair intervals.
# Total group effect = population mean + random deviation, summarised from
# the full joint posterior. Trend line is not a formal estimate of ρ.
grp_params <- group_draws_site %>%
  mutate(
    total_intercept = baseline_intercept + re_intercept,
    total_slope     = baseline_cq_slope  + re_slope
  ) %>%
  group_by(usgs_site) %>%
  summarise(
    int_est   = median(total_intercept),
    int_lo    = quantile(total_intercept, 0.25),
    int_hi    = quantile(total_intercept, 0.75),
    slope_est = median(total_slope),
    slope_lo  = quantile(total_slope, 0.25),
    slope_hi  = quantile(total_slope, 0.75),
    .groups   = "drop"
  )

ggplot(grp_params, aes(x = int_est, y = slope_est, label = usgs_site)) +
  geom_errorbar(aes(ymin = slope_lo, ymax = slope_hi),
                color = "grey60", linewidth = 0.5, width = 0) +
  geom_errorbarh(aes(xmin = int_lo, xmax = int_hi),
                 color = "grey60", linewidth = 0.5, height = 0) +
  geom_smooth(method = "lm", se = FALSE,
              linetype = "dashed", color = "steelblue", linewidth = 0.7) +
  geom_point() +
  geom_text_repel(size = 3) +
  geom_hline(yintercept = 0, linetype = "dotted", color = "red") +
  labs(
    x       = "Pre-fire intercept (median)",
    y       = "Pre-fire CQ slope (median)",
    title   = "Slope-intercept relationship across ecoregions",
    subtitle = paste0("Population-level \u03c1 = ",
                      round(median(rho_site_draws$rho), 2),
                      "  [P(rho<0) = ",
                      round(mean(rho_site_draws$rho < 0), 2), "]"),
    caption = "Lines = 50% CrI; dashed trend = OLS; dotted = chemostatic boundary"
  ) +
  theme_bw()
