# CQ fire model — brms implementation
# Converted from STAN_lm_multilevel_nc_template_revised2_covs.stan
#
# Model structure (mirrors Stan exactly):
#
#   log_N_sc[i] = a_g[group[i]] + b_g[group[i]] * log_Q_sc[i]
#               + (delta_a + beta_a1*z1[i] + beta_a2*z2[i]) * post[i]
#               + (delta_b + beta_b1*z1[i] + beta_b2*z2[i]) * post[i] * log_Q_sc[i]
#
# Parameter mapping (Stan → brms):
#   mu_a          → b_Intercept           (population pre-fire intercept)
#   mu_b          → b_log_Q_sc            (population pre-fire CQ slope)
#   delta_a       → b_post                (global intercept shift, at z1=z2=0)
#   delta_b       → b_log_Q_sc:post       (global slope shift, at z1=z2=0)
#   beta_a1       → b_post:z1             (burn% intercept shift)
#   beta_a2       → b_post:z2             (fire area intercept shift)
#   beta_b1       → b_log_Q_sc:post:z1    (burn% slope shift)
#   beta_b2       → b_log_Q_sc:post:z2    (fire area slope shift)
#   a_g, b_g      → r_na_l3name[, Intercept/log_Q_sc]  (group random effects)
#
# Variables:
#   log_N_sc   — standardized log(nitrate)
#   log_Q_sc   — standardized log(flow)
#   post       — 0 = pre-fire, 1 = post-fire  (from 'segment' column)
#   z1         — std(cum_per_cent_burned)
#   z2         — std(log1p(cum_fire_area))
#   na_l3name  — EPA Level III ecoregion (grouping factor)
#
# NOTE: brms models correlated random intercept + slope via (log_Q_sc | na_l3name).
# The Stan model treated them as independent. To match Stan exactly, swap to
# (log_Q_sc || na_l3name) and remove the lkj(2) prior. The correlated version
# is kept here as it is generally the better-identified choice.

#### Setup ####

library(tidyverse)
library(ggplot2)
library(brms)
library(tidybayes)
library(ggrepel)

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

#### Data Loading ####

nitrate <- read_csv("data/processed/nitrate_largest_pre_post_covariates.csv",
                    na = c(".", "-999", "NA"))
problems(nitrate)
nitrate
summary(nitrate)

#### Data Preparation ####

# Log-transform, create fire indicator, prepare covariates
scaled <- nitrate %>%
  filter(flow > 0, value_std > 0) %>%
  mutate(
    log_N  = log(value_std),
    log_Q  = log(flow),
    # Fire indicator: 0 = pre-fire, 1 = post-fire (matches Stan's f vector)
    post   = as.integer(segment == "after"),
    z1_raw = cum_per_cent_burned,
    z2_raw = log1p(cum_fire_area)         # log1p to handle skew and unit ambiguity
  ) %>%
  filter(!is.na(z1_raw), !is.na(z2_raw))

# Standardize x, y, z1, z2 so Stan-derived priors are appropriate (all ~unit scale).
# Save scale constants — multiply by y_sd and add y_mean to back-transform predictions.
x_mean  <- mean(scaled$log_Q);  x_sd  <- sd(scaled$log_Q)
y_mean  <- mean(scaled$log_N);  y_sd  <- sd(scaled$log_N)
z1_mean <- mean(scaled$z1_raw); z1_sd <- sd(scaled$z1_raw)
z2_mean <- mean(scaled$z2_raw); z2_sd <- sd(scaled$z2_raw)

scaled <- scaled %>%
  mutate(
    log_Q_sc = (log_Q - x_mean) / x_sd,
    log_N_sc = (log_N - y_mean) / y_sd,
    z1       = (z1_raw - z1_mean) / z1_sd,
    z2       = (z2_raw - z2_mean) / z2_sd
  )

colnames(scaled)

# Sanity check: segment breakdown per ecoregion
scaled %>% count(segment, post)
scaled %>% count(na_l3name, post)

#### Exploratory Data Analysis ####

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

# Covariate distributions
scaled %>%
  ggplot(aes(x = z1, y = log_N_sc)) + geom_point() +
  ggtitle("Nitrate vs std(cum_per_cent_burned) — z1")

scaled %>%
  ggplot(aes(x = z2, y = log_N_sc)) + geom_point() +
  ggtitle("Nitrate vs std(log1p(cum_fire_area)) — z2")

scaled %>%
  ggplot(aes(x = na_l3name, y = log_N_sc)) + geom_boxplot() +
  ggtitle("Nitrate by ecoregion")

#### Model Specification ####

# Run get_prior() first to confirm brms coefficient names before setting priors.
# If any coef= strings below cause a warning, adjust them to match fixef(fit1) output.
brms_formula <- bf(
  log_N_sc ~ log_Q_sc + post + post:z1 + post:z2 +
    log_Q_sc:post + log_Q_sc:post:z1 + log_Q_sc:post:z2 +
    (log_Q_sc | na_l3name)
)

get_prior(brms_formula, data = scaled)

# Priors matched to the Stan model.
# Stan used T[0,] half-normal for SDs; brms truncates sd/sigma at 0 automatically.
priors <- c(
  prior(normal(0, 1.5), class = Intercept),                          # mu_a
  prior(normal(0, 1.5), class = b, coef = log_Q_sc),                 # mu_b
  prior(normal(0, 0.5), class = b, coef = post),                     # delta_a
  prior(normal(0, 0.3), class = b, coef = "post:z1"),                 # beta_a1
  prior(normal(0, 0.3), class = b, coef = "post:z2"),                 # beta_a2
  prior(normal(0, 0.3), class = b, coef = "log_Q_sc:post"),           # delta_b
  prior(normal(0, 0.2), class = b, coef = "log_Q_sc:post:z1"),        # beta_b1
  prior(normal(0, 0.2), class = b, coef = "log_Q_sc:post:z2"),        # beta_b2
  prior(normal(0, 1),   class = sigma),                               # sigma_y
  prior(normal(0, 1),   class = sd),                                  # sigma_a, sigma_b
  prior(lkj(2),         class = cor)                                  # RE correlation
)

#### Model Fitting ####

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
  control = list(adapt_delta = 0.95, max_treedepth = 14)
)

fit1
summary(fit1, prob = 0.5)

#### Diagnostics ####

pp_check(fit1, ndraws = 50)
pp_check(fit1, type = "scatter_avg")

posterior_summary(fit1, probs = c(0.25, 0.75))

# Confirm coefficient names — use these to verify the prior coef= strings above
fixef(fit1)

bayes_R2(fit1)

#### Population-Level Pre / Post Summaries ####
# Evaluated at average covariates (z1 = z2 = 0 after standardization).
# Mirrors pop_50 and deltas_50 from the Stan workflow.

pop_50 <- fit1 %>%
  spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`) %>%
  # Rename to avoid column collision when creating post-fire quantities
  rename(delta_a = b_post,
         delta_b = `b_log_Q_sc:post`) %>%
  mutate(
    intercept_pre  = b_Intercept,
    intercept_post = b_Intercept + delta_a,
    slope_pre      = b_log_Q_sc,
    slope_post     = b_log_Q_sc + delta_b
  ) %>%
  pivot_longer(c(intercept_pre, intercept_post, slope_pre, slope_post),
               names_to = c("param", "period"), names_sep = "_",
               values_to = "value") %>%
  group_by(param, period) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

pop_50

# Global delta_a and delta_b at z1 = z2 = 0 (average fire exposure)
deltas_50 <- fit1 %>%
  spread_draws(b_post, `b_log_Q_sc:post`) %>%
  rename(delta_a = b_post,
         delta_b = `b_log_Q_sc:post`) %>%
  pivot_longer(c(delta_a, delta_b), names_to = "param", values_to = "value") %>%
  mutate(param = recode(param,
                        delta_a = "intercept shift (post - pre)",
                        delta_b = "slope shift (post - pre)")) %>%
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
       caption = "Evaluated at average covariates (z1 = z2 = 0)") +
  theme_bw()

#### Covariate Effects on the Post-Fire Shifts ####
# beta_a1, beta_a2: how burn% and fire area modulate the intercept shift
# beta_b1, beta_b2: how burn% and fire area modulate the slope shift
# Mirrors cov_50 from the Stan workflow.

cov_50 <- fit1 %>%
  spread_draws(`b_post:z1`, `b_post:z2`,
               `b_log_Q_sc:post:z1`, `b_log_Q_sc:post:z2`) %>%
  rename(beta_a1 = `b_post:z1`,
         beta_a2 = `b_post:z2`,
         beta_b1 = `b_log_Q_sc:post:z1`,
         beta_b2 = `b_log_Q_sc:post:z2`) %>%
  pivot_longer(c(beta_a1, beta_a2, beta_b1, beta_b2),
               names_to = "param", values_to = "value") %>%
  mutate(param = recode(param,
                        beta_a1 = "burn% \u2192 intercept shift",
                        beta_a2 = "fire area \u2192 intercept shift",
                        beta_b1 = "burn% \u2192 slope shift",
                        beta_b2 = "fire area \u2192 slope shift")) %>%
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
       title = "Covariate modulation of post-fire shifts") +
  theme_bw()

#### Group-Level Pre / Post CQ Slopes ####
# Computes total (fixed + random) slopes and intercepts per ecoregion,
# scaled by each group's actual mean fire exposure.
# Mirrors prepost_groups_50 from the Stan workflow.

# z1 and z2 are observation-level (cumulative, so they grow over the post-fire
# period). Using the mean of post-fire observations per group gives a representative
# "typical fire exposure" for that ecoregion.
burn_by_group <- scaled %>%
  filter(post == 1) %>%
  group_by(na_l3name) %>%
  summarise(z1 = mean(z1), z2 = mean(z2), .groups = "drop")

# Mixing scalar and array parameters in one spread_draws call creates duplicate
# key rows that break pivot_wider. Pull them separately and join on draw index.

fixed_draws <- fit1 %>%
  spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`,
               `b_post:z1`, `b_post:z2`,
               `b_log_Q_sc:post:z1`, `b_log_Q_sc:post:z2`) %>%
  rename(delta_a = b_post,
         delta_b = `b_log_Q_sc:post`,
         beta_a1 = `b_post:z1`,
         beta_a2 = `b_post:z2`,
         beta_b1 = `b_log_Q_sc:post:z1`,
         beta_b2 = `b_log_Q_sc:post:z2`)

# spread_draws with the [group, coef] two-index syntax calls spread() internally
# and fails when any group level name contains a comma (some ecoregion names do).
# Use ranef() for RE posterior medians instead — it handles all name formats
# reliably. The dominant uncertainty in pre→post shifts comes from the fixed-effect
# deltas (delta_a, delta_b, beta terms), which fixed_draws fully captures.
re_vals <- as.data.frame(ranef(fit1)$na_l3name[, "Estimate", ]) %>%
  rownames_to_column("na_l3name") %>%
  rename(re_a = Intercept, re_b = log_Q_sc) %>%
  left_join(burn_by_group, by = "na_l3name")

# crossing() gives one row per draw × group (~4000 × N_groups rows)
prepost_groups_50 <- fixed_draws %>%
  crossing(re_vals) %>%
  mutate(
    intercept_pre  = b_Intercept + re_a,
    intercept_post = b_Intercept + re_a + delta_a + beta_a1 * z1 + beta_a2 * z2,
    slope_pre      = b_log_Q_sc  + re_b,
    slope_post     = b_log_Q_sc  + re_b + delta_b + beta_b1 * z1 + beta_b2 * z2
  ) %>%
  pivot_longer(c(intercept_pre, intercept_post, slope_pre, slope_post),
               names_to = c("param", "period"), names_sep = "_",
               values_to = "value") %>%
  group_by(na_l3name, param, period) %>%
  median_qi(value, .width = 0.50) %>%
  ungroup() %>%
  rename(median = value, lo50 = .lower, hi50 = .upper)

prepost_groups_50

# Slopes pre vs post, faceted by period
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

# Scatter: per-group slopes pre vs post (points above the 1:1 line = increased slope)
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
       title = "Group CQ slopes: pre vs post fire",
       caption = "Points above dashed line = increased slope post-fire") +
  theme_bw()

#### Delta Mu: Burn Effect at Different Flow Conditions ####
# Population-level: expected shift in log_N_sc going from pre to post fire,
# evaluated at average covariates (z1 = z2 = 0) and ±1 SD of flow.
# Mirrors delta_mu_50 from the Stan workflow.
# Since log_Q_sc is standardized, ±1 SD = ±1.

x_star <- c(-1, 0, 1)

delta_mu_50 <- fit1 %>%
  spread_draws(b_post, `b_log_Q_sc:post`) %>%
  rename(delta_a = b_post,
         delta_b = `b_log_Q_sc:post`) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    delta_mu = delta_a + delta_b * x
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
       caption = "Population-level; evaluated at average covariates (z1 = z2 = 0)") +
  theme_bw()

# Group-level delta mu: shift scaled by each group's actual z1, z2.
# Mirrors delta_mu_site_50 from the Stan workflow.

delta_mu_site_50 <- fit1 %>%
  spread_draws(b_post, `b_log_Q_sc:post`,
               `b_post:z1`, `b_post:z2`,
               `b_log_Q_sc:post:z1`, `b_log_Q_sc:post:z2`) %>%
  rename(delta_a = b_post,
         delta_b = `b_log_Q_sc:post`,
         beta_a1 = `b_post:z1`,
         beta_a2 = `b_post:z2`,
         beta_b1 = `b_log_Q_sc:post:z1`,
         beta_b2 = `b_log_Q_sc:post:z2`) %>%
  crossing(burn_by_group) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    # Total post-pre shift at this group's actual fire exposure and flow level
    delta_mu = (delta_a + beta_a1 * z1 + beta_a2 * z2) +
      (delta_b + beta_b1 * z1 + beta_b2 * z2) * x
  ) %>%
  group_by(na_l3name, flow_condition, x, z1, z2) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(na_l3name, x)

delta_mu_site_50 %>%
  mutate(
    flow_condition = fct_reorder(flow_condition, x),
    na_l3name      = fct_reorder(na_l3name, z1)   # order by burn extent
  ) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~na_l3name, ncol = 3) +
  labs(x = "\u0394 log-N (post \u2212 pre, median & 50% CrI)", y = NULL,
       title = "Burn effect on nitrate by ecoregion and flow condition",
       caption = "Ecoregions ordered by std(cum_per_cent_burned)") +
  theme_bw() +
  theme(strip.text = element_text(size = 7))

#### Predicted Post-Fire CQ Curves Across Burn Extent ####
# Population-level predictions varying z1 (burn%) with z2 (fire area) held at 0.
# Shows how the post-fire CQ line shifts as more of the watershed burns.
# All values are on the standardized log scale; to back-transform:
#   log_N = log_N_sc * y_sd + y_mean  (natural log scale)

z1_seq <- seq(min(scaled$z1), max(scaled$z1), length.out = 5)

fit1 %>%
  spread_draws(b_Intercept, b_log_Q_sc, b_post, `b_log_Q_sc:post`,
               `b_post:z1`, `b_log_Q_sc:post:z1`) %>%
  rename(delta_a = b_post,
         delta_b = `b_log_Q_sc:post`,
         beta_a1 = `b_post:z1`,
         beta_b1 = `b_log_Q_sc:post:z1`) %>%
  crossing(
    z1       = z1_seq,
    log_Q_sc = seq(min(scaled$log_Q_sc), max(scaled$log_Q_sc), length.out = 50)
  ) %>%
  mutate(
    # Post-fire predicted mean (z2 = 0, post = 1)
    log_N_sc  = b_Intercept + b_log_Q_sc * log_Q_sc +
      (delta_a + beta_a1 * z1) +
      (delta_b + beta_b1 * z1) * log_Q_sc,
    # Back-transform z1 label for display
    pct_burned = round(z1 * z1_sd + z1_mean, 1)
  ) %>%
  group_by(pct_burned, log_Q_sc) %>%
  median_qi(log_N_sc, .width = 0.50) %>%
  ggplot(aes(log_Q_sc, log_N_sc,
             color = factor(pct_burned),
             fill  = factor(pct_burned))) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.8) +
  scale_color_viridis_d(name = "Cum. % burned", option = "plasma") +
  scale_fill_viridis_d(name = "Cum. % burned", option = "plasma") +
  labs(x       = "log flow (standardized)",
       y       = "log nitrate (standardized)",
       title   = "Post-fire CQ relationships across burn extent",
       caption = "Population-level predictions; fire area (z2) held at mean; ribbons = 50% CrI") +
  theme_bw()
