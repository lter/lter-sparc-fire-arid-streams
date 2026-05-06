# Data exploration for fire models

library(tidyverse)
library(ggplot2)
library(brms)
library(tidybayes)

# This sets a global theme for all my plots. 
theme_set(theme_bw() +
            theme(
              plot.background = element_blank()
              ,panel.grid.major = element_blank()
              ,panel.grid.minor = element_blank()
              ,panel.background = element_blank()
              ,panel.border = element_blank()
              ,axis.text.x  = element_text(angle=90, vjust=0.5, size=8)
              ,axis.ticks = element_blank()
              ,strip.background = element_rect()
            ))

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

# The data we are looking at has already been processed so it's in the processed folder.

nitrate <- read_csv("data/processed/nitrate_largest_pre_post_covariates.csv", na = c('.','-999','NA'))
problems(nitrate)
nitrate
summary(nitrate)

nitrate %>% ggplot(., aes(x=value_std)) + geom_histogram() +
  ggtitle("Nitrate standardized value")

nitrate %>% ggplot(., aes(x=flow)) + geom_histogram() +
  ggtitle("Nitrate data set flow values")

# Need to scale the data
scaled <- nitrate %>% filter(flow > 0 & value_std > 0) %>% 
  # --- Log-only transforms ---
  mutate(
    log_flow = log(flow),           # log Q
    log_nitrate = log(value_std)       # log N
  )

scaled <- scaled |>
  mutate(burn_prop = all_per_cent_burned / 100)

colnames(scaled)

scaled %>% ggplot(., aes(x=log_flow, y=log_nitrate)) + geom_point() +
  ggtitle("Nitrate and river flow data")

scaled %>% ggplot(., aes(x=log_flow, y=log_nitrate)) + geom_point() +
  ggtitle("Nitrate and river flow data") +
  facet_wrap(~na_l3name)

scaled %>% ggplot(., aes(x=log_flow, y=log_nitrate, color=segment)) + geom_point() +
  ggtitle("Nitrate and river flow data") +
  facet_wrap(~na_l3name)


scaled %>% 
  group_by(na_l3name) %>% 
  summarise(n_sites = n_distinct(usgs_site)) %>% 
  arrange(desc(n_sites))

#
brms.mod.0 <- bf(
  log_nitrate ~ log_flow * burn_prop + (log_flow | na_l3name)
)

prior0 <- c(
  prior(normal(0, 1),    class = b),           # fixed effects (incl. cross-level interaction)
  prior(normal(0, 2),    class = Intercept),
  prior(exponential(1),  class = sigma),        # obs-level SD
  prior(exponential(1),  class = sd),           # SD of random effects
  prior(lkj(2),          class = cor)           # correlation of random intercept & slope
)

fit0 <- brm(brms.mod.0, 
            data = scaled,
            family = "gaussian", prior = prior0,
            chains = 4, cores = 8, iter = 2000, backend = "cmdstanr",
            control = list(adapt_delta = 0.95, max_treedepth = 14))
fit0

summary(fit0, prob = 0.5)

pp_check(fit0)
posterior_summary(fit0, probs = c(0.25, 0.75))

# Fixed effects: overall CQ slope and effect of %burn on slope
fixef(fit0)

# Site-specific CQ slopes (intercept + random deviation)
# This is the full posterior for each site's slope
coef(fit0)$na_l3name[ , , "log_flow"]

# How much variance does burn explain at the ecoregion level?
bayes_R2(fit0)

### CQ by ecoregion
# Quick - posterior median + 50% CrI per site
coef(fit0, probs = c(0.25, 0.75))$na_l3name[,,"log_flow"] %>%
  as.data.frame() %>%
  rownames_to_column("na_l3name") %>%
  arrange(Estimate)

###
# Full posterior decomposition of site-level CQ slopes
# Pull random slopes via ranef() - avoids the comma parsing issue entirely
random_slopes <- ranef(fit0, pars = "log_flow")$na_l3name[,,"log_flow"] %>%
  as.data.frame() %>%
  rownames_to_column("na_l3name") %>%
  select(na_l3name, r_na_l3name = Estimate)

# Now build the decomposition without spread_draws on the random effects
cq_slopes <- fit0 %>%
  spread_draws(b_log_flow, `b_log_flow:burn_prop`) %>%
  crossing(burn_by_site) %>%
  left_join(random_slopes, by = "na_l3name") %>%
  mutate(
    slope_population = b_log_flow,
    slope_burn       = `b_log_flow:burn_prop` * burn_prop,
    slope_residual   = r_na_l3name,
    slope_total      = slope_population + slope_burn + slope_residual
  ) %>%
  group_by(na_l3name, burn_prop) %>%
  median_qi(slope_total, slope_burn, slope_residual, .width = 0.50) %>%
  ungroup() %>%
  arrange(slope_total)

cq_slopes %>%
  mutate(na_l3name = fct_reorder(na_l3name, slope_total)) %>%
  ggplot(aes(x = slope_total, y = na_l3name)) +
  geom_point() +
  geom_errorbarh(aes(xmin = slope_total.lower, xmax = slope_total.upper), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(
    x     = "CQ slope (median & 50% CrI)",
    y     = NULL,
    title = "Site-level CQ slopes",
    caption = "Ordered by slope; dashed line = chemostatic boundary"
  ) +
  theme_bw()

### CQ Slope as a Function of Burn Proportion
fit0 %>%
  spread_draws(b_log_flow, `b_log_flow:burn_prop`) %>%
  crossing(burn_prop = seq(0, 1, by = 0.01)) %>%
  mutate(cq_slope = b_log_flow + `b_log_flow:burn_prop` * burn_prop) %>%
  group_by(burn_prop) %>%
  median_qi(cq_slope, .width = c(0.50, 0.90)) %>%
  ggplot(aes(burn_prop, cq_slope)) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper, group = .width), alpha = 0.2) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
  annotate("text", x = 0.05, y = 0.02, label = "mobilization", 
           color = "red", hjust = 0, size = 3) +
  annotate("text", x = 0.05, y = -0.02, label = "dilution", 
           color = "red", hjust = 0, size = 3) +
  labs(
    x     = "Proportion burned",
    y     = "CQ slope",
    title = "Fire shifts CQ from dilution to mobilization",
    caption = "Ribbons = 50% and 90% CrI"
  ) +
  theme_bw()

### Overlay on sites
fit0 %>%
  spread_draws(b_log_flow, `b_log_flow:burn_prop`) %>%
  crossing(burn_prop = seq(0, 1, by = 0.01)) %>%
  mutate(cq_slope = b_log_flow + `b_log_flow:burn_prop` * burn_prop) %>%
  group_by(burn_prop) %>%
  median_qi(cq_slope, .width = c(0.50, 0.90)) %>%
  ggplot(aes(burn_prop, cq_slope)) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper, group = .width), alpha = 0.2) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
  # overlay site-level slopes from the decomposition
  geom_pointrange(
    data = cq_slopes,
    aes(x = burn_prop, y = slope_total,
        ymin = slope_total.lower, ymax = slope_total.upper),
    color = "steelblue", size = 0.4
  ) +
  ggrepel::geom_text_repel(
    data = cq_slopes,
    aes(x = burn_prop, y = slope_total, label = na_l3name),
    size = 2.5, color = "steelblue"
  ) +
  labs(
    x     = "Proportion burned",
    y     = "CQ slope",
    title = "CQ slope vs. burn proportion",
    subtitle = "Line = population-level trend; points = ecoregion-level slopes (median & 50% CrI)",
    caption = "Dashed line = chemostatic boundary"
  ) +
  theme_bw()

### Predicted CQ a N percent burn
fit0 %>%
  spread_draws(b_Intercept, b_log_flow, b_burn_prop, `b_log_flow:burn_prop`) %>%
  crossing(
    burn_prop = c(0, 0.25, 0.50, 0.75, 1.0),
    log_flow  = seq(min(scaled$log_flow), max(scaled$log_flow), length.out = 50)
  ) %>%
  mutate(
    log_nitrate = b_Intercept + b_log_flow * log_flow +
      b_burn_prop * burn_prop +
      `b_log_flow:burn_prop` * log_flow * burn_prop
  ) %>%
  group_by(burn_prop, log_flow) %>%
  median_qi(log_nitrate, .width = 0.50) %>%
  ggplot(aes(log_flow, log_nitrate, 
             color = factor(burn_prop), 
             fill  = factor(burn_prop),
             group = factor(burn_prop))) +
  geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
  geom_line(linewidth = 0.8) +
  scale_color_viridis_d(name = "Burn proportion", option = "plasma") +
  scale_fill_viridis_d(name = "Burn proportion", option = "plasma") +
  labs(
    x     = "log flow",
    y     = "log nitrate",
    title = "CQ relationships across the burn percent",
    caption = "Population-level predictions; ribbons = 50% CrI"
  ) +
  theme_bw()


### Fun stuff below here pulled from our Stan model
# ±1 SD around mean log_flow (no idea what values should go here, not a watershed human)
sd_x  <- sd(scaled$log_flow)
x_star <- c(-sd_x, 0, sd_x)

# delta_mu: expected change in log_nitrate going from burn_prop=0 to burn_prop=1
# at low / mean / high flow
# b_burn_prop            = intercept shift due to full burn (analog of delta_a)
# b_log_flow:burn_prop   = slope shift due to full burn    (analog of delta_b)
delta_mu_50 <- fit0 %>%
  spread_draws(b_burn_prop, `b_log_flow:burn_prop`) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0 ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0 ~ "high flow (+1 SD)"
    ),
    # total effect of going from unburned to fully burned at this flow level
    delta_mu = b_burn_prop + `b_log_flow:burn_prop` * x
  ) %>%
  group_by(flow_condition, x) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(x)

delta_mu_50

# Plot it
delta_mu_50 %>%
  mutate(flow_condition = fct_reorder(flow_condition, x)) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(
    x     = "Δ log-nitrate (fully burned vs unburned, median & 50% CrI)",
    y     = NULL,
    title = "Burn effect on nitrate varies with flow conditions"
  ) +
  theme_bw()

#
# Get each ecoregion's actual burn_prop (one value per site)
burn_by_site <- scaled %>%
  group_by(na_l3name) %>%
  summarise(burn_prop = first(burn_prop), .groups = "drop")

# delta_mu for site j at flow x:
# (b_burn_prop + b_log_flow:burn_prop * x) * burn_prop_j
# random effects cancel in the burned vs unburned difference
delta_mu_site_50 <- fit0 %>%
  spread_draws(b_burn_prop, `b_log_flow:burn_prop`) %>%
  crossing(burn_by_site) %>%
  crossing(x = x_star) %>%
  mutate(
    flow_condition = case_when(
      x < 0  ~ "low flow (-1 SD)",
      x == 0 ~ "mean flow",
      x > 0  ~ "high flow (+1 SD)"
    ),
    # scale the population-level burn effect by this site's actual burn_prop
    delta_mu = (b_burn_prop + `b_log_flow:burn_prop` * x) * burn_prop
  ) %>%
  group_by(na_l3name, flow_condition, x, burn_prop) %>%
  median_qi(delta_mu, .width = 0.50) %>%
  ungroup() %>%
  rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
  arrange(na_l3name, x)

# Plot faceted by ecoregion, ordered by burn_prop
delta_mu_site_50 %>%
  mutate(
    flow_condition = fct_reorder(flow_condition, x),
    # order ecoregions by burn extent for readability
    na_l3name = fct_reorder(na_l3name, burn_prop)
  ) %>%
  ggplot(aes(x = median, y = flow_condition)) +
  geom_point() +
  geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~ na_l3name, ncol = 3) +
  labs(
    x     = "Δ log-nitrate (actual burn vs unburned, median & 50% CrI)",
    y     = NULL,
    title = "Burn effect on nitrate by ecoregion and flow condition",
    caption = "Ecoregions ordered by % burned; effect scaled by each site's actual burn extent"
  ) +
  theme_bw() +
  theme(strip.text = element_text(size = 7))
