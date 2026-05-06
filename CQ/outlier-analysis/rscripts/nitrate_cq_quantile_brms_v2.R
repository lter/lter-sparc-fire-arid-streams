# =============================================================================
# Nitrate C-Q residuals — Quantile regression (tau = 0.95) via brms
# Model: rs ~ dsf_scaled * burn_scaled + ecoregion + cq_status + (1|catchment)
# ============================================================================──────────────────────────────────────────────────────────────────
library(tidyverse)
library(brms)
library(tidybayes)
library(patchwork)

# ── Load & prep data ──────────────────────────────────────────────────────────
df <- read_csv("/Users/ash/Documents/projects/CRASS/nitrate_cq_residuals_with_ecoregion_.2slope.csv") %>%
  filter(days_since_last_fire > 0) %>%  # post-fire only
  mutate(
    catchment   = factor(usgs_site),
    # Reference ecoregion = COLD DESERTS (first alphabetically)
    # Relevel here if you want a different reference
    ecoregion   = factor(ecoregion_name),
    # Reference CQ status = neutral (chemostatic analog)
    cq_status   = factor(slopedir, levels = c("neutral", "positive", "negative")),
    log_n = log(value_std),
    log_q = log(Flow),
    #post  = as.integer(segment == "after"), 
    # Scale continuous predictors — important for MCMC mixing
    rs_scaled   = scale(rs)[, 1],
    slope_scaled = scale(slope)[, 1],
    dsf_scaled  = scale(days_since_last_fire)[, 1],
    burn_scaled = scale(total_burned)[, 1]
  )
colnames(df)
hist(df$post)
##get the means and sds for back-transforming later
x_mean <- mean(df$log_q)
x_sd   <- sd(df$log_q)
y_mean <- mean(df$log_n)
y_sd   <- sd(df$log_n)


scaled_df <- df %>%
  mutate(
    log_Q_sc  = (log_q        - x_mean)          / x_sd,
    log_N_sc  = (log_n        - y_mean)           / y_sd
    #pct_burned = (total_burned - brn_mean) / brn_sd,
    #dsf_scaled = (days_since_last_fire - dsf_mean) / dsf_sd
    )

colnames(scaled_df)

models_nitrate <- scaled_df %>%
  group_by(usgs_site) %>%
  mutate(rs = resid((lm(log_N_sc ~ log_Q_sc))),
         int = coef(lm(log_N_sc ~ log_Q_sc))[1],
         slope = coef(lm(log_N_sc ~ log_Q_sc))[2],
         slopedir = ifelse(slope >= .2, "positive", ifelse(slope <= -.2 , "negative", "neutral"))) %>% ungroup()
colnames(models_nitrate)

brn_mean <- mean(models_nitrate$total_burned)
brn_sd   <- sd(models_nitrate$total_burned)
dsf_mean <- mean(models_nitrate$days_since_last_fire)
dsf_sd   <- sd(models_nitrate$days_since_last_fire)

models_nitrate <- models_nitrate %>%
  mutate(
    pct_burned = (total_burned - brn_mean) / brn_sd,
    dsf_scaled = (days_since_last_fire - dsf_mean) / dsf_sd
  )
hist(models_nitrate$rs)
## ── Model formula & priors ───────────────────────────────────────────────────
brms_formula <- bf(
  rs ~ dsf_scaled + pct_burned + dsf_scaled:pct_burned +
      cq_status + (1 | ecoregion:usgs_site)
)
levels(models_nitrate$cq_status)
models_nitrate$cq_status <- relevel(factor(models_nitrate$cq_status), ref = "neutral")
##now use rs as the respones variable
get_prior(brms_formula, data = models_nitrate)[c(1:3)] #, family = asym_laplace())[1]

priors <- c(
  prior(normal(0, 1), class = b),                        # all slopes
  prior(normal(0, 1), class = Intercept),                # intercept        # specific slope
  prior(exponential(1), class = sd),                     # random effect SDs                        # random effect correlations
  prior(exponential(1), class = sigma)                   # residual SD
)
# ── Weakly informative priors ─────────────────────────────────────────────────
# priors <- c(
#   prior(normal(0, 1), class = b),        # all fixed effects
#   prior(exponential(1), class = sd),     # random intercept SD
#   prior(exponential(1), class = sigma)   # residual scale
# )
##ara's model formula for reference
# brms_formula <- bf(
#   log_N_sc ~ log_Q_sc + post + post:pct_burned +
#     log_Q_sc:post + log_Q_sc:post:pct_burned +
#     (log_Q_sc | na_l3name)
# )
# get_prior(brms_formula, data = scaled)[1]

# models_nitrate_postfire <- models_nitrate %>% 
#   filter(post == 1)

# head(models_nitrate_postfire)
# length(models_nitrate_postfire$rs)
# ── Fit model at tau = 0.95 ───────────────────────────────────────────────────
fit_q95 <- brm(
  formula = bf(
    rs ~ dsf_scaled + pct_burned + dsf_scaled:pct_burned +
    cq_status + (1 | ecoregion/usgs_site),
    quantile = 0.95
  ),
  family  = asym_laplace(),
  data    = models_nitrate,
  prior   = priors,
  chains  = 4,
  cores   = 4,
  iter    = 4000,
  warmup  = 2000,
  seed    = 42,
  threads = threading(4),
  backend = "cmdstanr",
  control = list(adapt_delta = 0.99, max_treedepth = 15),
  file    = "fit_q95_v4"   # caches to disk — delete file to force refit
)

# ── Diagnostics ───────────────────────────────────────────────────────────────
summary(fit_med)
summary(fit_q95)
# R-hat should be < 1.01 for all parameters
# Bulk ESS and Tail ESS should be > 400
plot(fit_med, ask = FALSE)
plot(fit_q95, ask = FALSE)

# Trace plots — check chains are mixing (should look like hairy caterpillars)
mcmc_plot(fit_95, type = "trace",
          variable = c("b_dsf_scaled", "b_pct_burned", 
                      "b_dsf_scaled:pct_burned")) 

# Pairs plot — check for funnel shapes indicating geometry problems
pairs(fit_95, variable = c("b_dsf_scaled", "b_pct_burned", 
                      "b_dsf_scaled:pct_burned")) 

# Posterior predictive check
pp_check(fit_95, ndraws = 100) +
  labs(title = "Posterior predictive check (tau = 0.95)")

# ── Also fit tau = 0.50 for comparison ────────────────────────────────────────
fit_q50 <- brm(
  formula = bf(
        rs ~ dsf_scaled + pct_burned + dsf_scaled:pct_burned +
    cq_status + (1 | ecoregion/usgs_site),
        quantile = 0.50
  ),
  family  = asym_laplace(),
  data    = models_nitrate,
  prior   = priors,
  chains  = 4,
  cores   = 4,
  iter    = 4000,
  warmup  = 2000,
  seed    = 42,
  threads = threading(4),
  backend = "cmdstanr",
  control = list(adapt_delta = 0.99, max_treedepth = 15),
  file    = "fit_q50_v4"
)

summary(fit_q50)
plot(fit_q50, ask = FALSE)

pp_check(fit_q50, ndraws = 100) +
  labs(title = "Posterior predictive check (tau = 0.50)")

pp_check(fit_q95, ndraws = 100) +
  labs(title = "Posterior predictive check: tau = 0.95")


### .05 quantile model
fit_q05 <- brm(
  formula = bf(
        rs ~ dsf_scaled + pct_burned + dsf_scaled:pct_burned +
    cq_status + (1 | ecoregion/usgs_site),
    quantile = 0.05
  ),
  family  = asym_laplace(), ##this is for quantiles
  data    = models_nitrate,
  prior   = priors,
  chains  = 4,
  cores   = 4,
  iter    = 4000,
  warmup  = 2000,
  seed    = 42,
  threads = threading(4),
  backend = "cmdstanr",
  control = list(adapt_delta = 0.99, max_treedepth = 15),
  #file    = "fit_q05"
)
plot(fit_q05, ask = FALSE)

pp_check(fit_q05, ndraws = 100) +
  labs(title = "Posterior predictive check (tau = 0.05)")

pp_check(fit_q05,type = "scatter_avg")

# ── Shared aesthetics ─────────────────────────────────────────────────────────
eco_colours <- c(
  "Cold Deserts"        = "#534AB7",
  "Mediterranean CA"    = "#1D9E75",
  "Upper Gila Mtns"     = "#D85A30",
  "Warm Deserts"        = "#BA7517",
  "S. Central Semiarid" = "#D4537E",
  "W. Sierra Madre"     = "#378ADD"
)

eco_labels <- c(
  "COLD DESERTS"                    = "Cold Deserts",
  "MEDITERRANEAN CALIFORNIA"        = "Mediterranean CA",
  "UPPER GILA MOUNTAINS"            = "Upper Gila Mtns",
  "WARM DESERTS"                    = "Warm Deserts",
  "SOUTH CENTRAL SEMIARID PRAIRIES" = "S. Central Semiarid",
  "WESTERN SIERRA MADRE PIEDMONT"   = "W. Sierra Madre"
)

# Sample sizes per ecoregion for plot annotation
eco_n <- models_nitrate %>%
  mutate(eco_label = recode(ecoregion_name, !!!eco_labels)) %>%
  count(eco_label) %>% rename(ecoregion = eco_label)

# ── Extract interaction coefficient (dsf_scaled:burn_scaled) ──────────────────
# In this model the interaction is a single coefficient — same across ecoregions
# Ecoregion shifts the intercept only (additive)

# extract_draws <- function(fit, tau_label) {
#   fit %>%
#     spread_draws(`b_dsf_scaled:burn_scaled`) %>%
#     rename(interaction_coef = `b_dsf_scaled:burn_scaled`) %>%
#     mutate(tau = tau_label)
# }

extract_draws <- function(fit, tau_label) {
   fit %>%
     spread_draws(c(`b_dsf_scaled:pct_burned`)) %>%
     rename(interaction_coef = `b_dsf_scaled:pct_burned`) %>%
     mutate(tau = tau_label)
 }

interaction_draws <- bind_rows(
  extract_draws(fit_q95, "tau = 0.95"),
  extract_draws(fit_q50, "tau = 0.50"),
  extract_draws(fit_q05, "tau = 0.05")
)
head(interaction_draws)

# Summary table
interaction_summary <- interaction_draws %>%
  group_by(tau) %>%
  median_qi(interaction_coef, .width = c(0.80, 0.95))

cat("\n── Interaction coefficient summary ──\n")
print(interaction_summary)

cat("\n── Posterior probability that interaction > 0 ──\n")
interaction_draws %>%
  group_by(tau) %>%
  summarise(
    median_coef  = median(interaction_coef),
    prob_gt_zero = mean(interaction_coef > 0),
    prob_lt_zero = mean(interaction_coef < 0)
  ) %>%
  print()

# ── Extract ecoregion coefficients ────────────────────────────────────────────
# These are intercept shifts relative to Cold Deserts
summary(fit_q95)
rf <- ranef(fit_q95) #$`ecoregion:catchment`) # random intercepts
rf$ecoregion[, , "Intercept"] # extract ecoregion-level intercepts

tst <- spread_draws(fit_q95, r_ecoregion[ecoregion, term]) %>%
  filter(term == "Intercept") %>%
  mutate(ecoregion = gsub("\\.", " ", ecoregion))%>%
  left_join(eco_n, by = c("ecoregion" = "ecoregion")) %>%
  mutate(ecoregion = factor(ecoregion, levels = names(eco_colours)),
         eco_n_label = paste0(ecoregion, "\n(n = ", n, ")"))
tst

eco_draws_q95 <- fit_q95 %>%
  spread_draws(b_Intercept, r_ecoregion[ecoregion, term]) %>%
  filter(term == "Intercept") %>%
  mutate(
    intercept  = b_Intercept + r_ecoregion,
    ecoregion1  = gsub("\\.", " ", ecoregion),
    ecoregion = recode(ecoregion1, !!!eco_labels)
  ) %>%
  left_join(eco_n, by = "ecoregion") %>%
  mutate(
    ecoregion   = factor(ecoregion, levels = names(eco_colours)),
    eco_n_label = paste0(ecoregion, "\n(n = ", n, ")")
  )

head(eco_draws_q95$eco_n_label)

# eco_draws_q95 <- fit_q95 %>%
#   spread_draws(
#     b_Intercept,
#     `b_ecoregionMEDITERRANEANCALIFORNIA`,
#     `b_ecoregionUPPERGILAMOUNTAINS`,
#     `b_ecoregionWARMDESERTS`,
#     `b_ecoregionSOUTHCENTRALSEMIARIDPRAIRIES`,
#     `b_ecoregionWESTERNSIERRAMADREPIEDMONT`
#   ) %>%
#   mutate(
#     `Cold Deserts`        = b_Intercept,
#     `Mediterranean CA`    = b_Intercept + `b_ecoregionMEDITERRANEANCALIFORNIA`,
#     `Upper Gila Mtns`     = b_Intercept + `b_ecoregionUPPERGILAMOUNTAINS`,
#     `Warm Deserts`        = b_Intercept + `b_ecoregionWARMDESERTS`,
#     `S. Central Semiarid` = b_Intercept + `b_ecoregionSOUTHCENTRALSEMIARIDPRAIRIES`,
#     `W. Sierra Madre`     = b_Intercept + `b_ecoregionWESTERNSIERRAMADREPIEDMONT`
#   ) %>%
#   pivot_longer(
#     cols      = `Cold Deserts`:`W. Sierra Madre`,
#     names_to  = "ecoregion",
#     values_to = "intercept"
#   ) %>%
#   left_join(eco_n, by = "ecoregion") %>%
#   mutate(
#     ecoregion   = factor(ecoregion, levels = names(eco_colours)),
#     eco_n_label = paste0(ecoregion, "\n(n = ", n, ")")
#   )

eco_summary_q95 <- eco_draws_q95 %>%
  group_by(ecoregion, n) %>%
  median_qi(intercept, .width = c(0.80, 0.95)) %>%
  mutate(ecoregion = factor(ecoregion, levels = names(eco_colours)))

# ── Plot 1: interaction coefficient — half-eye, tau=0.95 vs 0.50 ──────────────
p1 <- interaction_draws %>%
  mutate(tau = factor(tau, levels = c("tau = 0.05","tau = 0.50", "tau = 0.95"))) %>%
  ggplot(aes(x = interaction_coef, y = tau, fill = tau, colour = tau)) +
  stat_halfeye(
    .width             = c(0.80, 0.95),
    point_interval     = median_qi,
    slab_alpha         = 0.6,
    interval_size_range = c(0.6, 1.4)
  ) +
  geom_vline(xintercept = 0, linetype = "dashed",
             colour = "grey40", linewidth = 0.5) +
  scale_fill_manual(values   = c("tau = 0.50" = "#888780",
                                  "tau = 0.95" = "#534AB7", 
                                  "tau = 0.05" = "#1D9E75"),
                    guide = "none") +
  scale_colour_manual(values = c("tau = 0.50" = "#888780",
                                  "tau = 0.95" = "#534AB7", 
                                  "tau = 0.05" = "#1D9E75"),
                      guide = "none") +
  labs(
    x        = "Interaction coefficient\n(days since fire × cumulative % burn, scaled)",
    y        = NULL,
    title    = "Days since fire × burn extent interaction on C-Q residuals",
    subtitle = "Intervals: 80% and 95% credible"
  ) +
  theme_bw(base_size = 12) +
  theme(panel.grid.minor  = element_blank(),
        plot.subtitle      = element_text(colour = "grey40", size = 10))
p1
# ── Plot 2: ecoregion intercepts at tau = 0.95 ────────────────────────────────
p2 <- eco_draws_q95 %>%
  mutate(eco_n_label = fct_reorder(eco_n_label, intercept, .fun = median)) %>%
  ggplot(aes(x = intercept, y = eco_n_label,
             fill = ecoregion, colour = ecoregion)) +
  stat_halfeye(
    .width             = c(0.80, 0.95),
    point_interval     = median_qi,
    slab_alpha         = 0.6,
    interval_size_range = c(0.6, 1.4)
  ) +
  geom_vline(xintercept = 0, linetype = "dashed",
             colour = "grey40", linewidth = 0.5) +
  scale_fill_manual(values = eco_colours, guide = "none") +
  scale_colour_manual(values = eco_colours, guide = "none") +
  labs(
    x        = "Intercept",
    y        = NULL,
    title    = "Ecoregion intercepts at τ = 0.95",
    subtitle = "Intervals: 80% and 95% credible"
  ) +
  theme_bw(base_size = 12) +
  theme(panel.grid.minor = element_blank(),
        plot.subtitle     = element_text(colour = "grey40", size = 10))
p2
# ── Plot 3: all fixed-effect coefficients at tau = 0.95 ──────────────────────
# Quick overview of every term in the model

coef_draws_q95 <- fit_q95 %>%
  gather_draws(
    b_Intercept,
    `b_dsf_scaled`,
    `b_pct_burned`,
    `b_dsf_scaled:pct_burned`,
    # `r_ecoregionMEDITERRANEANCALIFORNIA`,
    # `r_ecoregionUPPERGILAMOUNTAINS`,
    # `r_ecoregionWARMDESERTS`,
    # `b_ecoregionSOUTHCENTRALSEMIARIDPRAIRIES`,
    # `b_ecoregionWESTERNSIERRAMADREPIEDMONT`,
    b_cq_statuspositive,
    b_cq_statusnegative
  ) %>%
  mutate(
    term = recode(.variable,
      "b_Intercept"                                          = "Intercept",
      "b_dsf_scaled"                                         = "Days since fire",
      "b_pct_burned"                                        = "Cumulative % burn",
      "b_dsf_scaled:pct_burned"                             = "Fire × burn (interaction)",
      # "r_ecoregionMEDITERRANEANCALIFORNIA"                  = "Eco: Mediterranean CA",
      # "r_ecoregionUPPERGILAMOUNTAINS"                      = "Eco: Upper Gila Mtns",
      # "r_ecoregionWARMDESERTS"                              = "Eco: Warm Deserts",
      # "r_ecoregionSOUTHCENTRALSEMIARIDPRAIRIES"           = "Eco: S. Central Semiarid",
      # "r_ecoregionWESTERNSIERRAMADREPIEDMONT"             = "Eco: W. Sierra Madre",
      "b_cq_statuspositive"                                  = "CQ: flushing",
      "b_cq_statusnegative"                                  = "CQ: diluting"
    ),
    term = fct_reorder(term, .value, .fun = median)
  )

p3 <- coef_draws_q95 %>%
  ggplot(aes(x = .value, y = term)) +
  stat_halfeye(
    .width             = c(0.80, 0.95),
    point_interval     = median_qi,
    fill               = "#534AB7",
    colour             = "#534AB7",
    slab_alpha         = 0.5,
    interval_size_range = c(0.6, 1.4)
  ) +
  geom_vline(xintercept = 0, linetype = "dashed",
             colour = "grey40", linewidth = 0.5) +
  labs(
    x        = "Posterior coefficient estimate (scaled predictors)",
    y        = NULL,
    title    = "All fixed-effect coefficients at τ = 0.95",
    subtitle = "Intervals: 80% and 95% credible  |  Reference: Cold Deserts, CQ neutral"
  ) +
  theme_bw(base_size = 12) +
  theme(panel.grid.minor = element_blank(),
        plot.subtitle     = element_text(colour = "grey40", size = 10))
p3
# ── Plot 4: tau = 0.50 vs 0.95 coefficient comparison ────────────────────────
extract_all_coefs <- function(fit, tau_label) {
  fit %>%
    gather_draws(
      b_dsf_scaled,
      b_pct_burned,
      `b_dsf_scaled:pct_burned`,
      b_cq_statuspositive,
      b_cq_statusnegative,
   #   `b_dsf_scaled:cq_statuspositive`,
   #   `b_dsf_scaled:cq_statusnegative`,
   #   `b_burn_scaled:cq_statuspositive`,
   #   `b_burn_scaled:cq_statusnegative`,
   #   `b_dsf_scaled:burn_scaled:cq_statuspositive`,
   #   `b_dsf_scaled:burn_scaled:cq_statusnegative`
    ) %>%
    mutate(tau = tau_label)
}

# coef_comparison <- bind_rows(
#   extract_all_coefs(fit_q95, "τ = 0.95"),
#   extract_all_coefs(fit_q50, "τ = 0.50"),
#   extract_all_coefs(fit_q05, "τ = 0.05")
# ) %>%
#   mutate(
#     ) %>%
#     mutate(tau = tau_label)
# }

coef_comparison <- bind_rows(
  extract_all_coefs(fit_q95, "τ = 0.95"),
  extract_all_coefs(fit_q50, "τ = 0.50"),
  extract_all_coefs(fit_q05, "τ = 0.05")
) %>%
  mutate(
    term = recode(.variable,
      "b_dsf_scaled"             = "Days since fire",
      "b_pct_burned"            = "Cumulative % burn",
      "b_dsf_scaled:pct_burned" = "DSF × % burn",
      "b_cq_statuspositive"      = "CQ: flushing",
      "b_cq_statusnegative"      = "CQ: diluting",
 #     "b_dsf_scaled:cq_statuspositive" = "DSF × CQ: flushing",
 #     "b_dsf_scaled:cq_statusnegative" = "DSF × CQ: diluting",
 #     "b_burn_scaled:cq_statuspositive" = "% burn × CQ: flushing",
 #     "b_burn_scaled:cq_statusnegative" = "% burn × CQ: diluting",
 #     "b_dsf_scaled:burn_scaled:cq_statuspositive" = "DSF × % burn × CQ: flushing",
 #     "b_dsf_scaled:burn_scaled:cq_statusnegative" = "DSF × % burn × CQ: diluting"
    ),
    term = fct_reorder(term, .value, .fun = median)
  )

coef_comparison_summary <- coef_comparison %>%
  group_by(term, tau) %>%
  median_qi(.value, .width = 0.95)

p4 <- ggplot(coef_comparison_summary,
             aes(x = .value, y = term,
                 colour = tau, shape = tau)) +
  geom_vline(xintercept = 0, linetype = "dashed",
             colour = "grey40", linewidth = 0.5) +
  geom_linerange(aes(xmin = .lower, xmax = .upper),
                 position = position_dodge(width = 0.5),
                 linewidth = 0.8) +
  geom_point(size = 3,
             position = position_dodge(width = 0.5)) +
  scale_colour_manual(values = c("τ = 0.05" = "#1D9E75",
                                  "τ = 0.50" = "#888780",
                                  "τ = 0.95" = "#534AB7"),
                      name = NULL) +
  scale_shape_manual(values  = c("τ = 0.05" = 1,
                                  "τ = 0.50" = 1,
                                  "τ = 0.95" = 1),
                     name = NULL) +
  labs(
    x        = "Posterior coefficient estimate (scaled predictors)",
    y        = NULL,
    title    = "Coefficient comparison: τ = 0.05, τ = 0.50, τ = 0.95",
    #subtitle = "95% credible intervals  |  Filled = τ 0.95, Open = τ 0.50",
    caption  = "Reference: Cold Deserts ecoregion, CQ neutral"
  ) +
  theme_bw(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    legend.position  = "top",
    plot.subtitle    = element_text(colour = "grey40", size = 10),
    plot.caption     = element_text(colour = "grey50", size = 9)
  )
p4
# ── Print and save all plots ──────────────────────────────────────────────────
print(p1)
print(p2)
print(p3)
print(p4)

ggsave("p1_interaction_halfeye_v2.png",  p1, width = 7, height = 4, dpi = 300)
ggsave("p2_ecoregion_intercepts_v2.png", p2, width = 8, height = 6, dpi = 300)
ggsave("p3_all_coefs_q95_v2.png",        p3, width = 8, height = 6, dpi = 300)
ggsave("p4_tau_comparison_v2.png",       p4, width = 8, height = 6, dpi = 300)

# Combined
p_combined <- (p1 / p4) | (p2 / p3)
ggsave("p_combined.png", p_combined, width = 14, height = 12, dpi = 300)

# ── Posterior probability summary table ───────────────────────────────────────
cat("\n── Full coefficient posterior summary (tau = 0.95) ──\n")
coef_draws_q95 %>%
  group_by(term) %>%
  median_qi(.value, .width = 0.95) %>%
  mutate(prob_gt_zero = map_dbl(term, ~ mean(
    filter(coef_draws_q95, term == .x)$.value > 0
  ))) %>%
  select(term, median = .value, lower = .lower, upper = .upper, prob_gt_zero) %>%
  arrange(desc(abs(median))) %>%
  print(n = Inf)

###estimated effect of days since burn of the .95 .5 and .05 quantiles on the C-Q residualsplotted against residual values to see how the effect changes across the distribution of residuals
###plotted against residual values to see how the effect changes across the distribution of residuals
compare_plot_dsf <- coef_comparison %>%
  filter(term == "Days since fire") %>%
  ggplot(aes(x = .value, y = tau, colour = tau)) +
  stat_halfeye() +
  geom_vline(xintercept = 0, linetype = "dashed", colour = "grey40") +
  labs(
    x = "Coefficient estimate for Days since fire (scaled)",
    y = NULL,
    title = "Effect of Days since fire across quantiles"
  ) +
  scale_colour_manual(values = c("tau = 0.05" = "#1D9E75",
                                  "tau = 0.50" = "#888780",
                                  "tau = 0.95" = "#534AB7"),
                      name = NULL) +
  theme_bw(base_size = 12) +
  theme(panel.grid.minor = element_blank(),
        legend.position = "none")

compare_plot_dsf

compare_plot_pct_burn <- coef_comparison %>%
  filter(term == "Cumulative % burn") %>%
  ggplot(aes(x = .value, y = tau, colour = tau)) +
  stat_halfeye() +
  geom_vline(xintercept = 0, linetype = "dashed", colour = "grey40") +
  labs(
    x = "Percentage burned (scaled)",
    y = NULL,
    title = "Effect of Cumulative % burn across quantiles"
  ) +
  scale_colour_manual(values = c("tau = 0.05" = "#1D9E75",
                                  "tau = 0.50" = "#888780",
                                  "tau = 0.95" = "#534AB7"),
                      name = NULL) +
  theme_bw(base_size = 12) +
  theme(panel.grid.minor = element_blank(),
        legend.position = "none")

compare_plot_pct_burn

##by constraining the data to the quantiles of the residuals-we limit the data to 
##relativley small subsets of the data... meaning the effect 
##on the 95th percentile is not strong in that group

###instead of looking at only the specific quantiles of the residuals, we can look across the distribution 
##of residuals by plotting the coefficient estimates against the residual values. 
##This way we can see how the effect changes across the distribution of residuals without constraining the data to specific quantiles.
## then maybe we can look at the different quantiles

coef_comparison %>%
  filter(term == "Days since fire") %>%
  group_by(tau) 
  %>%
  median_qi(.value, .width = 0.95) %>%
  mutate(prob_gt_zero = mean(.value > 0)) %>%
  select(tau, median = .value, lower = .lower, upper = .upper, prob_gt_zero) %>%
  print()

conditional_effects(fit_q95) # extract ecoregion-level intercepts
head(coef_comparison)



as_draws_df(fit_q95)
