# =============================================================================
# CQ Bayes Analysis - SoCal Burns
# Author: Shanthini Ode
# Updated: biome_pca added to model; multiple code review fixes applied
# =============================================================================
#
# CODE REVIEW NOTES — issues found and fixed from original script:
#
# [FIX 1] pca_scores.csv removed. biome_pca is already a column in
#         allchems_loq.csv. The original load + select(-biome_pca) +
#         left_join(biome_key) pattern was circular and would error if
#         pca_scores.csv was missing.
#
# [FIX 2] final_Q removed. It was loaded but never referenced anywhere
#         in the script. Would error on startup if the file was missing.
#
# [FIX 3] summary(allchems) moved out of the middle of the wrangling
#         pipeline where it would silently print to console mid-script.
#
# [FIX 4] ungroup() added before building scaled. CQ_raw_data inherits
#         the Site, solute grouping from Qchemsitey %>% nest(). Without
#         ungroup(), that invisible grouping travels into scaled, which
#         makes grouped mutates and joins behave unexpectedly downstream.
#
# [FIX 5] z_pct_burned removed from site_level. It was computed but the
#         model uses raw pct_burned (proportion), never z_pct_burned.
#         Dead code that implied the z-scored version was in the model.
#
# [FIX 6] CQ_plot_data removed. It was computed (broom::augment across
#         all models) but never used in the rest of the script.
#
# [FIX 7] biome_key dependency inside the function replaced by a local
#         site_biome lookup derived from dat. The original used the
#         global biome_key via lexical scoping - works but fragile.
#
# [FIX 8] fits_env = NULL parameter removed from run_cq_analysis. It
#         was never referenced inside the function.
#
# [FIX 9] dir.create("Plots", ...) added before the function. The
#         ggsave("Plots/...") call would error if the directory did not
#         already exist.
#
# [FIX 10] biome_pca added to the model formula and priors (requested).
#          Forest set as reference level. p_pred now facets by biome_pca
#          so predictions show the intercept shift per biome.
#
# [NOTE] car is loaded after tidyverse/dplyr. car::recode masks
#        dplyr::recode. Not an immediate error in this script since
#        recode() is not currently called, but any future use of
#        recode() will call car::recode silently. Use dplyr::recode()
#        explicitly or switch to case_when() if needed.
# =============================================================================


# -----------------------------------------------------------------------------
# 1. Libraries
# -----------------------------------------------------------------------------

library(broom)
library(tidyverse)
library(here)
library(lme4)
library(ggplot2)
library(dplyr)
library(emmeans)
library(patchwork)
library(car)
library(MuMIn)
library(brms)
library(tidybayes)
library(ggrepel)

options(scipen = 999)


# -----------------------------------------------------------------------------
# 2. Load data
# -----------------------------------------------------------------------------

# biome_pca is a column in allchems_loq.csv - no pca_scores.csv join needed
# [FIX 1] removed pca <- read.csv(...)
# [FIX 2] removed final_Q <- read.csv(...) - never used
allchems <- read.csv("data/processed/allchems_loq.csv")

# [FIX 3] summary moved here, out of the wrangling pipeline
summary(allchems)

# Below-detection-limit check
# Half-LOQ substitution produces repeated exact minimum values per solute.
# This flags those values and reports their proportion.

loq_check <- allchems %>%
  select(avg_NPOC, avg_TN, SRP_uM, NitrateN_mgL) %>%
  pivot_longer(everything(), names_to = "solute", values_to = "value") %>%
  filter(!is.na(value)) %>%
  group_by(solute) %>%
  mutate(loq_value = min(value)) %>%
  summarise(
    loq_value   = first(loq_value),
    n_total     = n(),
    n_at_loq    = sum(value == loq_value),
    pct_at_loq  = round(100 * n_at_loq / n_total, 1),
    .groups     = "drop"
  ) %>%
  arrange(desc(pct_at_loq))

print(loq_check)


# -----------------------------------------------------------------------------
# 3. Data wrangling
# -----------------------------------------------------------------------------

allchems_long <- allchems %>%
  pivot_longer(
    cols      = c(avg_NPOC, avg_TN, SRP_uM, Chloride_mgL, Sulfate_mgL, NitrateN_mgL),
    names_to  = "solute",
    values_to = "concentration"
  ) %>%
  filter(!is.na(Q..L.s.))

# Sites sampled monthly (reference list - uncomment filter below to subset)
monthly_sites <- c(
  "arroyohondo", "coldwater", "deep", "mission", "ortega", "rattlesnake",
  "refugio", "santaynez", "santiago", "sedgwick", "silverado", "stunt",
  "topanga", "waterman", "zuma"
  # excluding trabuco - only 2 points, not reliable for CQ slope
)

allchems_filtered <- allchems_long %>%
  # filter(Site %in% monthly_sites) %>%
  filter(concentration > 0, Q..L.s. > 0)

Qchemsitey <- allchems_filtered %>%
  filter(solute %in% c("avg_NPOC", "avg_TN", "SRP_uM",
                       "Chloride_mgL", "Sulfate_mgL", "NitrateN_mgL")) %>%
  group_by(Site, solute) %>%
  nest()


# -----------------------------------------------------------------------------
# 4. Frequentist CQ regressions (site x solute)
# -----------------------------------------------------------------------------

CQ <- function(df) { lm(log(concentration) ~ log(Q..L.s.), data = df) }

CQmods <- Qchemsitey %>%
  mutate(model = map(data, CQ))


# -----------------------------------------------------------------------------
# 5. Build raw data frame and site-level lookup
# -----------------------------------------------------------------------------

# biome_pca is already in allchems_loq.csv - derive lookup from the source data
# [FIX 1] replaces: biome_key <- pca %>% dplyr::select(Site, biome_pca) %>% distinct()
biome_key <- allchems %>%
  dplyr::select(Site, biome_pca) %>%
  distinct()

# [FIX 6] CQ_plot_data removed - was computed (broom::augment) but never used

# [FIX 1, 4] Removed select(-biome_pca) + left_join(biome_key) - circular.
# biome_pca is already in the nested data from allchems_loq.csv.
# ungroup() added to drop the invisible Site, solute grouping from nest().
CQ_raw_data <- Qchemsitey %>%
  unnest(data) %>%
  ungroup()                          # [FIX 4]

solute_labels <- c(
  "avg_NPOC"     = "DOC",
  "avg_TN"       = "TN",
  "SRP_uM"       = "SRP",
  "Chloride_mgL" = "Chloride",
  "Sulfate_mgL"  = "Sulfate",
  "NitrateN_mgL" = "Nitrate"
)

biome_colors <- c("shrub_grass" = "dodgerblue3", "forest" = "darkgreen")


# -----------------------------------------------------------------------------
# 6. Global plot theme
# -----------------------------------------------------------------------------

theme_set(
  theme_bw() +
    theme(
      plot.background  = element_blank(),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.background = element_blank(),
      panel.border     = element_blank(),
      axis.text.x      = element_text(angle = 90, vjust = 0.5, size = 8),
      axis.ticks       = element_blank(),
      strip.background = element_rect()
    )
)


# -----------------------------------------------------------------------------
# 7. Scale and prepare data for Bayesian model
# -----------------------------------------------------------------------------

scaled <- CQ_raw_data %>%
  filter(Q..L.s. > 0 & concentration > 0) %>%
  mutate(
    log_flow          = log(Q..L.s.),
    log_concentration = log(concentration),
    # [FIX 10] set forest as reference level for biome_pca (16 sites).
    # shrub_grass (18 sites) becomes the estimated contrast b_biome_pcashrub_grass.
    biome_pca         = relevel(factor(biome_pca), ref = "forest")
  )

# Z-scores computed at the site level (one value per site) then joined back.
# [FIX 5] z_pct_burned removed - it was computed here but the model uses
# raw pct_burned (proportion scale), not the z-scored version.
site_level <- scaled %>%
  group_by(Site) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(
    z_mean_slope = scale(mean_slope)[, 1],
    z_time_since = scale(time_since)[, 1]
  ) %>%
  select(Site, z_mean_slope, z_time_since)

scaled <- scaled %>%
  left_join(site_level, by = "Site") %>%
  mutate(pct_burned = pct_burned / 100)   # convert to proportion [0, 1]


# -----------------------------------------------------------------------------
# 8. Bayesian model formula and priors
# -----------------------------------------------------------------------------

# [FIX 10] biome_pca added as a fixed main effect on the intercept.
# Forest sites have systematically different baseline concentrations from
# shrub_grass sites - omitting this was contributing to residual structure
# visible in pp_check plots.
brms.mod <- bf(
  log_concentration ~ log_flow * pct_burned + biome_pca +
    z_time_since + z_mean_slope + (log_flow | Site)
)

# normal(0, 1) on class = b covers all fixed effects including the
# biome_pca contrast. On the log-concentration scale, 1 unit ~ 2.7x
# change in concentration, so this is regularising but not restrictive.
prior0 <- c(
  prior(normal(0, 1),   class = b),
  prior(normal(0, 2),   class = Intercept),
  prior(exponential(1), class = sigma),
  prior(exponential(1), class = sd),
  prior(lkj(2),         class = cor)
)

# [FIX 9] Create output directory for ggsave before running models
dir.create("Plots", showWarnings = FALSE)


# -----------------------------------------------------------------------------
# 9. Core analysis function
# -----------------------------------------------------------------------------

# [FIX 8] removed fits_env = NULL - was never used inside the function
run_cq_analysis <- function(sol, scaled, brms.mod, prior0, solute_labels) {
  
  lab <- solute_labels[[sol]]
  cat("\n\n===== Fitting:", lab, "=====\n")
  
  # Filter to this solute
  dat <- scaled %>% filter(solute == sol)
  
  # [FIX 7] Site-level biome lookup derived from dat, not the global biome_key.
  # Using the global via lexical scoping works but is fragile if biome_key
  # is ever modified or out of scope.
  site_biome <- dat %>%
    group_by(Site) %>%
    slice(1) %>%
    ungroup() %>%
    select(Site, biome_pca)
  
  # Burn proportion per site (for slope decomposition and delta_mu plots)
  burn_by_site <- dat %>%
    group_by(Site) %>%
    summarise(pct_burned = first(pct_burned), .groups = "drop")
  
  # ---- Fit model ----
  fit <- brm(
    brms.mod,
    data    = dat,
    family  = "gaussian",
    prior   = prior0,
    chains  = 4, cores = 8, iter = 2000, backend = "cmdstanr",
    control = list(adapt_delta = 0.95, max_treedepth = 14)
  )
  
  print(summary(fit, prob = 0.5))
  print(pp_check(fit) + ggtitle(paste("pp_check:", lab)))
  
  # ---- Site-level CQ slope decomposition ----
  random_slopes <- ranef(fit)$Site[,, "log_flow"] %>%
    as.data.frame() %>%
    rownames_to_column("Site") %>%
    select(Site, r_Site = Estimate)
  
  cq_sl <- fit %>%
    spread_draws(b_log_flow, `b_log_flow:pct_burned`) %>%
    crossing(burn_by_site) %>%
    left_join(random_slopes, by = "Site") %>%
    mutate(
      slope_population = b_log_flow,
      slope_burn       = `b_log_flow:pct_burned` * pct_burned,
      slope_residual   = r_Site,
      slope_total      = slope_population + slope_burn + slope_residual
    ) %>%
    group_by(Site, pct_burned) %>%
    median_qi(slope_total, slope_burn, slope_residual, .width = 0.50) %>%
    ungroup() %>%
    arrange(slope_total) %>%
    mutate(solute = sol)
  
  # Plot: site-level CQ slopes (caterpillar)
  p_slopes <- cq_sl %>%
    left_join(site_biome, by = "Site") %>%
    mutate(Site = fct_reorder(Site, slope_total)) %>%
    ggplot(aes(x = slope_total, y = Site, color = biome_pca)) +
    geom_point() +
    geom_errorbarh(aes(xmin = slope_total.lower, xmax = slope_total.upper),
                   height = 0) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    scale_color_manual(values = biome_colors, name = "Biome") +
    labs(
      x     = "CQ slope (median & 50% CrI)",
      y     = NULL,
      title = paste(lab, "- Site-level CQ slopes")
    )
  print(p_slopes)
  
  # Plot: population CQ slope as function of burn proportion
  p_burn <- fit %>%
    spread_draws(b_log_flow, `b_log_flow:pct_burned`) %>%
    crossing(pct_burned = seq(0, 1, by = 0.01)) %>%
    mutate(cq_slope = b_log_flow + `b_log_flow:pct_burned` * pct_burned) %>%
    group_by(pct_burned) %>%
    median_qi(cq_slope, .width = c(0.80, 0.95)) %>%
    ggplot(aes(pct_burned, cq_slope)) +
    geom_ribbon(
      aes(ymin = .lower, ymax = .upper, group = .width,
          alpha = factor(.width)),
      fill = "steelblue"
    ) +
    scale_alpha_manual(
      values = c("0.8" = 0.4, "0.95" = 0.15),
      labels = c("80% CrI", "95% CrI"),
      name   = NULL
    ) +
    geom_line() +
    geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
    annotate("text", x = 0.05, y =  0.02, label = "mobilization",
             color = "red", hjust = 0, size = 3) +
    annotate("text", x = 0.05, y = -0.02, label = "dilution",
             color = "red", hjust = 0, size = 3) +
    labs(
      x       = "Proportion burned",
      y       = "CQ slope",
      title   = paste(lab, "- CQ slope vs burn proportion"),
      caption = "Population-level (averaged over biomes); ribbons = 80% and 95% CrI"
    )
  print(p_burn)
  
  # Plot: population trend + site overlay
  cq_sl_biome <- cq_sl %>% left_join(site_biome, by = "Site")
  
  p_overlay <- fit %>%
    spread_draws(b_log_flow, `b_log_flow:pct_burned`) %>%
    crossing(pct_burned = seq(0, 1, by = 0.01)) %>%
    mutate(cq_slope = b_log_flow + `b_log_flow:pct_burned` * pct_burned) %>%
    group_by(pct_burned) %>%
    median_qi(cq_slope, .width = c(0.80, 0.95)) %>%
    ggplot(aes(pct_burned, cq_slope)) +
    geom_ribbon(
      aes(ymin = .lower, ymax = .upper, group = .width,
          alpha = factor(.width)),
      fill = "darkgray"
    ) +
    scale_alpha_manual(
      values = c("0.8" = 0.4, "0.95" = 0.15),
      labels = c("80% CrI", "95% CrI"),
      name   = NULL
    ) +
    geom_line() +
    geom_hline(yintercept = 0, linetype = "dashed", color = "red") +
    geom_pointrange(
      data = cq_sl_biome,
      aes(x = pct_burned, y = slope_total,
          ymin = slope_total.lower, ymax = slope_total.upper,
          color = biome_pca),
      size = 0.4
    ) +
    ggrepel::geom_text_repel(
      data  = cq_sl_biome,
      aes(x = pct_burned, y = slope_total, label = Site, color = biome_pca),
      size  = 2.5
    ) +
    scale_color_manual(values = biome_colors, name = "Biome") +
    labs(
      x        = "Proportion burned",
      y        = "CQ slope",
      title    = paste(lab, "- CQ slope vs burn proportion with sites"),
      subtitle = "Line = population trend; points = site slopes (median & 50% CrI)",
      caption  = "Dashed line = chemostatic boundary"
    )
  print(p_overlay)
  
  # Plot: predicted CQ at varying burn levels, faceted by biome_pca
  # [FIX 10] b_biome_pcashrub_grass included in draws so the intercept
  # shift is correctly applied per biome panel. Covariates held at mean (z = 0).
  p_pred <- fit %>%
    spread_draws(b_Intercept, b_log_flow, b_pct_burned,
                 `b_log_flow:pct_burned`, b_z_time_since, b_z_mean_slope,
                 b_biome_pcashrub_grass) %>%
    crossing(
      biome_pca  = c("forest", "shrub_grass"),
      pct_burned = c(0, 0.25, 0.50, 0.75, 1.0),
      log_flow   = seq(min(dat$log_flow), max(dat$log_flow), length.out = 50)
    ) %>%
    mutate(
      biome_offset      = if_else(biome_pca == "shrub_grass", b_biome_pcashrub_grass, 0),
      log_concentration =
        b_Intercept +
        biome_offset +
        b_log_flow              * log_flow +
        b_pct_burned            * pct_burned +
        `b_log_flow:pct_burned` * log_flow * pct_burned +
        b_z_time_since * 0 +   # held at mean (z = 0)
        b_z_mean_slope * 0
    ) %>%
    group_by(biome_pca, pct_burned, log_flow) %>%
    median_qi(log_concentration, .width = 0.50) %>%
    ggplot(aes(log_flow, log_concentration,
               color = factor(pct_burned),
               fill  = factor(pct_burned),
               group = factor(pct_burned))) +
    geom_ribbon(aes(ymin = .lower, ymax = .upper), alpha = 0.15, color = NA) +
    geom_line(linewidth = 0.8) +
    scale_color_viridis_d(name = "Burn proportion", option = "plasma") +
    scale_fill_viridis_d(name  = "Burn proportion", option = "plasma") +
    facet_wrap(~biome_pca, ncol = 2) +
    labs(
      x       = "log flow",
      y       = paste0("log ", lab),
      title   = paste(lab, "- Predicted CQ across burn levels by biome"),
      caption = "Population-level predictions; ribbons = 50% CrI; covariates at mean"
    )
  print(p_pred)
  
  # ---- Burn effect (delta_mu) at low / mean / high flow ----
  sd_x   <- sd(dat$log_flow)
  x_star <- c(-sd_x, 0, sd_x)
  
  dmu <- fit %>%
    spread_draws(b_pct_burned, `b_log_flow:pct_burned`) %>%
    crossing(x = x_star) %>%
    mutate(
      flow_condition = case_when(
        x < 0  ~ "low flow (-1 SD)",
        x == 0 ~ "mean flow",
        x > 0  ~ "high flow (+1 SD)"
      ),
      delta_mu = b_pct_burned + `b_log_flow:pct_burned` * x
    ) %>%
    group_by(flow_condition, x) %>%
    median_qi(delta_mu, .width = 0.50) %>%
    ungroup() %>%
    rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
    arrange(x) %>%
    mutate(solute = sol)
  
  p_dmu <- dmu %>%
    mutate(flow_condition = fct_reorder(flow_condition, x)) %>%
    ggplot(aes(x = median, y = flow_condition)) +
    geom_point() +
    geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    labs(
      x     = paste0("Δ log-", lab, " (fully burned vs unburned, median & 50% CrI)"),
      y     = NULL,
      title = paste(lab, "- Burn effect across flow conditions")
    )
  print(p_dmu)
  
  # ---- Burn effect by site ----
  dmu_site <- fit %>%
    spread_draws(b_pct_burned, `b_log_flow:pct_burned`) %>%
    crossing(burn_by_site) %>%
    crossing(x = x_star) %>%
    mutate(
      flow_condition = case_when(
        x < 0  ~ "low flow (-1 SD)",
        x == 0 ~ "mean flow",
        x > 0  ~ "high flow (+1 SD)"
      ),
      delta_mu = (b_pct_burned + `b_log_flow:pct_burned` * x) * pct_burned
    ) %>%
    group_by(Site, flow_condition, x, pct_burned) %>%
    median_qi(delta_mu, .width = 0.50) %>%
    ungroup() %>%
    rename(median = delta_mu, lo50 = .lower, hi50 = .upper) %>%
    arrange(Site, x) %>%
    mutate(solute = sol)
  
  p_dmu_site <- dmu_site %>%
    left_join(site_biome, by = "Site") %>%
    mutate(
      flow_condition = fct_reorder(flow_condition, x),
      Site           = fct_reorder(Site, pct_burned)
    ) %>%
    ggplot(aes(x = median, y = flow_condition, color = biome_pca)) +
    geom_point(size = 1.5) +
    geom_errorbarh(aes(xmin = lo50, xmax = hi50), height = 0) +
    geom_vline(xintercept = 0, linetype = "dashed") +
    scale_color_manual(values = biome_colors, name = "Biome") +
    facet_wrap(~Site, ncol = 4) +
    labs(
      x       = paste0("Δ log-", lab, " (actual burn vs unburned, median & 50% CrI)"),
      y       = NULL,
      title   = paste(lab, "- Burn effect by site and flow condition"),
      caption = "Sites ordered by % burned"
    ) +
    theme_classic() +
    theme(
      strip.text    = element_text(size = 7),
      axis.text.y   = element_text(size = 6),
      axis.text.x   = element_text(size = 6),
      panel.spacing = unit(0.5, "lines")
    )
  
  ggsave(paste0("Plots/dmu_site_", sol, ".png"), p_dmu_site,
         width = 16, height = 12, dpi = 150)
  print(p_dmu_site)
  
  cat("===== Done:", lab, "=====\n")
  
  invisible(list(
    fit        = fit,
    cq_slopes  = cq_sl,
    delta_mu   = dmu,
    delta_site = dmu_site
  ))
}


# -----------------------------------------------------------------------------
# 10. Run models for each solute
# -----------------------------------------------------------------------------

fits       <- list()
cq_slopes  <- list()
delta_mu   <- list()
delta_site <- list()

results_doc <- run_cq_analysis("avg_NPOC",     scaled, brms.mod, prior0, solute_labels)
fits[["avg_NPOC"]]           <- results_doc$fit
cq_slopes[["avg_NPOC"]]      <- results_doc$cq_slopes
delta_mu[["avg_NPOC"]]       <- results_doc$delta_mu
delta_site[["avg_NPOC"]]     <- results_doc$delta_site

results_tn  <- run_cq_analysis("avg_TN",       scaled, brms.mod, prior0, solute_labels)
fits[["avg_TN"]]             <- results_tn$fit
cq_slopes[["avg_TN"]]        <- results_tn$cq_slopes
delta_mu[["avg_TN"]]         <- results_tn$delta_mu
delta_site[["avg_TN"]]       <- results_tn$delta_site

results_srp <- run_cq_analysis("SRP_uM",       scaled, brms.mod, prior0, solute_labels)
fits[["SRP_uM"]]             <- results_srp$fit
cq_slopes[["SRP_uM"]]        <- results_srp$cq_slopes
delta_mu[["SRP_uM"]]         <- results_srp$delta_mu
delta_site[["SRP_uM"]]       <- results_srp$delta_site

results_cl  <- run_cq_analysis("Chloride_mgL", scaled, brms.mod, prior0, solute_labels)
fits[["Chloride_mgL"]]       <- results_cl$fit
cq_slopes[["Chloride_mgL"]]  <- results_cl$cq_slopes
delta_mu[["Chloride_mgL"]]   <- results_cl$delta_mu
delta_site[["Chloride_mgL"]] <- results_cl$delta_site

results_so4 <- run_cq_analysis("Sulfate_mgL",  scaled, brms.mod, prior0, solute_labels)
fits[["Sulfate_mgL"]]        <- results_so4$fit
cq_slopes[["Sulfate_mgL"]]   <- results_so4$cq_slopes
delta_mu[["Sulfate_mgL"]]    <- results_so4$delta_mu
delta_site[["Sulfate_mgL"]]  <- results_so4$delta_site

results_no3 <- run_cq_analysis("NitrateN_mgL", scaled, brms.mod, prior0, solute_labels)
fits[["NitrateN_mgL"]]       <- results_no3$fit
cq_slopes[["NitrateN_mgL"]]  <- results_no3$cq_slopes
delta_mu[["NitrateN_mgL"]]   <- results_no3$delta_mu
delta_site[["NitrateN_mgL"]] <- results_no3$delta_site